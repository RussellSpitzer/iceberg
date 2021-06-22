/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.actions;

import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.math.LongMath;
import org.apache.iceberg.util.BinPacking;
import org.apache.iceberg.util.BinPacking.ListPacker;
import org.apache.iceberg.util.PropertyUtil;

/**
 * A rewrite strategy for data files which determines which files to rewrite
 * based on their size. If files are either smaller than the {@link #MIN_FILE_SIZE_BYTES} threshold or
 * larger than the {@link #MAX_FILE_SIZE_BYTES} threshold, they are considered targets for being rewritten.
 * <p>
 * Once selected files are grouped based on a {@link BinPacking} into groups defined
 * by {@link RewriteDataFiles#MAX_FILE_GROUP_SIZE_BYTES}. Groups will be considered for rewriting if they contain
 * more files than {@link #MIN_INPUT_FILES} or would produce at least one file of
 * {@link RewriteDataFiles#TARGET_FILE_SIZE_BYTES}.
 */
public abstract class BinPackStrategy implements RewriteStrategy {

  /**
   * The minimum number of files that need to be in a file group for it to be considered for
   * compaction if the total size of that group is less than the {@link RewriteDataFiles#TARGET_FILE_SIZE_BYTES}.
   * This can also be thought of as the maximum number of non-target-size files that could remain in a file
   * group (partition) after rewriting.
   */
  public static final String MIN_INPUT_FILES = "min-input-files";
  public static final int MIN_INPUT_FILES_DEFAULT = 5;

  /**
   * Adjusts files which will be considered for rewriting. Files smaller than
   * {@link #MIN_FILE_SIZE_BYTES} will be considered for rewriting. This functions independently
   * of {@link #MAX_FILE_SIZE_BYTES}.
   * <p>
   * Defaults to 75% of the target file size
   */
  public static final String MIN_FILE_SIZE_BYTES = "min-file-size-bytes";
  public static final double MIN_FILE_SIZE_DEFAULT_RATIO = 0.75d;

  /**
   * Adjusts files which will be considered for rewriting. Files larger than
   * {@link #MAX_FILE_SIZE_BYTES} will be considered for rewriting. This functions independently
   * of {@link #MIN_FILE_SIZE_BYTES}.
   * <p>
   * Defaults to 180% of the target file size
   */
  public static final String MAX_FILE_SIZE_BYTES = "max-file-size-bytes";
  public static final double MAX_FILE_SIZE_DEFAULT_RATIO = 1.80d;

  private int minInputFiles;
  private long minFileSize;
  private long maxFileSize;
  private long targetFileSize;
  private long maxGroupSize;

  @Override
  public String name() {
    return "BINPACK";
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.of(
        MIN_INPUT_FILES,
        MIN_FILE_SIZE_BYTES,
        MAX_FILE_SIZE_BYTES
    );
  }

  @Override
  public RewriteStrategy options(Map<String, String> options) {
    targetFileSize = PropertyUtil.propertyAsLong(options,
        RewriteDataFiles.TARGET_FILE_SIZE_BYTES,
        PropertyUtil.propertyAsLong(
            table().properties(),
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT));

    minFileSize = PropertyUtil.propertyAsLong(options,
        MIN_FILE_SIZE_BYTES,
        (long) (targetFileSize * MIN_FILE_SIZE_DEFAULT_RATIO));

    maxFileSize = PropertyUtil.propertyAsLong(options,
        MAX_FILE_SIZE_BYTES,
        (long) (targetFileSize * MAX_FILE_SIZE_DEFAULT_RATIO));

    maxGroupSize = PropertyUtil.propertyAsLong(options,
        RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES,
        RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES_DEFAULT);

    minInputFiles = PropertyUtil.propertyAsInt(options,
        MIN_INPUT_FILES,
        MIN_INPUT_FILES_DEFAULT);

    validateOptions();
    return this;
  }

  @Override
  public Iterable<FileScanTask> selectFilesToRewrite(Iterable<FileScanTask> dataFiles) {
    return FluentIterable.from(dataFiles)
        .filter(scanTask -> scanTask.length() < minFileSize || scanTask.length() > maxFileSize);
  }

  @Override
  public Iterable<List<FileScanTask>> planFileGroups(Iterable<FileScanTask> dataFiles) {
    ListPacker<FileScanTask> packer = new BinPacking.ListPacker<>(maxGroupSize, 1, false);
    List<List<FileScanTask>> potentialGroups = packer.pack(dataFiles, FileScanTask::length);
    return potentialGroups.stream().filter(group ->
      group.size() >= minInputFiles || sizeOfInputFiles(group) > targetFileSize
    ).collect(Collectors.toList());
  }

  protected long targetFileSize() {
    return this.targetFileSize;
  }

  /**
   * Determine how many output files to create when rewriting. We use this to determine the split-size
   * we want to use when actually writing files to avoid the following situation.
   * <p>
   * If we are writing 10.1 G of data with a target file size of 1G we would end up with
   * 11 files, one of which would only have 0.1g. This would most likely be less preferable to
   * 10 files each of which was 1.01g. So here we decide whether to round up or round down
   * based on what the estimated average file size will be if we ignore the remainder (0.1g). If
   * the new file size is less than 10% greater than the target file size then we will round down
   * when determining the number of output files.
   * @param totalSizeInBytes total data size for a file group
   * @return the number of files this strategy should create
   */
  protected long numOutputFiles(long totalSizeInBytes) {
    if (totalSizeInBytes < targetFileSize) {
      return 1;
    }

    long fileCountWithRemainder = LongMath.divide(totalSizeInBytes, targetFileSize, RoundingMode.CEILING);
    if (LongMath.mod(totalSizeInBytes, targetFileSize) > minFileSize) {
      // Our Remainder file is of valid size for this compaction so keep it
      return fileCountWithRemainder;
    }

    long fileCountWithoutRemainder = LongMath.divide(totalSizeInBytes, targetFileSize, RoundingMode.FLOOR);
    long avgFileSizeWithoutRemainder = totalSizeInBytes / fileCountWithoutRemainder;
    if (avgFileSizeWithoutRemainder < Math.min(1.1 * targetFileSize, writeMaxFileSize())) {
      // Round down and distribute remainder amongst other files
      return fileCountWithoutRemainder;
    } else {
      // Keep the remainder file
      return fileCountWithRemainder;
    }
  }

  /**
   * Returns the smaller of our max write file threshold, and our estimated split size based on
   * the number of output files we want to generate.
   */
  protected long splitSize(long totalSizeInBytes) {
    long estimatedSplitSize = totalSizeInBytes / numOutputFiles(totalSizeInBytes);
    return Math.min(estimatedSplitSize, writeMaxFileSize());
  }

  protected long inputFileSize(List<FileScanTask> fileToRewrite) {
    return fileToRewrite.stream().mapToLong(FileScanTask::length).sum();
  }

  /**
   * Estimates a larger max target file size than our target size used in task creation to avoid
   * tasks which are predicted to have a certain size, but exceed that target size when serde is complete creating
   * tiny remainder files.
   * <p>
   * While we create tasks that should all be smaller than our target size there is a chance that the actual
   * data will end up being larger than our target size due to various factors of compression, serialization and
   * other factors outside our control. If this occurs, instead of making a single file that is close in size to
   * our target we would end up producing one file of the target size, and then a small extra file with the remaining
   * data. For example, if our target is 512 MB we may generate a rewrite task that should be 500 MB. When we write
   * the data we may find we actually have to write out 530 MB. If we use the target size while writing we would
   * produced a 512 MB file and a 18 MB file. If instead we use a larger size estimated by this method,
   * then we end up writing a single file.
   * @return the target size plus one half of the distance between max and target
   */
  protected long writeMaxFileSize() {
    return (long) (targetFileSize + ((maxFileSize - targetFileSize) * 0.5));
  }

  private long sizeOfInputFiles(List<FileScanTask> group) {
    return group.stream().mapToLong(FileScanTask::length).sum();
  }

  private void validateOptions() {
    Preconditions.checkArgument(minFileSize >= 0,
        "Cannot set %s to a negative number, %d < 0",
        MIN_FILE_SIZE_BYTES, minFileSize);

    Preconditions.checkArgument(maxFileSize > minFileSize,
        "Cannot set %s greater than or equal to %s, %d >= %d",
        MIN_FILE_SIZE_BYTES, MAX_FILE_SIZE_BYTES, minFileSize, maxFileSize);

    Preconditions.checkArgument(targetFileSize > minFileSize,
        "Cannot set %s greater than or equal to %s, all files written will be smaller than the threshold, %d >= %d",
        MIN_FILE_SIZE_BYTES, RewriteDataFiles.TARGET_FILE_SIZE_BYTES, minFileSize, targetFileSize);

    Preconditions.checkArgument(targetFileSize < maxFileSize,
        "Cannot set %s is greater than or equal to %s, all files written will be larger than the threshold, %d >= %d",
        MAX_FILE_SIZE_BYTES, RewriteDataFiles.TARGET_FILE_SIZE_BYTES, maxFileSize, targetFileSize);

    Preconditions.checkArgument(minInputFiles > 0,
        "Cannot set %s is less than 1. All values less than 1 have the same effect as 1. %d < 1",
        MIN_INPUT_FILES, minInputFiles);
  }

  protected long targetFileSize() {
    return this.targetFileSize;
  }

  /**
   * Determine how many output files to create when rewriting. We use this to determine the split-size
   * we want to use when actually writing files to avoid the following situation.
   * <p>
   * If we are writing 10.1 G of data with a target file size of 1G we would end up with
   * 11 files, one of which would only have 0.1g. This would most likely be less preferable to
   * 10 files each of which was 1.01g. So here we decide whether to round up or round down
   * based on what the estimated average file size will be if we ignore the remainder (0.1g). If
   * the new file size is less than 10% greater than the target file size then we will round down
   * when determining the number of output files.
   * @param totalSizeInBytes total data size for a file group
   * @return the number of files this strategy should create
   */
  protected long numOutputFiles(long totalSizeInBytes) {
    if (totalSizeInBytes < targetFileSize) {
      return 1;
    }

    long fileCountWithRemainder = LongMath.divide(totalSizeInBytes, targetFileSize, RoundingMode.CEILING);
    if (LongMath.mod(totalSizeInBytes, targetFileSize) > minFileSize) {
      // Our Remainder file is of valid size for this compaction so keep it
      return fileCountWithRemainder;
    }

    long fileCountWithoutRemainder = LongMath.divide(totalSizeInBytes, targetFileSize, RoundingMode.FLOOR);
    long avgFileSizeWithoutRemainder = totalSizeInBytes / fileCountWithoutRemainder;
    if (avgFileSizeWithoutRemainder < 1.1 * targetFileSize) {
      // Round down and distribute remainder amongst other files
      return fileCountWithoutRemainder;
    } else {
      // Keep the remainder file
      return fileCountWithRemainder;
    }
  }

  /**
   * Returns the smaller of our max write file threshold, and our estimated split size based on
   * the number of output files we want to generate.
   */
  protected long splitSize(long totalSizeInBytes) {
    long estimatedSplitSize = totalSizeInBytes / numOutputFiles(totalSizeInBytes);
    return Math.min(estimatedSplitSize, writeMaxFileSize());
  }

  protected static long inputFileSize(List<FileScanTask> fileToRewrite) {
    return fileToRewrite.stream().mapToLong(FileScanTask::length).sum();
  }


  /**
   * Ideally every Spark Task that is generated will be less than or equal to our target size but
   * in practice this is not the case. When we actually write our files, they may exceed the target
   * size and end up being split. This would end up producing 2 files out of one task, one target sized
   * and one very small file. Since the output file can vary in size, it is better to
   * use a slightly larger (but still within threshold) size for actually writing the tasks out.
   * This helps us in the case where our estimate for the task size is under the target size but the
   * actual written file size is slightly larger.
   * @return the target size plus one half of the distance between max and target
   */
  protected long writeMaxFileSize() {
    return (long) (this.targetFileSize + ((this.maxFileSize - this.targetFileSize) * 0.5));
  }
}
