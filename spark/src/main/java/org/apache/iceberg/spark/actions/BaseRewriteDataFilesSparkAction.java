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

package org.apache.iceberg.spark.actions;

import java.io.IOException;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BinPackStrategy;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteStrategy;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.relocated.com.google.common.math.IntMath;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseRewriteDataFilesSparkAction
    extends BaseSnapshotUpdateSparkAction<RewriteDataFiles, RewriteDataFiles.Result> implements RewriteDataFiles {

  private static final Logger LOG = LoggerFactory.getLogger(BaseRewriteDataFilesSparkAction.class);
  private static final Set<String> VALID_OPTIONS = ImmutableSet.of(
      MAX_CONCURRENT_FILE_GROUP_ACTIONS,
      MAX_FILE_GROUP_SIZE_BYTES,
      PARTIAL_PROGRESS_ENABLED,
      PARTIAL_PROGRESS_MAX_COMMITS,
      TARGET_FILE_SIZE_BYTES
  );

  private final Table table;

  private RewriteDataFiles.Strategy strategyType = Strategy.BINPACK;
  private Expression filter = Expressions.alwaysTrue();
  private int maxConcurrentFileGroupActions;
  private int maxCommits;
  private boolean partialProgressEnabled;

  protected BaseRewriteDataFilesSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
  }

  protected Table table() {
    return table;
  }

  protected abstract DataFilesRewriter newRewriter(RewriteStrategy strategy);

  protected interface DataFilesRewriter {
    Set<DataFile> rewrite(String groupID, List<FileScanTask> fileScanTasks);

    void commit(Set<String> groupIDs);

    void abort(String groupID);
  }

  private RewriteStrategy rewriteStrategy(Strategy type) {
    switch (type) {
      case BINPACK:
        return new BinPackStrategy();
      default:
        throw new IllegalArgumentException("TBD");
    }
  }

  private void commitOrClean(DataFilesRewriter rewriter, Set<String> completedGroupIDs) {
    try {
      rewriter.commit(completedGroupIDs);
    } catch (Exception e) {
      LOG.error("Cannot commit groups {}, attempting to clean up written files", completedGroupIDs, e);
      Tasks.foreach(completedGroupIDs)
          .suppressFailureWhenFinished()
          .run(rewriter::abort);
      throw e;
    }
  }

  @Override
  public RewriteDataFiles strategy(Strategy type) {
    strategyType = type;
    return this;
  }

  @Override
  public RewriteDataFiles filter(Expression expression) {
    filter = Expressions.and(filter, expression);
    return this;
  }

  @VisibleForTesting
  void rewriteFiles(Pair<FileGroupInfo, List<FileScanTask>> infoListPair, int totalGroups,
                    Map<StructLike, Integer> numGroupsPerPartition, DataFilesRewriter rewriter,
                    RewriteStrategy strategy, ConcurrentLinkedQueue<String> completedRewrite,
                    ConcurrentHashMap<String, Pair<FileGroupInfo, FileGroupRewriteResult>> results) {

    FileGroupInfo fileGroupInfo = infoListPair.first();
    String groupID = fileGroupInfo.groupID();
    List<FileScanTask> files = infoListPair.second();
    String desc = jobDesc(fileGroupInfo, totalGroups, files.size(),
        numGroupsPerPartition.get(fileGroupInfo.partition), strategy.name());
    JobGroupInfo jobGroupInfo = newJobGroupInfo(groupID, desc);

    Set<DataFile> addedFiles = withJobGroupInfo(jobGroupInfo, () -> rewriter.rewrite((groupID, files));

    completedRewrite.offer(groupID);
    FileGroupRewriteResult fileGroupResult = new FileGroupRewriteResult(addedFiles.size(), files.size());

    results.put(groupID, Pair.of(fileGroupInfo, fileGroupResult));
  }

  private Result doExecute(Stream<Pair<FileGroupInfo, List<FileScanTask>>> groupStream,
      RewriteStrategy strategy, int totalGroups, Map<StructLike, Integer> numGroupsPerPartition) {

    ExecutorService rewriteService = Executors.newFixedThreadPool(maxConcurrentFileGroupActions,
        new ThreadFactoryBuilder().setNameFormat("Rewrite-Service-%d").build());

    ConcurrentLinkedQueue<String> completedRewrite = new ConcurrentLinkedQueue<>();
    ConcurrentHashMap<String, Pair<FileGroupInfo, FileGroupRewriteResult>> results = new ConcurrentHashMap<>();

    DataFilesRewriter rewriter = newRewriter(strategy);

    Tasks.Builder<Pair<FileGroupInfo, List<FileScanTask>>> rewriteTaskBuilder = Tasks.foreach(groupStream.iterator())
        .executeWith(rewriteService)
        .stopOnFailure()
        .noRetry()
        .onFailure((info, exception) -> {
          LOG.error("Failure during rewrite process for group {}", info.first(), exception);
        });

    try {
      rewriteTaskBuilder
          .run(infoListPair ->
              rewriteFiles(infoListPair, totalGroups, numGroupsPerPartition, rewriter, strategy, completedRewrite, results));
    } catch (Exception e) {
      // At least one rewrite group failed, clean up all completed rewrites
      LOG.error("Cannot complete rewrite, partial progress is not enabled and one of the file set groups failed to " +
          "be rewritten. Cleaning up {} groups which finished being written.", completedRewrite.size(), e);
      Tasks.foreach(completedRewrite)
          .suppressFailureWhenFinished()
          .run(rewriter::abort);
      throw e;
    }

    commitOrClean(rewriter, ImmutableSet.copyOf(completedRewrite));

    return new Result(results.values().stream().collect(Collectors.toMap(Pair::first, Pair::second)));
  }

  private Result doExecutePartialProgress(Stream<Pair<FileGroupInfo, List<FileScanTask>>> groupStream,
      RewriteStrategy strategy, int totalGroups, Map<StructLike, Integer> numGroupsPerPartition) {

    ExecutorService rewriteService = Executors.newFixedThreadPool(maxConcurrentFileGroupActions,
        new ThreadFactoryBuilder().setNameFormat("Rewrite-Service-%d").build());

    ExecutorService committerService = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat("Committer-Service").build());

    int groupsPerCommit = partialProgressEnabled ?
        IntMath.divide(totalGroups, maxCommits, RoundingMode.CEILING) :
        totalGroups;

    AtomicBoolean stillRewriting = new AtomicBoolean(true);
    ConcurrentHashMap<String, Pair<FileGroupInfo, FileGroupRewriteResult>> results = new ConcurrentHashMap<>();
    ConcurrentLinkedQueue<String> completedRewriteIds = new ConcurrentLinkedQueue<>();
    ConcurrentLinkedQueue<String> completedCommitIds = new ConcurrentLinkedQueue<>();

    DataFilesRewriter rewriter = newRewriter(strategy);

    // Partial progress commit service
    committerService.execute(() -> {
      while (stillRewriting.get() || completedRewriteIds.size() > 0) {
        Thread.yield();
        // Either we have a full commit group, or we have completed writing and need to commit what is left over
        if (completedRewriteIds.size() > groupsPerCommit ||
            (!stillRewriting.get() && completedRewriteIds.size() > 0)) {

          Set<String> batch = Sets.newHashSetWithExpectedSize(groupsPerCommit);
          for (int i = 0; i < groupsPerCommit && !completedRewriteIds.isEmpty(); i++) {
            batch.add(completedRewriteIds.poll());
          }

          try {
            commitOrClean(rewriter, batch);
            completedCommitIds.addAll(batch);
          } catch (Exception e) {
            batch.forEach(results::remove);
            LOG.error("Failure during rewrite commit process, partial progress enabled. Ignoring", e);
          }
        }
      }
    });

    // Start rewrite tasks
    Tasks.foreach(groupStream.iterator())
        .suppressFailureWhenFinished()
        .executeWith(rewriteService)
        .noRetry()
        .onFailure((info, exception) -> {
          LOG.error("Failure during rewrite process for group {}", info.first(), exception);
          rewriter.abort(info.first().groupID);
        })
        .run(infoListPair ->
            rewriteFiles(infoListPair, totalGroups, numGroupsPerPartition, rewriter, strategy, completedRewriteIds, results));

    stillRewriting.set(false);
    committerService.shutdown();

    try {
      committerService.awaitTermination(10, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      throw new RuntimeException("Cannot complete commit for rewrite, commit service interuptted", e);
    }

    return new Result(results.values().stream().collect(Collectors.toMap(Pair::first, Pair::second)));
  }

  @Override
  public Result execute() {
    RewriteStrategy strategy = rewriteStrategy(strategyType).options(this.options());
    validateOptions(strategy);

    Map<StructLike, List<List<FileScanTask>>> fileGroupsByPartition = planFileGroups(strategy);

    Map<StructLike, Integer> numGroupsPerPartition = fileGroupsByPartition.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size())
    );

    Integer totalGroups = numGroupsPerPartition.values().stream().reduce(Integer::sum).orElse(0);

    if (totalGroups == 0) {
      return new Result(Collections.emptyMap());
    }

    Map<StructLike, Integer> partitionIndex = new HashMap<>();
    AtomicInteger jobIndex = new AtomicInteger(1);

    // Todo Check if we need to randomize the order in which we do jobs, instead of being partition centric
    Stream<Pair<FileGroupInfo, List<FileScanTask>>> groupStream = fileGroupsByPartition.entrySet().stream()
        .flatMap(
            e -> e.getValue().stream().map(tasks -> {
              int myJobIndex = jobIndex.getAndIncrement();
              int myPartIndex = partitionIndex.merge(e.getKey(), 1, Integer::sum);
              String groupID = UUID.randomUUID().toString();
              return Pair.of(new FileGroupInfo(groupID, myJobIndex, myPartIndex, e.getKey()), tasks);
            }));

    if (partialProgressEnabled) {
      return doExecutePartialProgress(groupStream, strategy, totalGroups, numGroupsPerPartition);
    } else {
      return doExecute(groupStream, strategy, totalGroups, numGroupsPerPartition);
    }
  }

  private Map<StructLike, List<List<FileScanTask>>> planFileGroups(RewriteStrategy strategy) {
    CloseableIterable<FileScanTask> fileScanTasks = table.newScan()
        .filter(filter)
        .ignoreResiduals()
        .planFiles();

    try {
      Map<StructLike, List<FileScanTask>> filesByPartition = Streams.stream(fileScanTasks)
          .collect(Collectors.groupingBy(task -> task.file().partition()));

      Map<StructLike, List<List<FileScanTask>>> fileGroupsByPartition = Maps.newHashMap();

      filesByPartition.forEach((partition, tasks) -> {
        List<List<FileScanTask>> fileGroups = toFileGroups(tasks, strategy);
        if (fileGroups.size() > 0) {
          fileGroupsByPartition.put(partition, fileGroups);
        }
      });

      return fileGroupsByPartition;
    } finally {
      try {
        fileScanTasks.close();
      } catch (IOException io) {
        LOG.error("Cannot properly close file scan tasks while planning the rewrite", io);
      }
    }
  }

  private List<List<FileScanTask>> toFileGroups(List<FileScanTask> tasks, RewriteStrategy strategy) {
    Iterable<FileScanTask> filteredTasks = strategy.selectFilesToRewrite(tasks);
    Iterable<List<FileScanTask>> groupedTasks = strategy.planFileGroups(filteredTasks);
    return ImmutableList.copyOf(groupedTasks);
  }

  private void validateOptions(RewriteStrategy strategy) {
    Set<String> validOptions = Sets.newHashSet(strategy.validOptions());
    validOptions.addAll(VALID_OPTIONS);

    Set<String> invalidKeys = Sets.newHashSet(options().keySet());
    invalidKeys.removeAll(validOptions);

    Preconditions.checkArgument(invalidKeys.isEmpty(),
        "Cannot use options %s, they are not supported by RewriteDatafiles or the strategy %s",
        invalidKeys, strategy.name());

    maxConcurrentFileGroupActions = PropertyUtil.propertyAsInt(options(),
        MAX_CONCURRENT_FILE_GROUP_ACTIONS,
        MAX_CONCURRENT_FILE_GROUP_ACTIONS_DEFAULT);

    maxCommits = PropertyUtil.propertyAsInt(options(),
        PARTIAL_PROGRESS_MAX_COMMITS,
        PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT);

    partialProgressEnabled = PropertyUtil.propertyAsBoolean(options(),
        PARTIAL_PROGRESS_ENABLED,
        PARTIAL_PROGRESS_ENABLED_DEFAULT);

    Preconditions.checkArgument(maxConcurrentFileGroupActions >= 1,
        "Cannot set %s to %s, the value must be positive.",
        MAX_CONCURRENT_FILE_GROUP_ACTIONS, maxConcurrentFileGroupActions);

    Preconditions.checkArgument(!partialProgressEnabled || partialProgressEnabled && maxCommits > 0,
        "Cannot set %s to %s, the value must be positive when %s is true",
        PARTIAL_PROGRESS_MAX_COMMITS, maxCommits, PARTIAL_PROGRESS_ENABLED);
  }

  private String jobDesc(FileGroupInfo fileGroupInfo, int totalGroups,
      int numFilesToRewrite, int numFilesPerPartition, String strategyName) {

    return String.format("Rewrite %s, FileGroup %d/%d : Partition %s:%d/%d : Rewriting %d files : Table %s",
        strategyName, fileGroupInfo.globalIndex, totalGroups, fileGroupInfo.partition, fileGroupInfo.partitionIndex,
        numFilesPerPartition, numFilesToRewrite, table.name());
  }

  class Result implements RewriteDataFiles.Result {
    private final Map<RewriteDataFiles.FileGroupInfo, RewriteDataFiles.FileGroupRewriteResult> resultMap;

    Result(
        Map<RewriteDataFiles.FileGroupInfo, RewriteDataFiles.FileGroupRewriteResult> resultMap) {
      this.resultMap = resultMap;
    }

    @Override
    public Map<RewriteDataFiles.FileGroupInfo, RewriteDataFiles.FileGroupRewriteResult> resultMap() {
      return resultMap;
    }
  }

  class FileGroupInfo implements RewriteDataFiles.FileGroupInfo {

    private final String groupID;
    private final int globalIndex;
    private final int partitionIndex;
    private final StructLike partition;

    FileGroupInfo(String groupID, int globalIndex, int partitionIndex, StructLike partition) {
      this.groupID = groupID;
      this.globalIndex = globalIndex;
      this.partitionIndex = partitionIndex;
      this.partition = partition;
    }

    @Override
    public int globalIndex() {
      return globalIndex;
    }

    @Override
    public int partitionIndex() {
      return partitionIndex;
    }

    @Override
    public StructLike partition() {
      return partition;
    }

    @Override
    public String toString() {
      return "FileGroupInfo{" +
          "groupID=" + groupID +
          ", globalIndex=" + globalIndex +
          ", partitionIndex=" + partitionIndex +
          ", partition=" + partition +
          '}';
    }

    public String groupID() {
      return groupID;
    }
  }

  class FileGroupRewriteResult implements RewriteDataFiles.FileGroupRewriteResult {
    private final int addedDataFilesCount;
    private final int rewrittenDataFilesCount;

    FileGroupRewriteResult(int addedDataFilesCount, int rewrittenDataFilesCount) {
      this.addedDataFilesCount = addedDataFilesCount;
      this.rewrittenDataFilesCount = rewrittenDataFilesCount;
    }

    @Override
    public int addedDataFilesCount() {
      return this.addedDataFilesCount;
    }

    @Override
    public int rewrittenDataFilesCount() {
      return this.rewrittenDataFilesCount;
    }
  }
}
