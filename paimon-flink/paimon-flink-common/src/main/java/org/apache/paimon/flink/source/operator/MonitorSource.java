/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.source.operator;

import org.apache.paimon.flink.NestedProjectedRowData;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.flink.source.SplitListState;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.OptionalLong;
import java.util.TreeMap;

import static org.apache.paimon.table.BucketMode.BUCKET_UNAWARE;

/**
 * This is the single (non-parallel) monitoring task, it is responsible for:
 *
 * <ol>
 *   <li>Monitoring snapshots of the Paimon table.
 *   <li>Creating the {@link Split splits} corresponding to the incremental files
 *   <li>Assigning them to downstream tasks for further processing.
 * </ol>
 *
 * <p>The splits to be read are forwarded to the downstream {@link ReadOperator} which can have
 * parallelism greater than one.
 *
 * <p>Currently, there are two features that rely on this monitor:
 *
 * <ol>
 *   <li>Consumer-id: rely on this source to do aligned snapshot consumption, and ensure that all
 *       data in a snapshot is consumed within each checkpoint.
 *   <li>Snapshot-watermark: when there is no watermark definition, the default Paimon table will
 *       pass the watermark recorded in the snapshot.
 * </ol>
 */
public class MonitorSource extends AbstractNonCoordinatedSource<Split> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(MonitorSource.class);

    private final ReadBuilder readBuilder;
    private final long monitorInterval;
    private final boolean emitSnapshotWatermark;

    public MonitorSource(
            ReadBuilder readBuilder, long monitorInterval, boolean emitSnapshotWatermark) {
        this.readBuilder = readBuilder;
        this.monitorInterval = monitorInterval;
        this.emitSnapshotWatermark = emitSnapshotWatermark;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<Split, SimpleSourceSplit> createReader(
            SourceReaderContext sourceReaderContext) throws Exception {
        return new Reader();
    }

    private class Reader extends AbstractNonCoordinatedSourceReader<Split> {
        private static final String CHECKPOINT_STATE = "CS";
        private static final String NEXT_SNAPSHOT_STATE = "NSS";

        private final StreamTableScan scan = readBuilder.newStreamScan();
        private final SplitListState<Long> checkpointState =
                new SplitListState<>(CHECKPOINT_STATE, x -> Long.toString(x), Long::parseLong);
        private final SplitListState<Tuple2<Long, Long>> nextSnapshotState =
                new SplitListState<>(
                        NEXT_SNAPSHOT_STATE,
                        x -> x.f0 + ":" + x.f1,
                        x ->
                                Tuple2.of(
                                        Long.parseLong(x.split(":")[0]),
                                        Long.parseLong(x.split(":")[1])));
        private final TreeMap<Long, Long> nextSnapshotPerCheckpoint = new TreeMap<>();

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            // 获取所有已完成的 checkpoint Id <= checkpointId 的 map
            NavigableMap<Long, Long> nextSnapshots =
                    nextSnapshotPerCheckpoint.headMap(checkpointId, true);
            // 找出所有 snapshot 中最大的 snapshotId，表示这个 snapshotId 之前的 Snapshot 都被消费完了，在写入作业的 SnapshotExpire 相关逻辑中可以 Expire 掉这个 SnapshotId 之前的 Snapshot 了
            OptionalLong max = nextSnapshots.values().stream().mapToLong(Long::longValue).max();
            // 更新 consumer 文件中的 nextSnapshot
            max.ifPresent(scan::notifyCheckpointComplete);
            nextSnapshots.clear();
        }

        @Override
        public List<SimpleSourceSplit> snapshotState(long checkpointId) {
            this.checkpointState.clear();
            // 获取 nextSnapshotId
            Long nextSnapshot = this.scan.checkpoint();
            if (nextSnapshot != null) {
                // 将 nextSnapshotId 加入到 state 中
                this.checkpointState.add(nextSnapshot);
                // 将 cpId 和 nextSnapshotId 加入到 treeMap 中，然后更新 state
                this.nextSnapshotPerCheckpoint.put(checkpointId, nextSnapshot);
            }

            List<Tuple2<Long, Long>> nextSnapshots = new ArrayList<>();
            this.nextSnapshotPerCheckpoint.forEach((k, v) -> nextSnapshots.add(new Tuple2<>(k, v)));
            this.nextSnapshotState.update(nextSnapshots);

            if (LOG.isDebugEnabled()) {
                LOG.debug("{} checkpoint {}.", getClass().getSimpleName(), nextSnapshot);
            }

            List<SimpleSourceSplit> results = new ArrayList<>();
            results.addAll(checkpointState.snapshotState());
            results.addAll(nextSnapshotState.snapshotState());
            return results;
        }

        @Override
        public void addSplits(List<SimpleSourceSplit> list) {
            LOG.info("Restoring state for the {}.", getClass().getSimpleName());
            checkpointState.restoreState(list);
            nextSnapshotState.restoreState(list);

            List<Long> retrievedStates = checkpointState.get();

            // given that the parallelism of the source is 1, we can only have 1 retrieved items.
            Preconditions.checkArgument(
                    retrievedStates.size() <= 1,
                    getClass().getSimpleName() + " retrieved invalid state.");

            if (retrievedStates.size() == 1) {
                this.scan.restore(retrievedStates.get(0));
            }

            for (Tuple2<Long, Long> tuple2 : nextSnapshotState.get()) {
                nextSnapshotPerCheckpoint.put(tuple2.f0, tuple2.f1);
            }
        }

        @Override
        public InputStatus pollNext(ReaderOutput<Split> readerOutput) throws Exception {
            boolean isEmpty;
            try {
                // 对下一个 snapshot 求 plan，进而得到 splits
                List<Split> splits = scan.plan().splits();
                isEmpty = splits.isEmpty();
                // 把 splits 发送给下游的 ReadOperator
                splits.forEach(readerOutput::collect);

                // 如果 emitSnapshotWatermark 为 true，则将当前消费的 snapshot 的 watermark 发送到下游所有并发
                if (emitSnapshotWatermark) {
                    Long watermark = scan.watermark();
                    if (watermark != null) {
                        readerOutput.emitWatermark(new Watermark(watermark));
                    }
                }
            } catch (EndOfScanException esf) {
                LOG.info("Catching EndOfStreamException, the stream is finished.");
                return InputStatus.END_OF_INPUT;
            }

            if (isEmpty) {
                // 当前消费速度比上游产出 snapshot 速度快，则 sleep 10s
                Thread.sleep(monitorInterval);
            }
            return InputStatus.MORE_AVAILABLE;
        }
    }

    public static DataStream<RowData> buildSource(
            StreamExecutionEnvironment env,
            String name,
            TypeInformation<RowData> typeInfo,
            ReadBuilder readBuilder,
            long monitorInterval,
            boolean emitSnapshotWatermark,
            boolean shuffleBucketWithPartition,
            BucketMode bucketMode,
            NestedProjectedRowData nestedProjectedRowData) {
        SingleOutputStreamOperator<Split> singleOutputStreamOperator =
                env.fromSource(
                                new MonitorSource(
                                        readBuilder, monitorInterval, emitSnapshotWatermark), // 实例化 MonitorSource，传入 readBuilder，用来构建读取器
                                WatermarkStrategy.noWatermarks(), // 无水印策略
                                name + "-Monitor", // 数据源名称
                                new JavaTypeInfo<>(Split.class)) // 自定义 Split 类型，用来传输 Split 信息
                        .forceNonParallel();

        DataStream<Split> sourceDataStream =
                bucketMode == BUCKET_UNAWARE
                        ? shuffleUnwareBucket(singleOutputStreamOperator)
                        : shuffleNonUnwareBucket(
                                singleOutputStreamOperator, shuffleBucketWithPartition); // 非 BUCKET_UNAWARE 的 shuffle 策略

        return sourceDataStream.transform(
                name + "-Reader", typeInfo, new ReadOperator(readBuilder, nestedProjectedRowData)); // ReadOperator 读取 split 文件，然后转为 Flink RowData 发送给下游 Operator
    }

    private static DataStream<Split> shuffleUnwareBucket(
            SingleOutputStreamOperator<Split> singleOutputStreamOperator) {
        return singleOutputStreamOperator.rebalance();
    }

    private static DataStream<Split> shuffleNonUnwareBucket(
            SingleOutputStreamOperator<Split> singleOutputStreamOperator,
            boolean shuffleBucketWithPartition) {
        return singleOutputStreamOperator.partitionCustom(
                (key, numPartitions) -> {
                    if (shuffleBucketWithPartition) {
                        // 同时考虑分区和Bucket
                        return ChannelComputer.select(key.f0, key.f1, numPartitions);
                    }
                    // 只考虑Bucket
                    return ChannelComputer.select(key.f1, numPartitions);
                },
                split -> {
                    // 从 DataSplit 中提取分区和Bucket信息，返回 Tuple2<partition, bucket> 作为分区键
                    DataSplit dataSplit = (DataSplit) split;
                    return Tuple2.of(dataSplit.partition(), dataSplit.bucket());
                });
    }
}
