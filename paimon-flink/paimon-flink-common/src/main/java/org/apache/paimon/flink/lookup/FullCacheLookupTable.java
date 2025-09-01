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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.lookup.BulkLoader;
import org.apache.paimon.lookup.RocksDBState;
import org.apache.paimon.lookup.RocksDBStateFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.ExecutorUtils;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.PartialRow;
import org.apache.paimon.utils.TypeUtils;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_REFRESH_ASYNC;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_REFRESH_ASYNC_PENDING_SNAPSHOT_COUNT;

/** Lookup table of full cache. */
public abstract class FullCacheLookupTable implements LookupTable {
    private static final Logger LOG = LoggerFactory.getLogger(FullCacheLookupTable.class);

    protected final Object lock = new Object();
    protected final Context context;
    protected final RowType projectedType;
    protected final boolean refreshAsync;

    @Nullable protected final FieldsComparator userDefinedSeqComparator;
    protected final int appendUdsFieldNumber;

    protected RocksDBStateFactory stateFactory;
    @Nullable private final ExecutorService refreshExecutor;
    private final AtomicReference<Exception> cachedException;
    private final int maxPendingSnapshotCount;
    private final FileStoreTable table;
    private Future<?> refreshFuture;
    private LookupStreamingReader reader;
    private Predicate specificPartition;
    @Nullable private Filter<InternalRow> cacheRowFilter;

    public FullCacheLookupTable(Context context) {
        this.table = context.table;
        List<String> sequenceFields = new ArrayList<>();
        CoreOptions coreOptions = new CoreOptions(table.options());
        if (table.primaryKeys().size() > 0) {
            sequenceFields = coreOptions.sequenceField();
        }
        RowType projectedType = TypeUtils.project(table.rowType(), context.projection);
        if (sequenceFields.size() > 0) {
            RowType.Builder builder = RowType.builder();
            projectedType.getFields().forEach(f -> builder.field(f.name(), f.type()));
            RowType rowType = table.rowType();
            AtomicInteger appendUdsFieldNumber = new AtomicInteger(0);
            sequenceFields.stream()
                    .filter(projectedType::notContainsField)
                    .map(rowType::getField)
                    .forEach(
                            f -> {
                                appendUdsFieldNumber.incrementAndGet();
                                builder.field(f.name(), f.type());
                            });
            projectedType = builder.build();
            context = context.copy(table.rowType().getFieldIndices(projectedType.getFieldNames()));
            this.userDefinedSeqComparator =
                    UserDefinedSeqComparator.create(
                            projectedType,
                            sequenceFields,
                            coreOptions.sequenceFieldSortOrderIsAscending());
            this.appendUdsFieldNumber = appendUdsFieldNumber.get();
        } else {
            this.userDefinedSeqComparator = null;
            this.appendUdsFieldNumber = 0;
        }

        this.context = context;

        Options options = Options.fromMap(context.table.options());
        this.projectedType = projectedType;
        this.refreshAsync = options.get(LOOKUP_REFRESH_ASYNC);
        this.refreshExecutor =
                this.refreshAsync
                        ? Executors.newSingleThreadExecutor(
                                new ExecutorThreadFactory(
                                        String.format(
                                                "%s-lookup-refresh",
                                                Thread.currentThread().getName())))
                        : null;
        this.cachedException = new AtomicReference<>();
        this.maxPendingSnapshotCount = options.get(LOOKUP_REFRESH_ASYNC_PENDING_SNAPSHOT_COUNT);
    }

    @Override
    public void specificPartitionFilter(Predicate filter) {
        this.specificPartition = filter;
    }

    @Override
    public void specifyCacheRowFilter(Filter<InternalRow> filter) {
        this.cacheRowFilter = filter;
    }

    protected void openStateFactory() throws Exception {
        this.stateFactory =
                new RocksDBStateFactory(
                        context.tempPath.toString(),
                        context.table.coreOptions().toConfiguration(),
                        null);
    }

    protected void bootstrap() throws Exception {
        Predicate scanPredicate =
                PredicateBuilder.andNullable(context.tablePredicate, specificPartition);
        // 创建流读取器，用来加载数据到 LookupTable
        this.reader =
                new LookupStreamingReader(
                        context.table,
                        context.projection,
                        scanPredicate,
                        context.requiredCachedBucketIds,
                        cacheRowFilter);
        BinaryExternalSortBuffer bulkLoadSorter =
                RocksDBState.createBulkLoadSorter(
                        IOManager.create(context.tempPath.toString()), context.table.coreOptions());
        Predicate predicate = projectedPredicate();
        try (RecordReaderIterator<InternalRow> batch =
                new RecordReaderIterator<>(reader.nextBatch(true))) {
            while (batch.hasNext()) {
                // 从 hdfs 迭代读取数据并写入 inMemorySortBuffer，超过 inMemorySortBuffer 大小则溢写到文件，溢写之前 inMemorySortBuffer 会排序
                InternalRow row = batch.next();
                if (predicate == null || predicate.test(row)) {
                    bulkLoadSorter.write(GenericRow.of(toKeyBytes(row), toValueBytes(row)));
                }
            }
        }

        // 写 SST 之前做一次外部排序，否则 rocksdb 会报错
        MutableObjectIterator<BinaryRow> keyIterator = bulkLoadSorter.sortedIterator();
        BinaryRow row = new BinaryRow(2);
        TableBulkLoader bulkLoader = createBulkLoader();
        try {
            while ((row = keyIterator.next(row)) != null) {
                // 不断的写 SST 文件，最后通过 ingestExternalFile 方法把文件整体加载进 rocksdb，之后就可以提供高效点查服务了
                bulkLoader.write(row.getBinary(0), row.getBinary(1));
            }
        } catch (BulkLoader.WriteException e) {
            throw new RuntimeException(
                    "Exception in bulkLoad, the most suspicious reason is that "
                            + "your data contains duplicates, please check your lookup table. ",
                    e.getCause());
        }

        bulkLoader.finish();
        bulkLoadSorter.clear();
    }

    @Override
    public void refresh() throws Exception {
        // 如果异步刷新线程池为空，则直接执行刷新逻辑（同步）
        if (refreshExecutor == null) {
            doRefresh();
            return;
        }

        // 如果需要异步刷新，则需要比较最新快照 id 和 下一个要读取的快照 id 的差值，如果差值大于阈值，则说明快照数量差距过大，则需要等待上一个异步刷新完成
        Long latestSnapshotId = table.snapshotManager().latestSnapshotId();
        Long nextSnapshotId = reader.nextSnapshotId();
        if (latestSnapshotId != null
                && nextSnapshotId != null
                && latestSnapshotId - nextSnapshotId > maxPendingSnapshotCount) {
            LOG.warn(
                    "The latest snapshot id {} is much greater than the next snapshot id {} for {}}, "
                            + "you may need to increase the parallelism of lookup operator.",
                    latestSnapshotId,
                    nextSnapshotId,
                    maxPendingSnapshotCount);
            if (refreshFuture != null) {
                // Wait the previous refresh task to be finished.
                refreshFuture.get();
            }
            doRefresh();
        } else {
            // 创建一个异步刷新任务
            Future<?> currentFuture = null;
            try {
                currentFuture =
                        refreshExecutor.submit(
                                () -> {
                                    try {
                                        doRefresh();
                                    } catch (Exception e) {
                                        LOG.error(
                                                "Refresh lookup table {} failed",
                                                context.table.name(),
                                                e);
                                        cachedException.set(e);
                                    }
                                });
            } catch (RejectedExecutionException e) {
                LOG.warn("Add refresh task for lookup table {} failed", context.table.name(), e);
            }
            if (currentFuture != null) {
                refreshFuture = currentFuture;
            }
        }
    }

    private void doRefresh() throws Exception {
        while (true) {
            try (RecordReaderIterator<InternalRow> batch =
                    new RecordReaderIterator<>(reader.nextBatch(false))) {
                // 每读出一个 batch 则执行一次 refresh，如果没有下一个则跳出
                if (!batch.hasNext()) {
                    return;
                }
                refresh(batch);
            }
        }
    }

    @Override
    public final List<InternalRow> get(InternalRow key) throws IOException {
        List<InternalRow> values;
        if (refreshAsync) {
            synchronized (lock) {
                values = innerGet(key);
            }
        } else {
            values = innerGet(key);
        }
        if (appendUdsFieldNumber == 0) {
            return values;
        }

        List<InternalRow> dropSequence = new ArrayList<>(values.size());
        for (InternalRow matchedRow : values) {
            dropSequence.add(
                    new PartialRow(matchedRow.getFieldCount() - appendUdsFieldNumber, matchedRow));
        }
        return dropSequence;
    }

    public void refresh(Iterator<InternalRow> input) throws IOException {
        Predicate predicate = projectedPredicate();
        // 遍历 input（就是上层的 batch ）的每条数据，执行 refreshRow
        while (input.hasNext()) {
            InternalRow row = input.next();
            if (refreshAsync) {
                // 异步执行的情况下，refreshRow 方法需要保持同步
                synchronized (lock) {
                    refreshRow(row, predicate);
                }
            } else {
                refreshRow(row, predicate);
            }
        }
    }

    public abstract List<InternalRow> innerGet(InternalRow key) throws IOException;

    protected abstract void refreshRow(InternalRow row, Predicate predicate) throws IOException;

    @Nullable
    public Predicate projectedPredicate() {
        return context.projectedPredicate;
    }

    public abstract byte[] toKeyBytes(InternalRow row) throws IOException;

    public abstract byte[] toValueBytes(InternalRow row) throws IOException;

    public abstract TableBulkLoader createBulkLoader();

    @Override
    public void close() throws IOException {
        try {
            if (refreshExecutor != null) {
                ExecutorUtils.gracefulShutdown(1L, TimeUnit.MINUTES, refreshExecutor);
            }
        } finally {
            stateFactory.close();
            FileIOUtils.deleteDirectory(context.tempPath);
        }
    }

    /** Bulk loader for the table. */
    public interface TableBulkLoader {

        void write(byte[] key, byte[] value) throws BulkLoader.WriteException, IOException;

        void finish() throws IOException;
    }

    static FullCacheLookupTable create(Context context, long lruCacheSize) {
        List<String> primaryKeys = context.table.primaryKeys();
        if (primaryKeys.isEmpty()) {
            return new NoPrimaryKeyLookupTable(context, lruCacheSize);
        } else {
            if (new HashSet<>(primaryKeys).equals(new HashSet<>(context.joinKey))) {
                return new PrimaryKeyLookupTable(context, lruCacheSize, context.joinKey);
            } else {
                return new SecondaryIndexLookupTable(context, lruCacheSize);
            }
        }
    }

    /** Context for {@link LookupTable}. */
    public static class Context {

        public final LookupFileStoreTable table;
        public final int[] projection;
        @Nullable public final Predicate tablePredicate;
        @Nullable public final Predicate projectedPredicate;
        public final File tempPath;
        public final List<String> joinKey;
        public final Set<Integer> requiredCachedBucketIds;

        public Context(
                FileStoreTable table,
                int[] projection,
                @Nullable Predicate tablePredicate,
                @Nullable Predicate projectedPredicate,
                File tempPath,
                List<String> joinKey,
                @Nullable Set<Integer> requiredCachedBucketIds) {
            this.table = new LookupFileStoreTable(table, joinKey);
            this.projection = projection;
            this.tablePredicate = tablePredicate;
            this.projectedPredicate = projectedPredicate;
            this.tempPath = tempPath;
            this.joinKey = joinKey;
            this.requiredCachedBucketIds = requiredCachedBucketIds;
        }

        public Context copy(int[] newProjection) {
            return new Context(
                    table.wrapped(),
                    newProjection,
                    tablePredicate,
                    projectedPredicate,
                    tempPath,
                    joinKey,
                    requiredCachedBucketIds);
        }
    }
}
