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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.flink.FlinkConnectorOptions.LookupCacheMode;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.utils.TableScanUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.OutOfRangeException;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import org.apache.paimon.shade.guava30.com.google.common.primitives.Ints;

import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_CACHE_MODE;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_REFRESH_TIME_PERIODS_BLACKLIST;
import static org.apache.paimon.flink.query.RemoteTableQuery.isRemoteServiceAvailable;
import static org.apache.paimon.lookup.RocksDBOptions.LOOKUP_CACHE_ROWS;
import static org.apache.paimon.lookup.RocksDBOptions.LOOKUP_CONTINUOUS_DISCOVERY_INTERVAL;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.predicate.PredicateBuilder.transformFieldMapping;

/** A lookup {@link TableFunction} for file store. */
public class FileStoreLookupFunction implements Serializable, Closeable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreLookupFunction.class);

    private final Table table;
    @Nullable private final DynamicPartitionLoader partitionLoader;
    private final List<String> projectFields;
    private final List<String> joinKeys;
    @Nullable private final Predicate predicate;
    @Nullable private final RefreshBlacklist refreshBlacklist;

    private transient File path;
    private transient LookupTable lookupTable;

    // interval of refreshing lookup table
    private transient Duration refreshInterval;
    // timestamp when refreshing lookup table
    private transient long nextRefreshTime;

    protected FunctionContext functionContext;

    @Nullable private Filter<InternalRow> cacheRowFilter;

    public FileStoreLookupFunction(
            Table table, int[] projection, int[] joinKeyIndex, @Nullable Predicate predicate) {
        if (!TableScanUtils.supportCompactDiffStreamingReading(table)) {
            TableScanUtils.streamingReadingValidate(table);
        }

        this.table = table;
        this.partitionLoader = DynamicPartitionLoader.of(table);

        // join keys are based on projection fields
        this.joinKeys =
                Arrays.stream(joinKeyIndex)
                        .mapToObj(i -> table.rowType().getFieldNames().get(projection[i]))
                        .collect(Collectors.toList());

        this.projectFields =
                Arrays.stream(projection)
                        .mapToObj(i -> table.rowType().getFieldNames().get(i))
                        .collect(Collectors.toList());

        // add primary keys
        for (String field : table.primaryKeys()) {
            if (!projectFields.contains(field)) {
                projectFields.add(field);
            }
        }

        if (partitionLoader != null) {
            partitionLoader.addPartitionKeysTo(joinKeys, projectFields);
        }

        this.predicate = predicate;

        this.refreshBlacklist =
                RefreshBlacklist.create(
                        table.options().get(LOOKUP_REFRESH_TIME_PERIODS_BLACKLIST.key()));
    }

    public void open(FunctionContext context) throws Exception {
        this.functionContext = context;
        String tmpDirectory = getTmpDirectory(context);
        open(tmpDirectory);
    }

    // we tag this method friendly for testing
    void open(String tmpDirectory) throws Exception {
        this.path = new File(tmpDirectory, "lookup-" + UUID.randomUUID());
        if (!path.mkdirs()) {
            throw new RuntimeException("Failed to create dir: " + path);
        }
        open();
    }

    private void open() throws Exception {
        this.nextRefreshTime = -1;

        Options options = Options.fromMap(table.options());
        this.refreshInterval =
                options.getOptional(LOOKUP_CONTINUOUS_DISCOVERY_INTERVAL)
                        .orElse(options.get(CONTINUOUS_DISCOVERY_INTERVAL));

        List<String> fieldNames = table.rowType().getFieldNames();
        int[] projection = projectFields.stream().mapToInt(fieldNames::indexOf).toArray();
        FileStoreTable storeTable = (FileStoreTable) table;

        // 1、根据不同配置初始化 LookupTable
        // lookup.cache 为 AUTO 且表的主键就是 join key 时，走按需加载的 LookupTable
        if (options.get(LOOKUP_CACHE_MODE) == LookupCacheMode.AUTO
                && new HashSet<>(table.primaryKeys()).equals(new HashSet<>(joinKeys))) {
            if (isRemoteServiceAvailable(storeTable)) {
                // 用 flink 服务来做 lookup join
                this.lookupTable =
                        PrimaryKeyPartialLookupTable.createRemoteTable(
                                storeTable, projection, joinKeys);
            } else {
                try {
                    // 把 datafile 写到本地，然后再查询
                    this.lookupTable =
                            PrimaryKeyPartialLookupTable.createLocalTable(
                                    storeTable,
                                    projection,
                                    path,
                                    joinKeys,
                                    getRequireCachedBucketIds());
                } catch (UnsupportedOperationException ignore2) {
                }
            }
        }

        // 都不满足的情况下，走全量加载的 LookupTable
        if (lookupTable == null) {
            // Full Cache 的方式，需要初始化全量数据
            FullCacheLookupTable.Context context =
                    new FullCacheLookupTable.Context(
                            storeTable,
                            projection,
                            predicate,
                            createProjectedPredicate(projection),
                            path,
                            joinKeys,
                            getRequireCachedBucketIds());
            this.lookupTable = FullCacheLookupTable.create(context, options.get(LOOKUP_CACHE_ROWS));
        }

        // 2、获取最新的分区加到 filter 中
        if (partitionLoader != null) {
            partitionLoader.open();
            partitionLoader.checkRefresh();
            // 分区表在读 snapshot 来更新 rocksdb 时，会指定最新的 partition 作为过滤条件
            BinaryRow partition = partitionLoader.partition();
            if (partition != null) {
                lookupTable.specificPartitionFilter(createSpecificPartFilter(partition));
            }
        }

        if (cacheRowFilter != null) {
            lookupTable.specifyCacheRowFilter(cacheRowFilter);
        }

        // 对 LookupTable 进行 Open 操作
        lookupTable.open();
    }

    @Nullable
    private Predicate createProjectedPredicate(int[] projection) {
        Predicate adjustedPredicate = null;
        if (predicate != null) {
            // adjust to projection index
            adjustedPredicate =
                    transformFieldMapping(
                                    this.predicate,
                                    IntStream.range(0, table.rowType().getFieldCount())
                                            .map(i -> Ints.indexOf(projection, i))
                                            .toArray())
                            .orElse(null);
        }
        return adjustedPredicate;
    }

    public Collection<RowData> lookup(RowData keyRow) {
        try {
            // 在 lookup 之前，先要尝试刷新，拿到最新的数据
            tryRefresh();

            InternalRow key = new FlinkRowWrapper(keyRow);
            if (partitionLoader != null) {
                if (partitionLoader.partition() == null) {
                    return Collections.emptyList();
                }
                key = JoinedRow.join(key, partitionLoader.partition());
            }

            // 根据 key 获取结果行
            List<InternalRow> results = lookupTable.get(key);
            List<RowData> rows = new ArrayList<>(results.size());
            for (InternalRow matchedRow : results) {
                rows.add(new FlinkRowData(matchedRow));
            }
            return rows;
        } catch (OutOfRangeException | ReopenException e) {
            reopen();
            return lookup(keyRow);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Predicate createSpecificPartFilter(BinaryRow partition) {
        RowType rowType = table.rowType();
        List<String> partitionKeys = table.partitionKeys();
        Object[] partitionSpec =
                new RowDataToObjectArrayConverter(rowType.project(partitionKeys))
                        .convert(partition);
        Map<String, Object> partitionMap = new HashMap<>(partitionSpec.length);
        for (int i = 0; i < partitionSpec.length; i++) {
            partitionMap.put(partitionKeys.get(i), partitionSpec[i]);
        }

        // create partition predicate base on rowType instead of partitionType
        return createPartitionPredicate(rowType, partitionMap);
    }

    private void reopen() {
        try {
            close();
            open();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    void tryRefresh() throws Exception {
        // 1. check if this time is in black list
        // 判断当前时间是否在 Blacklist 里配置的时间，如果在的话，则不刷新
        if (refreshBlacklist != null && !refreshBlacklist.canRefresh()) {
            return;
        }

        // 2. refresh dynamic partition
        if (partitionLoader != null) {
            // 检查当前最大分区，和之前分区是否相同，如果不相同，说明最大分区已经变了
            boolean partitionChanged = partitionLoader.checkRefresh();
            BinaryRow partition = partitionLoader.partition();
            if (partition == null) {
                // no data to be load, fast exit
                return;
            }

            // 分区变了，所以要重新清理rocksdb
            if (partitionChanged) {
                // reopen with latest partition
                // 设置为最新的分区，并重新进行 reopen
                lookupTable.specificPartitionFilter(createSpecificPartFilter(partition));
                lookupTable.close();
                lookupTable.open();
                // no need to refresh the lookup table because it is reopened
                return;
            }
        }

        // 3. refresh lookup table
        // 如果已经到了下次刷新时间，则开始 refresh
        if (shouldRefreshLookupTable()) {
            // 不同 LookupTable 的实现有不同的刷新策略
            lookupTable.refresh();
            nextRefreshTime = System.currentTimeMillis() + refreshInterval.toMillis();
        }
    }

    private boolean shouldRefreshLookupTable() {
        if (nextRefreshTime > System.currentTimeMillis()) {
            return false;
        }

        if (nextRefreshTime > 0) {
            LOG.info(
                    "Lookup table {} has refreshed after {} second(s), refreshing",
                    table.name(),
                    refreshInterval.toMillis() / 1000);
        }
        return true;
    }

    @VisibleForTesting
    LookupTable lookupTable() {
        return lookupTable;
    }

    @VisibleForTesting
    long nextBlacklistCheckTime() {
        return refreshBlacklist == null ? -1 : refreshBlacklist.nextBlacklistCheckTime();
    }

    @Override
    public void close() throws IOException {
        if (lookupTable != null) {
            lookupTable.close();
            lookupTable = null;
        }

        if (path != null) {
            FileIOUtils.deleteDirectoryQuietly(path);
        }
    }

    private static String getTmpDirectory(FunctionContext context) {
        try {
            Field field = context.getClass().getDeclaredField("context");
            field.setAccessible(true);
            StreamingRuntimeContext runtimeContext =
                    extractStreamingRuntimeContext(field.get(context));
            String[] tmpDirectories =
                    runtimeContext.getTaskManagerRuntimeInfo().getTmpDirectories();
            return tmpDirectories[ThreadLocalRandom.current().nextInt(tmpDirectories.length)];
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static StreamingRuntimeContext extractStreamingRuntimeContext(Object runtimeContext)
            throws NoSuchFieldException, IllegalAccessException {
        if (runtimeContext instanceof StreamingRuntimeContext) {
            return (StreamingRuntimeContext) runtimeContext;
        }

        Field field = runtimeContext.getClass().getDeclaredField("runtimeContext");
        field.setAccessible(true);
        return extractStreamingRuntimeContext(field.get(runtimeContext));
    }

    /**
     * Get the set of bucket IDs that need to be cached by the current lookup join subtask.
     *
     * <p>The Flink Planner will distribute data to lookup join nodes based on buckets. This allows
     * paimon to cache only the necessary buckets for each subtask, improving efficiency.
     *
     * @return the set of bucket IDs to be cached
     */
    protected Set<Integer> getRequireCachedBucketIds() {
        // TODO: Implement the method when Flink support bucket shuffle for lookup join.
        return null;
    }

    protected void setCacheRowFilter(@Nullable Filter<InternalRow> cacheRowFilter) {
        this.cacheRowFilter = cacheRowFilter;
    }
}
