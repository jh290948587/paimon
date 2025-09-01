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

package org.apache.paimon.lookup;

import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.utils.ListDelimitedSerializer;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** RocksDB state for key -> List of value. */
public class RocksDBListState<K, V> extends RocksDBState<K, V, List<V>> {

    private final ListDelimitedSerializer listSerializer = new ListDelimitedSerializer();

    public RocksDBListState(
            RocksDBStateFactory stateFactory,
            ColumnFamilyHandle columnFamily,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize) {
        super(stateFactory, columnFamily, keySerializer, valueSerializer, lruCacheSize);
    }

    public void add(K key, V value) throws IOException {
        byte[] keyBytes = serializeKey(key);
        byte[] valueBytes = serializeValue(value);
        try {
            // 插入到 rocksdb 中
            db.merge(columnFamily, writeOptions, keyBytes, valueBytes);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
        // TODO WHY? 由于有新数据了，这个 key 对应的记录就要从 cache 中删掉（随后验证下这里是不是删除数据）
        cache.invalidate(wrap(keyBytes));
    }

    public List<V> get(K key) throws IOException {
        byte[] keyBytes = serializeKey(key);
        // 先看下 Cache 里有没有，没有的话就从 Rocksdb 中去 Get 并且将结果缓存起来
        return cache.get(
                wrap(keyBytes),
                k -> {
                    byte[] valueBytes;
                    try {
                        valueBytes = db.get(columnFamily, keyBytes);
                    } catch (RocksDBException e) {
                        throw new RuntimeException(e);
                    }
                    List<V> rows = listSerializer.deserializeList(valueBytes, valueSerializer);
                    if (rows == null) {
                        return Collections.emptyList();
                    }
                    return rows;
                });
    }

    public byte[] serializeValue(V value) throws IOException {
        valueOutputView.clear();
        valueSerializer.serialize(value, valueOutputView);
        return valueOutputView.getCopyOfBuffer();
    }

    public byte[] serializeList(List<byte[]> valueList) throws IOException {
        return listSerializer.serializeList(valueList);
    }
}
