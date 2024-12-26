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

package org.apache.paimon.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.data.RowData;

import java.util.function.Function;

/** Paimon {@link DataStreamSinkProvider}. */
public class PaimonDataStreamSinkProvider implements DataStreamSinkProvider {

    private final Function<DataStream<RowData>, DataStreamSink<?>> producer;
    // 先给SinkProvider传入一个函数，之后flink调用consumeDataStream方法时，直接调用函数即可
    public PaimonDataStreamSinkProvider(Function<DataStream<RowData>, DataStreamSink<?>> producer) {
        this.producer = producer;
    }

    @Override
    public DataStreamSink<?> consumeDataStream(
            ProviderContext providerContext, DataStream<RowData> dataStream) {
        return producer.apply(dataStream);
    }
}
