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

package org.apache.paimon.sort;

import org.apache.paimon.utils.MutableObjectIterator;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Binary version of {@code MergeIterator}. Use {@code RecordComparator} to compare record. */
public class BinaryMergeIterator<Entry> implements MutableObjectIterator<Entry> {

    // heap over the head elements of the stream
    private final PartialOrderPriorityQueue<HeadStream<Entry>> heap;
    private HeadStream<Entry> currHead;

    public BinaryMergeIterator(
            List<MutableObjectIterator<Entry>> iterators,
            List<Entry> reusableEntries,
            Comparator<Entry> comparator)
            throws IOException {
        checkArgument(iterators.size() == reusableEntries.size());

        // heap 是优先队列，小顶堆，comparator.compare 比较，文件第一个记录更小的 Iterator 排在队列前面
        this.heap =
                new PartialOrderPriorityQueue<>(
                        (o1, o2) -> comparator.compare(o1.getHead(), o2.getHead()),
                        iterators.size());
        for (int i = 0; i < iterators.size(); i++) {
            this.heap.add(new HeadStream<>(iterators.get(i), reusableEntries.get(i)));
        }
    }

    @Override
    public Entry next(Entry reuse) throws IOException {
        // Ignore reuse, because each HeadStream has its own reuse BinaryRow.
        return next();
    }

    @Override
    public Entry next() throws IOException {
        if (currHead != null) {
            if (currHead.noMoreHead()) {
                // 如果队列中堆顶文件已经被读完了，从队列中删除它
                this.heap.poll();
            } else {
                // 没读完一条数据都要调整最小堆，寻找下一个最小记录的文件
                this.heap.adjustTop();
            }
        }

        if (this.heap.size() > 0) {
            currHead = this.heap.peek();
            // 返回的是，对李忠最小堆堆顶的哪个迭代器的头，也就是文件中的第一条记录
            return currHead.getHead();
        } else {
            return null;
        }
    }

    private static final class HeadStream<Entry> {

        private final MutableObjectIterator<Entry> iterator;
        private Entry head;

        // 针对文件内容的迭代器，前面封装好后传到这里的
        private HeadStream(MutableObjectIterator<Entry> iterator, Entry head) throws IOException {
            this.iterator = iterator;
            this.head = head;
            if (noMoreHead()) {
                throw new IllegalStateException();
            }
        }

        // 返回这个文件中的记录
        private Entry getHead() {
            return this.head;
        }

        // 判断这个文件还有没有数据了
        private boolean noMoreHead() throws IOException {
            return (this.head = this.iterator.next(head)) == null;
        }
    }
}
