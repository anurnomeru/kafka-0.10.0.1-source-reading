/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;

/**
 * A pool of ByteBuffers kept under a given memory limit. This class is fairly specific to the needs of the producer. In
 * particular it has the following properties:
 * <ol>
 * <li>There is a special "poolable size" and buffers of this size are kept in a free list and recycled
 * <li>It is fair. That is all memory is given to the longest waiting thread until it has sufficient memory. This
 * prevents starvation or deadlock when a thread asks for a large chunk of memory and needs to block until multiple
 * buffers are deallocated.
 *
 * 维持着【给定内存以下大小】的byteBuffer池。特别是它具有如下属性：
 *
 * - 它有一个特别的“poolable size”并且这个大小的buffer会维持在free list中重复利用
 * - 它是公平的，这是说所有的内存会交给等待最久的线程，直到它拥有足够的内存。当
 * 一个线程请求一块大内存时，它需要阻塞着，直到很多buffer被释放，这种设计防止了饥饿或者死锁。
 *
 * </ol>
 */
public final class BufferPool {

    private final long totalMemory;

    private final int poolableSize;

    private final ReentrantLock lock;

    private final Deque<ByteBuffer> free;

    private final Deque<Condition> waiters;

    private long availableMemory;

    private final Metrics metrics;

    private final Time time;

    private final Sensor waitTime;

    /**
     * Create a new buffer pool
     *
     * @param memory The maximum amount of memory that this buffer pool can allocate
     * @param poolableSize The buffer size to cache in the free list rather than deallocating
     * @param metrics instance of Metrics
     * @param time time instance
     * @param metricGrpName logical group name for metrics
     */
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<ByteBuffer>();
        this.waiters = new ArrayDeque<Condition>();
        this.totalMemory = memory;
        this.availableMemory = memory;
        this.metrics = metrics;
        this.time = time;
        this.waitTime = this.metrics.sensor("bufferpool-wait-time");
        MetricName metricName = metrics.metricName("bufferpool-wait-ratio",
            metricGrpName,
            "The fraction of time an appender waits for space allocation.");
        this.waitTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));
    }

    /**
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the buffer pool
     * is configured with blocking mode.
     *
     * 分配一块指定大小的buffer，当内有足够内存时，这个方法会阻塞，缓冲pool可以配置阻塞模式。
     *
     * @param size The buffer size to allocate in bytes
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be available
     *
     * @return The buffer
     * @throws InterruptedException If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     * forever)
     */
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        if (size > this.totalMemory) {
            throw new IllegalArgumentException("Attempt to allocate " + size
                + " bytes, but there is a hard limit of "
                + this.totalMemory
                + " on memory allocations.");
        }

        this.lock.lock();
        try {
            // check if we have a free buffer of the right size pooled
            // 校验是否有合适的小的空闲的buffer
            if (size == poolableSize && !this.free.isEmpty()) {
                return this.free.pollFirst();
            }

            // now check if the request is immediately satisfiable with the
            // memory on hand or if we need to block
            // 校验现在的内存是否可以立即满足请求，或者是否需要阻塞
            int freeListSize = this.free.size() * this.poolableSize;

            // 如果可用内存+freeList大小大于申请大小
            if (this.availableMemory + freeListSize >= size) {
                // we have enough unallocated or pooled memory to immediately
                // satisfy the request
                // 有足够的被释放或存放在池中的内存来立即满足请求
                freeUp(size);
                this.availableMemory -= size;
                lock.unlock();
                return ByteBuffer.allocate(size);

            } else {// 现在可用的内存大小无法满足
                // we are out of memory and will have to block
                // 需要的内存可能会导致溢出，所以需要阻塞
                int accumulated = 0;
                ByteBuffer buffer = null;

                Condition moreMemory = this.lock.newCondition();
                long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                this.waiters.addLast(moreMemory);

                // loop over and over until we have a buffer or have reserved
                // enough memory to allocate one
                // 循环等待，直到获取到申请到足够的内存
                while (accumulated < size) {
                    long startWaitNs = time.nanoseconds();
                    long timeNs;
                    boolean waitingTimeElapsed;
                    try {
                        // 当前condition进行等待 remainingTimeToBlockNs 毫秒
                        waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        this.waiters.remove(moreMemory);
                        throw e;
                    } finally {
                        long endWaitNs = time.nanoseconds();
                        timeNs = Math.max(0L, endWaitNs - startWaitNs);
                        // 统计阻塞时间
                        this.waitTime.record(timeNs, time.milliseconds());
                    }

                    // 代表超时了
                    if (waitingTimeElapsed) {
                        this.waiters.remove(moreMemory);
                        throw new TimeoutException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
                    }

                    // 没超时，剩余时间等于自己减去耗时
                    remainingTimeToBlockNs -= timeNs;
                    // check if we can satisfy this request from the free list,
                    // otherwise allocate memory
                    // 检查free list是否可以满足请求，不满足则申请内存
                    if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                        // just grab a buffer from the free list
                        // 和前面是一样的，从free list中取一个出来
                        buffer = this.free.pollFirst();
                        accumulated = size;
                    } else {

                        //todo: 走到这里说明申请的大小要大于poolableSize，或者free为空
                        // we'll need to allocate memory, but we may only get
                        // part of what we need on this iteration
                        // 需要申请内存，但是在这个循环可能只能从中获取需要内存的一部分，也就是说太大了，会再获取一次
                        // size：要申请的大小。
                        freeUp(size - accumulated);

                        int got = (int) Math.min(size - accumulated, this.availableMemory);
                        this.availableMemory -= got;
                        accumulated += got;
                    }
                }

                // remove the condition for this thread to let the next thread
                // in line start getting memory
                Condition removed = this.waiters.removeFirst();
                if (removed != moreMemory) {
                    throw new IllegalStateException("Wrong condition: this shouldn't happen.");
                }

                // signal any additional waiters if there is more memory left
                // over for them
                // 通知其他waiters去拿内存
                if (this.availableMemory > 0 || !this.free.isEmpty()) {
                    if (!this.waiters.isEmpty()) {
                        this.waiters.peekFirst()
                                    .signal();
                    }
                }

                // unlock and return the buffer
                lock.unlock();
                if (buffer == null) {// buffer = null 代表内存时直接从free中轮询释放的
                    return ByteBuffer.allocate(size);
                } else {// buffer不为空，是直接复用free中的内存
                    return buffer;
                }
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * Attempt to ensure we have at least the requested number of bytes of memory for allocation by deallocating pooled
     * buffers (if needed)
     *
     * 当free列表不为空，并且可用内存小于申请内存时，来进行free列表的内存释放。
     * ：： 还是从free中取，这个取法，可以取多个
     * 直到取空，或者取出来的空间满足
     */
    private void freeUp(int size) {

        // 当free列表不为空，并且可用内存小于申请内存时
        // ：： 还是从free中取，这个取法，可以取多个
        while (!this.free.isEmpty() && this.availableMemory < size) {
            this.availableMemory += this.free.pollLast()
                                             .capacity();
        }
    }

    /**
     * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the
     * memory as free.
     *
     * 将buffers放回pool，如果他们符合条件就会被扔到free里面，否则只会把它标记为自由内存（等待gc）
     *
     * @param buffer The buffer to return
     * @param size The size of the buffer to mark as deallocated, note that this maybe smaller than buffer.capacity
     * since the buffer may re-allocate itself during in-place compression
     */
    public void deallocate(ByteBuffer buffer, int size) {
        lock.lock();
        try {
            if (size == this.poolableSize && size == buffer.capacity()) {
                buffer.clear();
                this.free.add(buffer);
            } else {
                this.availableMemory += size;
            }
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null) {
                moreMem.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        deallocate(buffer, buffer.capacity());
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    public long availableMemory() {
        lock.lock();
        try {
            return this.availableMemory + this.free.size() * this.poolableSize;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the unallocated memory (not in the free list or in use)
     */
    public long unallocatedMemory() {
        lock.lock();
        try {
            return this.availableMemory;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The number of threads blocked waiting on memory
     */
    public int queued() {
        lock.lock();
        try {
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     */
    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    // package-private method used only for testing
    Deque<Condition> waiters() {
        return this.waiters;
    }
}
