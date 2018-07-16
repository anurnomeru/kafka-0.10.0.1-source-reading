/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A nioSelector interface for doing non-blocking multi-connection network I/O.
 *
 * nioSelector 接口用于进行 非阻塞 多连接 的网络io
 * <p>
 * This class works with {@link NetworkSend} and {@link NetworkReceive} to transmit size-delimited network requests and
 * responses.
 *
 * 这个类通过{@link NetworkSend} 和 {@link NetworkReceive} 来对大小受限的网络请求和网络应答进行传输。
 * <p>
 * A connection can be added to the nioSelector associated with an integer id by doing
 * 连接关联一个nodeId，并添加到 nioSelector，我们可以通过nodeId 来进行nioSelector.connect操作
 * <pre>
 * nioSelector.connect(&quot;42&quot;, new InetSocketAddress(&quot;google.com&quot;, server.port), 64000, 64000);
 * </pre>
 *
 * The connect call does not block on the creation of the TCP connection, so the connect method only begins initiating
 * the connection. The successful invocation of this method does not mean a valid connection has been established.
 *
 * 调用connect方法不会再创建tcp连接时阻塞，所以这个方法只是初始化连接，这个方法成功调用并不意味着连接已经建立
 *
 * Sending requests, receiving responses, processing connection completions, and disconnections on the existing
 * connections are all done using the <code>poll()</code> call.
 *
 * 发送请求，接受应答，成功处理连接和断开连接，都是用poll来完成的
 *
 * <pre>
 * nioSelector.send(new NetworkSend(myDestination, myBytes));
 * nioSelector.send(new NetworkSend(myOtherDestination, myOtherBytes));
 * nioSelector.poll(TIMEOUT_MS);
 * </pre>
 *
 *
 * nioSelector维持着几个会在每次调用poll方法后重置的列表，这几个列表对各种渠道的getter都是可见的
 *
 *
 * The nioSelector maintains several lists that are reset by each call to <code>poll()</code> which are available via
 * various getters. These are reset by each call to <code>poll()</code>.
 *
 * This class is not thread safe!
 */
@NotTheadSafe
@SuppressWarnings("Duplicates")
public class Selector implements Selectable {

    private static final Logger log = LoggerFactory.getLogger(Selector.class);

    /** 监听网络io，java自带的selector */
    private final java.nio.channels.Selector nioSelector;

    /** 可以根据nodeId获取到channel，KafkaChannel是对socketChannel的进一步封装 */
    /**
     * kafkaChannel 里面有节点id、验证器、tpLayer（包含socketChannel和selectionKey）、一个接受buffer、一个发送buffer
     */
    private final Map<String/* nodeId */, KafkaChannel> channels;

    /** 记录已经完全发送出去的请求 */
    private final List<Send> completedSends;

    /** 记录已经完全接收到的请求 */
    private final List<NetworkReceive> completedReceives;

    /** 暂存一次OP_READ事件处理过程中读取到的全部请求，当一次 OP_READ 事件处理完成之后，会将stagedReceives集合中的请求保存到completeReceives */
    private final Map<KafkaChannel, Deque<NetworkReceive>> stagedReceives;

    /**
     * 可以立即连接的selectionKey
     */
    private final Set<SelectionKey> immediatelyConnectedKeys;

    /** 记录一次poll中断开的连接 */
    private final List<String> disconnected;

    /** 记录一次poll中新建立的连接 */
    private final List<String> connected;

    /** 记录一次poll发送失败的node */
    private final List<String> failedSends;

    private final Time time;

    private final SelectorMetrics sensors;

    private final String metricGrpPrefix;

    private final Map<String, String> metricTags;

    /** 用于创建KafkaChannel */
    private final ChannelBuilder channelBuilder;

    /** 记录各个连接的使用情况，据此关闭空闲时间超过 connectionsMaxIdleNanos 的链接 */
    private final Map<String, Long> lruConnections;

    private final long connectionsMaxIdleNanos;

    private final int maxReceiveSize;

    private final boolean metricsPerConnection;

    private long currentTimeNanos;

    private long nextIdleCloseCheckTime;

    /**
     * Create a new nioSelector
     */
    public Selector(int maxReceiveSize, long connectionMaxIdleMs, Metrics metrics, Time time, String metricGrpPrefix, Map<String, String> metricTags, boolean metricsPerConnection,
        ChannelBuilder channelBuilder) {
        try {
            this.nioSelector = java.nio.channels.Selector.open();
        } catch (IOException e) {
            throw new KafkaException(e);
        }
        this.maxReceiveSize = maxReceiveSize;
        this.connectionsMaxIdleNanos = connectionMaxIdleMs * 1000 * 1000;
        this.time = time;
        this.metricGrpPrefix = metricGrpPrefix;
        this.metricTags = metricTags;
        this.channels = new HashMap<>();
        this.completedSends = new ArrayList<>();
        this.completedReceives = new ArrayList<>();
        this.stagedReceives = new HashMap<>();
        this.immediatelyConnectedKeys = new HashSet<>();
        this.connected = new ArrayList<>();
        this.disconnected = new ArrayList<>();
        this.failedSends = new ArrayList<>();
        this.sensors = new SelectorMetrics(metrics);
        this.channelBuilder = channelBuilder;
        // initial capacity and load factor are default, we set them explicitly because we want to set accessOrder = true
        this.lruConnections = new LinkedHashMap<>(16, .75F, true);
        currentTimeNanos = time.nanoseconds();
        nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos;
        this.metricsPerConnection = metricsPerConnection;
    }

    public Selector(long connectionMaxIdleMS, Metrics metrics, Time time, String metricGrpPrefix, ChannelBuilder channelBuilder) {
        this(NetworkReceive.UNLIMITED, connectionMaxIdleMS, metrics, time, metricGrpPrefix, new HashMap<String, String>(), true, channelBuilder);
    }

    /**
     * 核心方法，负责创建KafkaChanel
     *
     * Begin connecting to the given address and add the connection to this nioSelector associated with the given id
     * number.
     * <p>
     * 开始连接到指定的地址并且将连接添加到与给定id相关联的nioSelector
     *
     * <p>
     * Note that this call only initiates the connection, which will be completed on a future {@link #poll(long)}
     * call. Check {@link #connected()} to see which (if any) connections have completed after a given poll call.
     * 注意这个方法只会在调用poll后才启动连接，检查{@link #connected()}来看看哪个连接在调用poll后连接完成。
     *
     * @param id The id for the new connection
     * @param address The address to connect to
     * @param sendBufferSize The send buffer for the new connection
     * @param receiveBufferSize The receive buffer for the new connection
     *
     * @throws IllegalStateException if there is already a connection for that id
     * @throws IOException if DNS resolution fails on the hostname or if the broker is down
     */
    @Override
    public void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        if (this.channels.containsKey(id)) {
            throw new IllegalStateException("There is already a connection for id " + id);
        }

        SocketChannel socketChannel = SocketChannel.open();// 创建一个socketChannel，并且在调用 socketChannel.connect(address)，后打开连接
        socketChannel.configureBlocking(false);// 非阻塞模式
        Socket socket = socketChannel.socket();
        socket.setKeepAlive(true);// 设置为长连接
        if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE) {
            socket.setSendBufferSize(sendBufferSize);// 设置SO_SNDBUF 大小
        }
        if (receiveBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE) {
            socket.setReceiveBufferSize(receiveBufferSize);// 设置 SO_RCVBUF 大小
        }
        socket.setTcpNoDelay(true);
        boolean connected;
        try {
            connected = socketChannel.connect(address);// 因为是非阻塞模式，所以方法可能会在连接正式连接之前返回
        } catch (UnresolvedAddressException e) {
            socketChannel.close();
            throw new IOException("Can't resolve address: " + address, e);
        } catch (IOException e) {
            socketChannel.close();
            throw e;
        }
        SelectionKey key = socketChannel.register(nioSelector, SelectionKey.OP_CONNECT);// 将当前这个socketChannel注册到nioSelector上，并关注OP_CONNECT事件
        KafkaChannel channel = channelBuilder.buildChannel(id, key, maxReceiveSize);// 创建KafkaChannel
        key.attach(channel);// 将channel绑定到key上
        this.channels.put(id, channel);// 将 nodeId 和 Channel绑定

        if (connected) {
            // OP_CONNECT won't trigger for immediately connected channels
            log.debug("Immediately connected to node {}", channel.id());
            immediatelyConnectedKeys.add(key);
            key.interestOps(0);
        }
    }

    /**
     * Register the nioSelector with an existing channel
     * Use this on server-side, when a connection is accepted by a different thread but processed by the Selector
     * Note that we are not checking if the connection id is valid - since the connection already exists
     */
    public void register(String id, SocketChannel socketChannel) throws ClosedChannelException {
        SelectionKey key = socketChannel.register(nioSelector, SelectionKey.OP_READ);
        KafkaChannel channel = channelBuilder.buildChannel(id, key, maxReceiveSize);
        key.attach(channel);
        this.channels.put(id, channel);
    }

    /**
     * Interrupt the nioSelector if it is blocked waiting to do I/O.
     */
    @Override
    public void wakeup() {
        this.nioSelector.wakeup();
    }

    /**
     * Close this selector and all associated connections
     */
    @Override
    public void close() {
        List<String> connections = new ArrayList<>(channels.keySet());
        for (String id : connections)
            close(id);
        try {
            this.nioSelector.close();
        } catch (IOException | SecurityException e) {
            log.error("Exception closing nioSelector:", e);
        }
        sensors.close();
        channelBuilder.close();
    }

    /**
     * Queue the given request for sending in the subsequent {@link #poll(long)} calls
     *
     * @param send The request to send
     */
    public void send(Send send) {
        KafkaChannel channel = channelOrFail(send.destination());
        try {
            channel.setSend(send);
        } catch (CancelledKeyException e) {
            this.failedSends.add(send.destination());
            close(channel);
        }
    }

    /**
     * Do whatever I/O can be done on each connection without blocking. This includes completing connections, completing
     * disconnections, initiating new sends, or making progress on in-progress sends or receives.
     *
     * When this call is completed the user can check for completed sends, receives, connections or disconnects using
     * {@link #completedSends()}, {@link #completedReceives()}, {@link #connected()}, {@link #disconnected()}. These
     * lists will be cleared at the beginning of each `poll` call and repopulated by the call if there is
     * any completed I/O.
     *
     * In the "Plaintext" setting, we are using socketChannel to read & write to the network. But for the "SSL" setting,
     * we encrypt the data before we use socketChannel to write data to the network, and decrypt before we return the responses.
     * This requires additional buffers to be maintained as we are reading from network, since the data on the wire is encrypted
     * we won't be able to read exact no.of bytes as kafka protocol requires. We read as many bytes as we can, up to SSLEngine's
     * application buffer size. This means we might be reading additional bytes than the requested size.
     * If there is no further data to read from socketChannel selector won't invoke that channel and we've have additional bytes
     * in the buffer. To overcome this issue we added "stagedReceives" map which contains per-channel deque. When we are
     * reading a channel we read as many responses as we can and store them into "stagedReceives" and pop one response during
     * the poll to add the completedReceives. If there are any active channels in the "stagedReceives" we set "timeout" to 0
     * and pop response and add to the completedReceives.
     *
     * @param timeout The amount of time to wait, in milliseconds, which must be non-negative
     *
     * @throws IllegalArgumentException If `timeout` is negative
     * @throws IllegalStateException If a send is given for which we have no existing connection or for which there is
     * already an in-progress send
     */
    @Override
    public void poll(long timeout) throws IOException {
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout should be >= 0");
        }

        clear();// 将上一次poll方法的结果清理掉

        if (hasStagedReceives() //TODO: ????????
            || !immediatelyConnectedKeys.isEmpty()) {
            timeout = 0;
        }

        /* check ready keys */
        long startSelect = time.nanoseconds();

        // readyKeys 已经准备好了的 SelectionKey 的数量
        int readyKeys = select(timeout);// 等待I/O事件发生
        long endSelect = time.nanoseconds();
        currentTimeNanos = endSelect;

        this.sensors.selectTime.record(endSelect - startSelect, time.milliseconds());// TODO:???????

        if (readyKeys > 0 || !immediatelyConnectedKeys.isEmpty()) {
            pollSelectionKeys(this.nioSelector.selectedKeys(), false);// 处理I/O的核心方法
            pollSelectionKeys(immediatelyConnectedKeys, true);
        }

        addToCompletedReceives();// 将stagedReceives复制到completedReceives集合中

        long endIo = time.nanoseconds();
        this.sensors.ioTime.record(endIo - endSelect, time.milliseconds());
        maybeCloseOldestConnection();// 关闭长期空闲的连接
    }

    private void pollSelectionKeys(Iterable<SelectionKey> selectionKeys, boolean isImmediatelyConnected) {
        Iterator<SelectionKey> iterator = selectionKeys.iterator();
        while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            iterator.remove();
            // 创建连接时(connect)将kafkaChannel注册到key上，就是为了在这里获取
            KafkaChannel channel = channel(key);

            // register all per-connection metrics at once
            // 注册所有的每一次连接
            sensors.maybeRegisterConnectionMetrics(channel.id());
            lruConnections.put(channel.id(), currentTimeNanos);// 记录一下当前channel最后一次使用的时间

            try {
                /* complete any connections that have finished their handshake (either normally or immediately) */
                // 完成三次握手的任意连接，无论是可以立即使用的channel，或者普通channel
                if (isImmediatelyConnected || key.isConnectable()) {
                    // finishConnect方法会先检测socketChannel是否建立完成，建立后，会取消对OP_CONNECT事件关注，开始关注OP_READ事件
                    if (channel.finishConnect()) {
                        this.connected.add(channel.id());// 将当前channel id 添加到已连接的集合中
                        this.sensors.connectionCreated.record();
                    } else {
                        continue;// 代表连接未完成，则跳过对此Channel的后续处理
                    }
                }

                /* if channel is not ready finish prepare */
                // TODO：第四章：身份验证
                if (channel.isConnected() && !channel.ready()) {
                    channel.prepare();
                }

                // channel是否已经准备好从连接中读取任何可读数据
                /* if channel is ready read from any connections that have readable data */
                if (channel.ready() // 连接的三次握手完成，并且 todo 权限验证通过
                    && key.isReadable() // key已经准备好
                    && !hasStagedReceive(channel)) {                                              // todo：他的意思可能是正在这个通道正在读数据
                    NetworkReceive networkReceive;

                    while ((networkReceive = channel.read()) != null) {
                        addToStagedReceives(channel, networkReceive);
                    }
                }

                /* if channel is ready write to any sockets that have space in their buffer and for which we have data */
                // 如果channel的buffer已经有足够的空间 并且 我们有数据来准备好写sockets
                if (channel.ready() && key.isWritable()) {
                    Send send = channel.write();
                    // 这里会将KafkaChannel的send字段发送出去，如果未完成发送，则返回null
                    // 否则将返回send
                    if (send != null) {
                        this.completedSends.add(send);// 添加到completedSends集合
                        this.sensors.recordBytesSent(channel.id(), send.size());
                    }
                }

                /* cancel any defunct sockets */
                if (!key.isValid()) {
                    close(channel);
                    this.disconnected.add(channel.id());
                }
            } catch (Exception e) {
                String desc = channel.socketDescription();
                if (e instanceof IOException) {
                    log.debug("Connection with {} disconnected", desc, e);
                } else {
                    log.warn("Unexpected error from {}; closing connection", desc, e);
                }
                close(channel);
                this.disconnected.add(channel.id());
            }
        }
    }

    @Override
    public List<Send> completedSends() {
        return this.completedSends;
    }

    @Override
    public List<NetworkReceive> completedReceives() {
        return this.completedReceives;
    }

    @Override
    public List<String> disconnected() {
        return this.disconnected;
    }

    @Override
    public List<String> connected() {
        return this.connected;
    }

    @Override
    public void mute(String id) {
        KafkaChannel channel = channelOrFail(id);
        mute(channel);
    }

    private void mute(KafkaChannel channel) {
        channel.mute();
    }

    @Override
    public void unmute(String id) {
        KafkaChannel channel = channelOrFail(id);
        unmute(channel);
    }

    private void unmute(KafkaChannel channel) {
        channel.unmute();
    }

    @Override
    public void muteAll() {
        for (KafkaChannel channel : this.channels.values())
            mute(channel);
    }

    @Override
    public void unmuteAll() {
        for (KafkaChannel channel : this.channels.values())
            unmute(channel);
    }

    private void maybeCloseOldestConnection() {
        if (currentTimeNanos > nextIdleCloseCheckTime) {
            if (lruConnections.isEmpty()) {
                nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos;
            } else {
                Map.Entry<String, Long> oldestConnectionEntry = lruConnections.entrySet()
                                                                              .iterator()
                                                                              .next();
                Long connectionLastActiveTime = oldestConnectionEntry.getValue();
                nextIdleCloseCheckTime = connectionLastActiveTime + connectionsMaxIdleNanos;
                if (currentTimeNanos > nextIdleCloseCheckTime) {
                    String connectionId = oldestConnectionEntry.getKey();
                    if (log.isTraceEnabled()) {
                        log.trace("About to close the idle connection from " + connectionId
                            + " due to being idle for " + (currentTimeNanos - connectionLastActiveTime) / 1000 / 1000 + " millis");
                    }

                    disconnected.add(connectionId);
                    close(connectionId);
                }
            }
        }
    }

    /**
     * Clear the results from the prior poll
     */
    private void clear() {
        this.completedSends.clear();
        this.completedReceives.clear();
        this.connected.clear();
        this.disconnected.clear();
        this.disconnected.addAll(this.failedSends);
        this.failedSends.clear();
    }

    /**
     * Check for data, waiting up to the given timeout.
     *
     * @param ms Length of time to wait, in milliseconds, which must be non-negative
     *
     * @return The number of keys ready
     */
    private int select(long ms) throws IOException {
        if (ms < 0L) {
            throw new IllegalArgumentException("timeout should be >= 0");
        }

        if (ms == 0L) {
            return this.nioSelector.selectNow();
        } else {
            return this.nioSelector.select(ms);
        }
    }

    /**
     * Close the connection identified by the given id
     */
    public void close(String id) {
        KafkaChannel channel = this.channels.get(id);
        if (channel != null) {
            close(channel);
        }
    }

    /**
     * Begin closing this connection
     */
    private void close(KafkaChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            log.error("Exception closing connection to node {}:", channel.id(), e);
        }
        this.stagedReceives.remove(channel);
        this.channels.remove(channel.id());
        this.lruConnections.remove(channel.id());
        this.sensors.connectionClosed.record();
    }

    /**
     * check if channel is ready
     */
    @Override
    public boolean isChannelReady(String id) {
        KafkaChannel channel = this.channels.get(id);
        return channel != null && channel.ready();
    }

    private KafkaChannel channelOrFail(String id) {
        KafkaChannel channel = this.channels.get(id);
        if (channel == null) {
            throw new IllegalStateException("Attempt to retrieve channel for which there is no open connection. Connection id " + id + " existing connections " + channels.keySet());
        }
        return channel;
    }

    /**
     * Return the selector channels.
     */
    public List<KafkaChannel> channels() {
        return new ArrayList<>(channels.values());
    }

    /**
     * Return the channel associated with this connection or `null` if there is no channel associated with the
     * connection.
     */
    public KafkaChannel channel(String id) {
        return this.channels.get(id);
    }

    /**
     * Get the channel associated with selectionKey
     */
    private KafkaChannel channel(SelectionKey key) {
        return (KafkaChannel) key.attachment();
    }

    /**
     * Check if given channel has a staged receive
     * 检查这个channel是否是多次接受
     */
    private boolean hasStagedReceive(KafkaChannel channel) {
        return stagedReceives.containsKey(channel);
    }

    /**
     * check if stagedReceives have unmuted channel
     */
    private boolean hasStagedReceives() {
        for (KafkaChannel channel : this.stagedReceives.keySet()) {
            if (!channel.isMute()) {
                return true;
            }
        }
        return false;
    }

    /**
     * adds a receive to staged receives
     */
    private void addToStagedReceives(KafkaChannel channel, NetworkReceive receive) {
        if (!stagedReceives.containsKey(channel)) {
            stagedReceives.put(channel, new ArrayDeque<NetworkReceive>());
        }

        Deque<NetworkReceive> deque = stagedReceives.get(channel);
        deque.add(receive);
    }

    /**
     * checks if there are any staged receives and adds to completedReceives
     */
    private void addToCompletedReceives() {
        if (!this.stagedReceives.isEmpty()) {
            Iterator<Map.Entry<KafkaChannel, Deque<NetworkReceive>>> iter = this.stagedReceives.entrySet()
                                                                                               .iterator();
            while (iter.hasNext()) {
                Map.Entry<KafkaChannel, Deque<NetworkReceive>> entry = iter.next();
                KafkaChannel channel = entry.getKey();
                if (!channel.isMute()) {
                    Deque<NetworkReceive> deque = entry.getValue();
                    NetworkReceive networkReceive = deque.poll();
                    this.completedReceives.add(networkReceive);
                    this.sensors.recordBytesReceived(channel.id(), networkReceive.payload()
                                                                                 .limit());
                    if (deque.isEmpty()) {
                        iter.remove();
                    }
                }
            }
        }
    }

    private class SelectorMetrics {

        private final Metrics metrics;

        public final Sensor connectionClosed;

        public final Sensor connectionCreated;

        public final Sensor bytesTransferred;

        public final Sensor bytesSent;

        public final Sensor bytesReceived;

        public final Sensor selectTime;

        public final Sensor ioTime;

        /* Names of metrics that are not registered through sensors */
        private final List<MetricName> topLevelMetricNames = new ArrayList<>();

        private final List<Sensor> sensors = new ArrayList<>();

        public SelectorMetrics(Metrics metrics) {
            this.metrics = metrics;
            String metricGrpName = metricGrpPrefix + "-metrics";
            StringBuilder tagsSuffix = new StringBuilder();

            for (Map.Entry<String, String> tag : metricTags.entrySet()) {
                tagsSuffix.append(tag.getKey());
                tagsSuffix.append("-");
                tagsSuffix.append(tag.getValue());
            }

            this.connectionClosed = sensor("connections-closed:" + tagsSuffix.toString());
            MetricName metricName = metrics.metricName("connection-close-rate", metricGrpName, "Connections closed per second in the window.", metricTags);
            this.connectionClosed.add(metricName, new Rate());

            this.connectionCreated = sensor("connections-created:" + tagsSuffix.toString());
            metricName = metrics.metricName("connection-creation-rate", metricGrpName, "New connections established per second in the window.", metricTags);
            this.connectionCreated.add(metricName, new Rate());

            this.bytesTransferred = sensor("bytes-sent-received:" + tagsSuffix.toString());
            metricName = metrics.metricName("network-io-rate", metricGrpName, "The average number of network operations (reads or writes) on all connections per second.", metricTags);
            bytesTransferred.add(metricName, new Rate(new Count()));

            this.bytesSent = sensor("bytes-sent:" + tagsSuffix.toString(), bytesTransferred);
            metricName = metrics.metricName("outgoing-byte-rate", metricGrpName, "The average number of outgoing bytes sent per second to all servers.", metricTags);
            this.bytesSent.add(metricName, new Rate());
            metricName = metrics.metricName("request-rate", metricGrpName, "The average number of requests sent per second.", metricTags);
            this.bytesSent.add(metricName, new Rate(new Count()));
            metricName = metrics.metricName("request-size-avg", metricGrpName, "The average size of all requests in the window..", metricTags);
            this.bytesSent.add(metricName, new Avg());
            metricName = metrics.metricName("request-size-max", metricGrpName, "The maximum size of any request sent in the window.", metricTags);
            this.bytesSent.add(metricName, new Max());

            this.bytesReceived = sensor("bytes-received:" + tagsSuffix.toString(), bytesTransferred);
            metricName = metrics.metricName("incoming-byte-rate", metricGrpName, "Bytes/second read off all sockets", metricTags);
            this.bytesReceived.add(metricName, new Rate());
            metricName = metrics.metricName("response-rate", metricGrpName, "Responses received sent per second.", metricTags);
            this.bytesReceived.add(metricName, new Rate(new Count()));

            this.selectTime = sensor("select-time:" + tagsSuffix.toString());
            metricName = metrics.metricName("select-rate", metricGrpName, "Number of times the I/O layer checked for new I/O to perform per second", metricTags);
            this.selectTime.add(metricName, new Rate(new Count()));
            metricName = metrics.metricName("io-wait-time-ns-avg", metricGrpName, "The average length of time the I/O thread spent waiting for a socket ready for reads or writes in nanoseconds.",
                metricTags);
            this.selectTime.add(metricName, new Avg());
            metricName = metrics.metricName("io-wait-ratio", metricGrpName, "The fraction of time the I/O thread spent waiting.", metricTags);
            this.selectTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));

            this.ioTime = sensor("io-time:" + tagsSuffix.toString());
            metricName = metrics.metricName("io-time-ns-avg", metricGrpName, "The average length of time for I/O per select call in nanoseconds.", metricTags);
            this.ioTime.add(metricName, new Avg());
            metricName = metrics.metricName("io-ratio", metricGrpName, "The fraction of time the I/O thread spent doing I/O", metricTags);
            this.ioTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));

            metricName = metrics.metricName("connection-count", metricGrpName, "The current number of active connections.", metricTags);
            topLevelMetricNames.add(metricName);
            this.metrics.addMetric(metricName, new Measurable() {

                public double measure(MetricConfig config, long now) {
                    return channels.size();
                }
            });
        }

        private Sensor sensor(String name, Sensor... parents) {
            Sensor sensor = metrics.sensor(name, parents);
            sensors.add(sensor);
            return sensor;
        }

        public void maybeRegisterConnectionMetrics(String connectionId) {
            if (!connectionId.isEmpty() && metricsPerConnection) {
                // if one sensor of the metrics has been registered for the connection,
                // then all other sensors should have been registered; and vice versa
                String nodeRequestName = "node-" + connectionId + ".bytes-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest == null) {
                    String metricGrpName = metricGrpPrefix + "-node-metrics";

                    Map<String, String> tags = new LinkedHashMap<>(metricTags);
                    tags.put("node-id", "node-" + connectionId);

                    nodeRequest = sensor(nodeRequestName);
                    MetricName metricName = metrics.metricName("outgoing-byte-rate", metricGrpName, tags);
                    nodeRequest.add(metricName, new Rate());
                    metricName = metrics.metricName("request-rate", metricGrpName, "The average number of requests sent per second.", tags);
                    nodeRequest.add(metricName, new Rate(new Count()));
                    metricName = metrics.metricName("request-size-avg", metricGrpName, "The average size of all requests in the window..", tags);
                    nodeRequest.add(metricName, new Avg());
                    metricName = metrics.metricName("request-size-max", metricGrpName, "The maximum size of any request sent in the window.", tags);
                    nodeRequest.add(metricName, new Max());

                    String nodeResponseName = "node-" + connectionId + ".bytes-received";
                    Sensor nodeResponse = sensor(nodeResponseName);
                    metricName = metrics.metricName("incoming-byte-rate", metricGrpName, tags);
                    nodeResponse.add(metricName, new Rate());
                    metricName = metrics.metricName("response-rate", metricGrpName, "The average number of responses received per second.", tags);
                    nodeResponse.add(metricName, new Rate(new Count()));

                    String nodeTimeName = "node-" + connectionId + ".latency";
                    Sensor nodeRequestTime = sensor(nodeTimeName);
                    metricName = metrics.metricName("request-latency-avg", metricGrpName, tags);
                    nodeRequestTime.add(metricName, new Avg());
                    metricName = metrics.metricName("request-latency-max", metricGrpName, tags);
                    nodeRequestTime.add(metricName, new Max());
                }
            }
        }

        public void recordBytesSent(String connectionId, long bytes) {
            long now = time.milliseconds();
            this.bytesSent.record(bytes, now);
            if (!connectionId.isEmpty()) {
                String nodeRequestName = "node-" + connectionId + ".bytes-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null) {
                    nodeRequest.record(bytes, now);
                }
            }
        }

        public void recordBytesReceived(String connection, int bytes) {
            long now = time.milliseconds();
            this.bytesReceived.record(bytes, now);
            if (!connection.isEmpty()) {
                String nodeRequestName = "node-" + connection + ".bytes-received";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null) {
                    nodeRequest.record(bytes, now);
                }
            }
        }

        public void close() {
            for (MetricName metricName : topLevelMetricNames)
                metrics.removeMetric(metricName);
            for (Sensor sensor : sensors)
                metrics.removeSensor(sensor.name());
        }
    }
}
