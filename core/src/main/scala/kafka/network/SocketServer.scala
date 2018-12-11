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

package kafka.network

import java.io.IOException
import java.net._
import java.nio.channels.{Selector => NSelector, _}
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic._

import com.yammer.metrics.core.Gauge
import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.network.{ChannelBuilders, KafkaChannel, LoginType, Mode, Selector => KSelector}
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection.JavaConverters._
import scala.collection._
import scala.util.control.{ControlThrowable, NonFatal}

/**
  * An NIO socket server. The threading model is
  * 1 Acceptor thread that handles new connections
  * Acceptor has N Processor threads that each have their own selector and read requests from sockets
  * M Handler threads that handle requests and produce responses back to the processor threads for writing.
  *
  * 一个nioSocket 服务器，线程模型如下：
  * 一个 Acceptor负责处理新的连接。
  * 这个Acceptor有N个Processor线程，每一个都有他们自己的selector并读取请求
  * M个Handle线程处理请求，并生成response返回到processor线程来进行写操作
  */
class SocketServer(val config: KafkaConfig, val metrics: Metrics, val time: Time) extends Logging with KafkaMetricsGroup {

    // endpoint集合，一般的服务器都有多块网卡，可以配置多个ip，Kafka可以同时监听多个端口，
    // 这里面封装了需要监听的host、port以及网络协议，每一个Endpoint都会创建一个对应的Acceptor对象
    private val endpoints = config.listeners

    private val numProcessorThreads = config.numNetworkThreads // 线程个数
    private val maxQueuedRequests = config.queuedMaxRequests // 线程最大个数
    private val totalProcessorThreads = numProcessorThreads * endpoints.size // 线程总个数

    private val maxConnectionsPerIp = config.maxConnectionsPerIp
    private val maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides

    this.logIdent = "[Socket Server on Broker " + config.brokerId + "], "

    val requestChannel = new RequestChannel(totalProcessorThreads, maxQueuedRequests) // Processor线程与Handler线程之间交换数据的队列
    private val processors = new Array[Processor](totalProcessorThreads) // Processor线程集合，包含所有endPoint对应的Processor

    private[network] val acceptors = mutable.Map[EndPoint, Acceptor]()
    // 每个endPoint对应一个acceptor
    private var connectionQuotas: ConnectionQuotas = _ // 提供了控制每个ip上最大连接数的功能，底层通过一个map对象，记录每个ip地址上建立的连接数
    //创建新的连接时，和maxConnectionsPerIpOverrides指定的最大值进行比较，如果超出限制，则报错，因为有多个Acceptor线程并发访问底层的Map对象，则需要锁进行同步。

    private val allMetricNames = (0 until totalProcessorThreads).map { i =>
        val tags = new util.HashMap[String, String]()
        tags.put("networkProcessor", i.toString)
        metrics.metricName("io-wait-ratio", "socket-server-metrics", tags)
    }

    // register the processor threads for notification of responses
    // 向RequestChannel中添加一个监听器，此监听器实现的功能是：当Handle线程向某个
    // responseQueue中写入数据时，会唤醒对应的Processor线程进行处理

    // 这个response监听器的作用就是去唤醒一下传入id对应的processor里的Selector
    requestChannel.addResponseListener(id => processors(id).wakeup())

    /**
      * Start the socket server
      * 初始化socketServer
      */
    def startup() {
        this.synchronized {

            connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides) // 新建一个连接配额器

            val sendBufferSize = config.socketSendBufferBytes
            val recvBufferSize = config.socketReceiveBufferBytes
            val brokerId = config.brokerId // 自己的id

            var processorBeginIndex = 0

            // 这里是这样的 如果是4核
            // 第一个 endpoint = 第一个Acceptor = (0 - 3)Processor
            // 第二个 endpoint = 第二个Acceptor = (4 - 7)Processor
            endpoints.values.foreach { endpoint =>
                val protocol = endpoint.protocolType
                // 网络协议
                val processorEndIndex = processorBeginIndex + numProcessorThreads

                for (i <- processorBeginIndex until processorEndIndex)
                    processors(i) = newProcessor(i, connectionQuotas, protocol) // 创建Processor

                // 在这里面会  // 循环启动processor线程
                val acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, brokerId,
                    processors.slice(processorBeginIndex, processorEndIndex), connectionQuotas) // 创建Acceptor
                acceptors.put(endpoint, acceptor) // 将当前的Acceptor和endpoint进行一一绑定
                Utils.newThread("kafka-socket-acceptor-%s-%d".format(protocol.toString, endpoint.port), acceptor, false).start() // 启动Acceptor线程
                acceptor.awaitStartup() // 让Acceptor await，阻塞，直到它初始化完毕

                processorBeginIndex = processorEndIndex
            }
        }

        newGauge("NetworkProcessorAvgIdlePercent",
            new Gauge[Double] {
                def value = allMetricNames.map(metricName =>
                    metrics.metrics().get(metricName).value()).sum / totalProcessorThreads
            }
        )

        info("Started " + acceptors.size + " acceptor threads")
    }


    /**
      * Shutdown the socket server
      */
    def shutdown() = {
        info("Shutting down")
        this.synchronized {
            // 调用acceptors的shutdown，就是调用它的父类
            //  def shutdown(): Unit = {
            //    alive.set(false)
            //    wakeup()
            //    shutdownLatch.await()
            //  }
            acceptors.values.foreach(_.shutdown())
            processors.foreach(_.shutdown())
        }
        info("Shutdown completed")
    }

    def boundPort(protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Int = {
        try {
            acceptors(endpoints(protocol)).serverChannel.socket().getLocalPort
        } catch {
            case e: Exception => throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
        }
    }

    /* `protected` for test usage */
    protected[network] def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, protocol: SecurityProtocol): Processor = {
        new Processor(id,
            time,
            config.socketRequestMaxBytes,
            requestChannel,
            connectionQuotas,
            config.connectionsMaxIdleMs,
            protocol,
            config.values,
            metrics
        )
    }

    /* For test usage */
    private[network] def connectionCount(address: InetAddress): Int =
        Option(connectionQuotas).fold(0)(_.get(address))

    /* For test usage */
    private[network] def processor(index: Int): Processor = processors(index)

}

/**
  * A base class with some helper variables and methods
  * 这个基类主要是提供一些启动关闭相应控制的方法
  */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

    private val startupLatch = new CountDownLatch(1)
    private val shutdownLatch = new CountDownLatch(1)
    private val alive = new AtomicBoolean(true)

    def wakeup()

    /**
      * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
      */
    def shutdown(): Unit = {
        alive.set(false)
        wakeup()
        shutdownLatch.await()
    }

    /**
      * Wait for the thread to completely start up
      * 阻塞等待直到startupComplete()被调用
      */
    def awaitStartup(): Unit = startupLatch.await()

    /**
      * Record that the thread startup is complete
      */
    protected def startupComplete() = {
        startupLatch.countDown()
    }

    /**
      * Record that the thread shutdown is complete
      */
    protected def shutdownComplete() = shutdownLatch.countDown()

    /**
      * Is the server still running?
      * alive状态管理是否running
      */
    protected def isRunning = alive.get

    /**
      * Close the connection identified by `connectionId` and decrement the connection count.
      * 循环关闭KSelector中打开的通道
      */
    def close(selector: KSelector, connectionId: String) {
        val channel = selector.channel(connectionId)
        if (channel != null) {
            debug(s"Closing selector connection $connectionId")
            val address = channel.socketAddress
            if (address != null)
                connectionQuotas.dec(address)
            selector.close(connectionId)
        }
    }

    /**
      * Close `channel` and decrement the connection count.
      * 关闭channel并且连接数减减
      */
    def close(channel: SocketChannel) {
        if (channel != null) {
            debug("Closing connection from " + channel.socket.getRemoteSocketAddress)
            connectionQuotas.dec(channel.socket.getInetAddress)
            swallowError(channel.socket().close())
            swallowError(channel.close())
        }
    }
}

/**
  * Thread that accepts and configures new connections. There is one of these per endpoint.
  * 这个线程用于建立并配置新的连接，一个endpoint就有一个Acceptor
  */
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              brokerId: Int,
                              processors: Array[Processor],
                              connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

    // 打开一个nioSelector
    private val nioSelector = NSelector.open()

    val serverChannel: ServerSocketChannel = openServerSocket(endPoint.host, endPoint.port)

    // 循环启动processor线程
    this.synchronized {
        processors.foreach { processor =>
            Utils.newThread("kafka-network-thread-%d-%s-%d".format(brokerId, endPoint.protocolType.toString, processor.id), processor, false).start()
        }
    }

    /**
      * Accept loop that checks for new connection attempts
      * 循环接受尝试连接的请求
      */
    def run() {
        serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT) // 关注 OP_ACCEPT
        startupComplete() // 通知不用阻塞啦
        try {
            var currentProcessor = 0
            while (isRunning) {
                try {
                    val ready: Int = nioSelector.select(500) // 半秒轮询一次
                    if (ready > 0) {
                        val keys: util.Set[SelectionKey] = nioSelector.selectedKeys()
                        val iter: util.Iterator[SelectionKey] = keys.iterator()
                        while (iter.hasNext && isRunning) {
                            try {
                                val key = iter.next
                                iter.remove()
                                if (key.isAcceptable) // 确实关注了 OP_ACCEPT
                                    accept(key, processors(currentProcessor)) // 让某个Processor接客，将创建的连接交给Processor
                                else
                                    throw new IllegalStateException("Unrecognized key state for acceptor thread.")

                                // round robin to the next processor thread
                                currentProcessor = (currentProcessor + 1) % processors.length // 负载均衡算法round robin
                            } catch {
                                case e: Throwable => error("Error while accepting connection", e)
                            }
                        }
                    }
                }
                catch {
                    // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
                    // to a select operation on a specific channel or a bad request. We don't want the
                    // the broker to stop responding to requests from other clients in these scenarios.
                    case e: ControlThrowable => throw e
                    case e: Throwable => error("Error occurred", e)
                }
            }
        } finally {
            debug("Closing server socket and selector.")
            swallowError(serverChannel.close())
            swallowError(nioSelector.close())
            shutdownComplete()
        }
    }

    /*
     * Create a server socket to listen for connections on.
     * 创建一个server socket 来监听连接
     */
    private def openServerSocket(host: String, port: Int): ServerSocketChannel = {
        val socketAddress =
            if (host == null || host.trim.isEmpty)
                new InetSocketAddress(port) // 如果没有host，建立一个localhost
            else
                new InetSocketAddress(host, port)
        val serverChannel: ServerSocketChannel = ServerSocketChannel.open()
        serverChannel.configureBlocking(false)
        serverChannel.socket().setReceiveBufferSize(recvBufferSize)
        try {
            serverChannel.socket.bind(socketAddress)
            info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostString, serverChannel.socket.getLocalPort))
        } catch {
            case e: SocketException =>
                throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostString, port, e.getMessage), e)
        }
        serverChannel
    }

    /*
     * Accept a new connection
     */
    def accept(key: SelectionKey, processor: Processor) {
        // 首先拿到ServerSocketChannel，也就是当时注册了这个Key的那个
        val serverSocketChannel: ServerSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]

        // 打开channel
        val socketChannel = serverSocketChannel.accept()
        try {
            connectionQuotas.inc(socketChannel.socket().getInetAddress) // 限制一下连接
            socketChannel.configureBlocking(false)
            socketChannel.socket().setTcpNoDelay(true)
            socketChannel.socket().setSendBufferSize(sendBufferSize)
            socketChannel.socket().setKeepAlive(true)

            debug("Accepted connection from %s on %s and assigned it to processor %d, sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
              .format(socketChannel.socket.getRemoteSocketAddress, socketChannel.socket.getLocalSocketAddress, processor.id,
                  socketChannel.socket.getSendBufferSize, sendBufferSize,
                  socketChannel.socket.getReceiveBufferSize, recvBufferSize))

            processor.accept(socketChannel) // 将建立的连接保存在Processor里面
        } catch {
            case e: TooManyConnectionsException =>
                info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
                close(socketChannel)
        }
    }

    /**
      * Wakeup the thread for selection.
      */
    @Override
    def wakeup = nioSelector.wakeup()

}

/**
  * Thread that processes all requests from a single connection. There are N of these running in parallel
  * each of which has its own selector
  */
private[kafka] class Processor(val id: Int, // 定置化，id一共有 endPoint * coreNum个
                               time: Time,
                               maxRequestSize: Int,
                               requestChannel: RequestChannel, // Processor 和 Handler线程之间传递数据的队列
                               connectionQuotas: ConnectionQuotas,
                               connectionsMaxIdleMs: Long,
                               protocol: SecurityProtocol, // 定置化，加密协议
                               channelConfigs: java.util.Map[String, _],
                               metrics: Metrics) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

    private object ConnectionId {
        def fromString(s: String): Option[ConnectionId] = s.split("-") match {
            case Array(local, remote) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
                BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
                    ConnectionId(localHost, localPort, remoteHost, remotePort)
                }
            }
            case _ => None
        }
    }

    private case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int) {
        override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort"
    }

    private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
    // 保存了这个Processor管理的一系列SocketChannel
    private val inflightResponses = mutable.Map[String, RequestChannel.Response]()
    // 和inFlightRequest有区别，它不用ack
    private val metricTags = Map("networkProcessor" -> id.toString).asJava

    newGauge("IdlePercent",
        new Gauge[Double] {
            def value = {
                metrics.metrics().get(metrics.metricName("io-wait-ratio", "socket-server-metrics", metricTags)).value()
            }
        },
        metricTags.asScala
    )

    private val selector = new KSelector(// kafkaSelector
        maxRequestSize,
        connectionsMaxIdleMs,
        metrics,
        time,
        "socket-server",
        metricTags,
        false,
        ChannelBuilders.create(protocol, Mode.SERVER, LoginType.SERVER, channelConfigs, null, true))


    /**
      * processor的核心方法，有多次注册/取消OP_READ事件以及注册/取消OP_WRITE事件的操作。
      * 以此保证每个连接上只有一个请求和一个响应，来实现请求的顺序性
      */
    override def run() {
        startupComplete() // 通知一下主线程可以继续了
        while (isRunning) {
            /** 调用shutdown就不再while了 */
            try {
                // setup any new connections that have been queued up
                configureNewConnections() // 处理新连接 CAUTION 会让新连接关注 READ

                // register any new responses for writing
                /**
                  * // 关注 OP_WRITE 事件
                  * // key.interestOps(key.interestOps() | ops);
                  * this.transportLayer.addInterestOps(SelectionKey.OP_WRITE)
                  *
                  * Comment on 2018/9/26 by Anur
                  */
                processNewResponses() // CAUTION 需要response的调用kafkaChannel的send，并且关注READ，也关注WRITE

                poll() // poll一波，poll完就取关了WRITE，一次poll只会从
                // 类似于networkClient里面那个，它其实就是将收到的东西扔进 RequestChannel 的 queue 里
                // RequestChannel 是 Processor 和 Handler线程之间传递数据的队列
                // 然后取消关注READ
                // CAUTION 核心就是 对 selector.completedReceives 进行操作
                processCompletedReceives() // 收到一波后，取消关注READ CAUTION 完全收到以后 // 然后将它们扔进requestQueue  requestChannel.sendRequest(req)

                // 移除掉inflight，然后关注READ
                processCompletedSends()

                processDisconnected()
            } catch {
                // We catch all the throwables here to prevent the processor thread from exiting. We do this because
                // letting a processor exit might cause a bigger impact on the broker. Usually the exceptions thrown would
                // be either associated with a specific socket channel or a bad request. We just ignore the bad socket channel
                // or request. This behavior might need to be reviewed if we see an exception that need the entire broker to stop.
                case e: ControlThrowable => throw e
                case e: Throwable =>
                    error("Processor got uncaught exception.", e)
            }
        }

        debug("Closing selector - processor " + id)
        swallowError(closeAll())
        shutdownComplete()
    }

    private def processNewResponses() {
        var curr: RequestChannel.Response = requestChannel.receiveResponse(id) // 从requestChannel中取出 response
        while (curr != null) {
            try {
                curr.responseAction match {
                    /** 表示此连接不需要相应，所以只需要关注一下OP_READ */
                    case RequestChannel.NoOpAction =>
                        // There is no response to send to the client, we need to read more pipelined requests
                        // that are sitting in the server's socket buffer
                        // todo：没有需要发送给client的response，我们需要读取更多位于服务器socket buffer的流水线请求
                        curr.request.updateRequestMetrics()
                        trace("Socket server received empty response to send, registering for read: " + curr)
                        selector.unmute(curr.request.connectionId) // 继续关注一下READ

                    /** 表示这个Response需要发送给客户端，首先查找对应的KafkaChannel，为其注册OP_WRITE事件，并将KafkaChannel的Send字段指向待发送Response对象
                      * 同时会将Response从responseQueue移除，放入inflightResponses中（在发完就会取消关注OP_WRITE） */
                    case RequestChannel.SendAction =>
                        sendResponse(curr) // 把response返回去，selector.send(response.responseSend)

                    /** 表示断开了连接 */
                    case RequestChannel.CloseConnectionAction =>
                        curr.request.updateRequestMetrics()
                        trace("Closing socket connection actively according to the response code.")
                        close(selector, curr.request.connectionId)
                }
            } finally {
                curr = requestChannel.receiveResponse(id) // 继续while循环
            }
        }
    }

    /* `protected` for test usage */
    protected[network] def sendResponse(response: RequestChannel.Response) {
        trace(s"Socket server received response to send, registering for write and sending data: $response")
        val channel: KafkaChannel = selector.channel(response.responseSend.destination) // 根据node_id获取到对应的channel
        // `channel` can be null if the selector closed the connection because it was idle for too long // 闲置太长时间的连接可能会被关掉
        if (channel == null) {
            warn(s"Attempting to send response via channel for which there is no open connection, connection id $id")
            response.request.updateRequestMetrics()
        }
        else {
            selector.send(response.responseSend)
            inflightResponses += (response.request.connectionId -> response)
        }
    }

    private def poll() {
        try selector.poll(300)
        catch {
            case e@(_: IllegalStateException | _: IOException) =>
                error(s"Closing processor $id due to illegal state or IO exception")
                swallow(closeAll())
                shutdownComplete()
                throw e
        }
    }

    /**
      * 处理刚接收到的请求
      */
    private def processCompletedReceives() {
        selector.completedReceives // List[NetworkReceive]
          .asScala.foreach { networkReceive =>
            try {
                // 这个 source 实际上就是 node.idString，根据id获取到相应的kafkaChannel
                val channel: KafkaChannel = selector.channel(networkReceive.source)

                // 创建 KafkaChannel对应的Session对象，书上说和权限控制有关，后面会讲
                val session: RequestChannel.Session = RequestChannel.Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, channel.principal.getName),
                    channel.socketAddress)

                // Processor id， node id， session， 返回的buffer， 协议等等信息存一下
                val req: RequestChannel.Request = RequestChannel.Request(processor = id, connectionId = networkReceive.source, session = session, buffer = networkReceive.payload, startTimeMs = time.milliseconds, securityProtocol = protocol)

                // 然后将它们扔进requestQueue
                requestChannel.sendRequest(req)

                // 这个刚收到消息的节点，不再关注READ请求
                selector.mute(networkReceive.source)
            } catch {
                case e@(_: InvalidRequestException | _: SchemaException) =>
                    // note that even though we got an exception, we can assume that receive.source is valid. Issues with constructing a valid receive object were handled earlier
                    error(s"Closing socket for ${networkReceive.source} because of error", e)
                    close(selector, networkReceive.source) // 如果异常，就关闭掉channel
            }
        }
    }

    private def processCompletedSends() {
        selector.completedSends.asScala.foreach { send =>
            val resp = inflightResponses.remove(send.destination).getOrElse {
                throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
            }
            resp.request.updateRequestMetrics()
            selector.unmute(send.destination)
        }
    }

    private def processDisconnected() {
        selector.disconnected.asScala.foreach { connectionId =>
            val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
                throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
            }.remoteHost
            inflightResponses.remove(connectionId).foreach(_.request.updateRequestMetrics())
            // the channel has been closed by the selector but the quotas still need to be updated
            connectionQuotas.dec(InetAddress.getByName(remoteHost))
        }
    }

    /**
      * Queue up a new connection for reading
      */
    def accept(socketChannel: SocketChannel) {
        newConnections.add(socketChannel)
        wakeup() // 通知当前这个Processor 的 selector不要阻塞了，Acceptor虽然也用了nio，但是没有用KSelector去做包装
    }

    /**
      * Register any new connections that have been queued up
      * 创建当前channel有关的配置，比如它的KSelector，生成其connectionId
      */
    private def configureNewConnections() {
        while (!newConnections.isEmpty) {
            val channel = newConnections.poll() // 配置一下newConnections
            try {

                // 还能这么用？？？？
                debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
                val localHost = channel.socket().getLocalAddress.getHostAddress
                val localPort = channel.socket().getLocalPort
                val remoteHost = channel.socket().getInetAddress.getHostAddress
                val remotePort = channel.socket().getPort
                val connectionId: String = ConnectionId(localHost, localPort, remoteHost, remotePort).toString
                selector.register(connectionId, channel) // 注册一下Read事件，并且创建KafkaSelector，同时将connectionId/ kafkaChannel/ SelectionKey 互相绑定
            } catch {
                // We explicitly catch all non fatal exceptions and close the socket to avoid a socket leak. The other
                // throwables will be caught in processor and logged as uncaught exceptions.
                case NonFatal(e) =>
                    // need to close the channel here to avoid a socket leak.
                    close(channel)
                    error(s"Processor $id closed connection from ${channel.getRemoteAddress}", e)
            }
        }
    }

    /**
      * Close the selector and all open connections
      */
    private def closeAll() {
        selector.channels.asScala.foreach { channel =>
            close(selector, channel.id)
        }
        selector.close()
    }

    /* For test usage */
    private[network] def channel(connectionId: String): Option[KafkaChannel] =
        Option(selector.channel(connectionId))

    /**
      * Wakeup the thread for selection.
      */
    @Override
    def wakeup = selector.wakeup()

}

class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {

    private val overrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
    private val counts = mutable.Map[InetAddress, Int]()

    def inc(address: InetAddress) {
        counts.synchronized {
            val count = counts.getOrElseUpdate(address, 0)
            counts.put(address, count + 1)
            val max = overrides.getOrElse(address, defaultMax)
            if (count >= max)
                throw new TooManyConnectionsException(address, max)
        }
    }

    def dec(address: InetAddress) {
        counts.synchronized {
            val count = counts.getOrElse(address,
                throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
            if (count == 1)
                counts.remove(address)
            else
                counts.put(address, count - 1)
        }
    }

    def get(address: InetAddress): Int = counts.synchronized {
        counts.getOrElse(address, 0)
    }

}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))
