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

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util
import java.util.concurrent._

import com.yammer.metrics.core.{Gauge, Histogram, Meter}
import kafka.api._
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.{Logging, SystemTime}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.{ApiKeys, Protocol, SecurityProtocol}
import org.apache.kafka.common.requests.{ControlledShutdownRequest => _, FetchRequest => _, _}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.log4j.Logger


object RequestChannel extends Logging {
  val AllDone = Request(processor = 1, connectionId = "2", Session(KafkaPrincipal.ANONYMOUS, InetAddress.getLocalHost), buffer = getShutdownReceive(), startTimeMs = 0, securityProtocol = SecurityProtocol.PLAINTEXT)

  def getShutdownReceive() = {
    val emptyRequestHeader = new RequestHeader(ApiKeys.PRODUCE.id, "", 0)
    val emptyProduceRequest = new ProduceRequest(0, 0, new util.HashMap[TopicPartition, ByteBuffer]())
    RequestSend.serialize(emptyRequestHeader, emptyProduceRequest.toStruct)
  }

  case class Session(principal: KafkaPrincipal, clientAddress: InetAddress)

  /**
    * 存在哪个解析器、连接id、session用于鉴权、过来的包是什么
    * 生成的时间、安全协议
    */
  case class Request(processor: Int, connectionId: String, session: Session, private var buffer: ByteBuffer, startTimeMs: Long,
                     securityProtocol: SecurityProtocol) {
    // These need to be volatile because the readers are in the network thread and the writers are in the request
    // handler threads or the purgatory threads
    @volatile var requestDequeueTimeMs: Long = -1L
    @volatile var apiLocalCompleteTimeMs: Long = -1L
    @volatile var responseCompleteTimeMs: Long = -1L
    @volatile var responseDequeueTimeMs: Long = -1L
    @volatile var apiRemoteCompleteTimeMs: Long = -1L

    /**
      * 这是刚接收到的请求：
      *
      * 看起来这个requestId有两种，
      * FETCH(1, "Fetch"),
      * CONTROLLED_SHUTDOWN_KEY(7, "ControlledShutdown"),
      *
      * Comment on 2018/9/26 by Anur
      */
    val requestId: Short = buffer.getShort()

    // TODO: this will be removed once we migrated to client-side format
    // for server-side request / response format
    // NOTE: this map only includes the server-side request/response handlers. Newer
    // request types should only use the client-side versions which are parsed with
    // o.a.k.common.requests.AbstractRequest.getRequest()
    // TODO：一旦我们将解析移到客户端去做，这个将会被移除
    // 注意：这个map只包含服务端的 request/response 处理，较新的请求类型应当且仅为使用
    // o.a.k.common.requests.AbstractRequest.getRequest()进行解析的客户端版本
    private val keyToNameAndDeserializerMap: Map[Short, ByteBuffer => RequestOrResponse] =
    Map(ApiKeys.FETCH.id -> FetchRequest.readFrom,
      ApiKeys.CONTROLLED_SHUTDOWN_KEY.id -> ControlledShutdownRequest.readFrom
    )

    // TODO: this will be removed once we migrated to client-side format
    // TODO：一旦我们将解析迁移到client端，这个将会被移除
    val requestObj: RequestOrResponse =
    keyToNameAndDeserializerMap.get(requestId).map(readFrom => readFrom(buffer)).orNull // 获取到某个requestId，并将ByteBuffer转换为RequestOrResponse

    // if we failed to find a server-side mapping, then try using the
    // client-side request / response format
    // 如果我们在找server-side映射的时候失败了，尝试使用client-side的 请求和响应 解析
    val header: RequestHeader =
    if (requestObj == null) {
      buffer.rewind
      try RequestHeader.parse(buffer)// 解析请求头 todo： 待看
      catch {
        case ex: Throwable =>
          throw new InvalidRequestException(s"Error parsing request header. Our best guess of the apiKey is: $requestId", ex)
      }
    } else
      null
    val body: AbstractRequest =
      if (requestObj == null)
        try {
          // For unsupported version of ApiVersionsRequest, create a dummy request to enable an error response to be returned later
          // 对于不支持的ApiVersionsRequest版本，创建一个虚拟请求来开启返回一个错误响应
          if (header.apiKey == ApiKeys.API_VERSIONS.id && !Protocol.apiVersionSupported(header.apiKey, header.apiVersion))
            new ApiVersionsRequest
          else
            AbstractRequest.getRequest(header.apiKey, header.apiVersion, buffer)
        } catch {
          case ex: Throwable =>
            throw new InvalidRequestException(s"Error getting request for apiKey: ${header.apiKey} and apiVersion: ${header.apiVersion}", ex)
        }
      else
        null

    buffer = null
    private val requestLogger = Logger.getLogger("kafka.request.logger")

    def requestDesc(details: Boolean): String = {
      if (requestObj != null)
        requestObj.describe(details)
      else
        header.toString + " -- " + body.toString
    }

    trace("Processor %d received request : %s".format(processor, requestDesc(true)))

    def updateRequestMetrics() {
      val endTimeMs = SystemTime.milliseconds
      // In some corner cases, apiLocalCompleteTimeMs may not be set when the request completes if the remote
      // processing time is really small. This value is set in KafkaApis from a request handling thread.
      // This may be read in a network thread before the actual update happens in KafkaApis which will cause us to
      // see a negative value here. In that case, use responseCompleteTimeMs as apiLocalCompleteTimeMs.
      if (apiLocalCompleteTimeMs < 0)
        apiLocalCompleteTimeMs = responseCompleteTimeMs
      // If the apiRemoteCompleteTimeMs is not set (i.e., for requests that do not go through a purgatory), then it is
      // the same as responseCompleteTimeMs.
      if (apiRemoteCompleteTimeMs < 0)
        apiRemoteCompleteTimeMs = responseCompleteTimeMs

      val requestQueueTime = math.max(requestDequeueTimeMs - startTimeMs, 0)
      val apiLocalTime = math.max(apiLocalCompleteTimeMs - requestDequeueTimeMs, 0)
      val apiRemoteTime = math.max(apiRemoteCompleteTimeMs - apiLocalCompleteTimeMs, 0)
      val apiThrottleTime = math.max(responseCompleteTimeMs - apiRemoteCompleteTimeMs, 0)
      val responseQueueTime = math.max(responseDequeueTimeMs - responseCompleteTimeMs, 0)
      val responseSendTime = math.max(endTimeMs - responseDequeueTimeMs, 0)
      val totalTime = endTimeMs - startTimeMs
      val fetchMetricNames =
        if (requestId == ApiKeys.FETCH.id) {
          val isFromFollower = requestObj.asInstanceOf[FetchRequest].isFromFollower
          Seq(
            if (isFromFollower) RequestMetrics.followFetchMetricName
            else RequestMetrics.consumerFetchMetricName
          )
        }
        else Seq.empty
      val metricNames = fetchMetricNames :+ ApiKeys.forId(requestId).name
      metricNames.foreach { metricName =>
        val m = RequestMetrics.metricsMap(metricName)
        m.requestRate.mark()
        m.requestQueueTimeHist.update(requestQueueTime)
        m.localTimeHist.update(apiLocalTime)
        m.remoteTimeHist.update(apiRemoteTime)
        m.throttleTimeHist.update(apiThrottleTime)
        m.responseQueueTimeHist.update(responseQueueTime)
        m.responseSendTimeHist.update(responseSendTime)
        m.totalTimeHist.update(totalTime)
      }

      if (requestLogger.isTraceEnabled)
        requestLogger.trace("Completed request:%s from connection %s;totalTime:%d,requestQueueTime:%d,localTime:%d,remoteTime:%d,responseQueueTime:%d,sendTime:%d,securityProtocol:%s,principal:%s"
          .format(requestDesc(true), connectionId, totalTime, requestQueueTime, apiLocalTime, apiRemoteTime, responseQueueTime, responseSendTime, securityProtocol, session.principal))
      else if (requestLogger.isDebugEnabled)
        requestLogger.debug("Completed request:%s from connection %s;totalTime:%d,requestQueueTime:%d,localTime:%d,remoteTime:%d,responseQueueTime:%d,sendTime:%d,securityProtocol:%s,principal:%s"
          .format(requestDesc(false), connectionId, totalTime, requestQueueTime, apiLocalTime, apiRemoteTime, responseQueueTime, responseSendTime, securityProtocol, session.principal))
    }
  }

  case class Response(processor: Int, request: Request, responseSend: Send, responseAction: ResponseAction) {
    request.responseCompleteTimeMs = SystemTime.milliseconds

    def this(processor: Int, request: Request, responseSend: Send) =
      this(processor, request, responseSend, if (responseSend == null) NoOpAction else SendAction)

    def this(request: Request, send: Send) =
      this(request.processor, request, send)
  }

  trait ResponseAction

  /** 表示这个Response需要发送给客户端，首先查找对应的KafkaChannel，为其注册OP_WRITE事件，并将KafkaChannel的Send字段指向待发送Response对象
    * 同时会将Response从responseQueue移除，放入inflightResponses中（在发完就会取消关注OP_WRITE） */
  case object SendAction extends ResponseAction

  /** 表示此连接不需要相应，所以只需要关注一下OP_READ */
  case object NoOpAction extends ResponseAction

  /** 表示断开了连接 */
  case object CloseConnectionAction extends ResponseAction

}

class RequestChannel(val numProcessors: Int, val queueSize: Int) extends KafkaMetricsGroup {
  // 一个空的列表，里面装载着关于某个node_id的listener
  private var responseListeners: List[Int => Unit] = Nil
  // 这是刚接收到的请求
  private val requestQueue = new ArrayBlockingQueue[RequestChannel.Request](queueSize)
  // 装request的阻塞队列
  private val responseQueues = new Array[BlockingQueue[RequestChannel.Response]](numProcessors) // 装response LinkedBlockingQueue的阻塞队列
  for (i <- 0 until numProcessors)
    responseQueues(i) = new LinkedBlockingQueue[RequestChannel.Response]() // 无界队列

  newGauge(
    "RequestQueueSize",
    new Gauge[Int] {
      def value: Int = requestQueue.size
    }
  )

  newGauge("ResponseQueueSize", new Gauge[Int] {
    def value: Int = responseQueues.foldLeft(0) { (total, q) => total + q.size() }
  })

  for (i <- 0 until numProcessors) {
    newGauge("ResponseQueueSize",
      new Gauge[Int] {
        def value: Int = responseQueues(i).size()
      },
      Map("processor" -> i.toString)
    )
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request
    * 将一个request发去处理(通过socket发到broker的请求)，可能会阻塞，直到queue有空间来存放这个request
    *
    * 刚接收到的请求 */
  def sendRequest(request: RequestChannel.Request) {
    requestQueue.put(request)
  }

  /** Send a response back to the socket server to be sent over the network */
  def sendResponse(response: RequestChannel.Response) {
    responseQueues(response.processor).put(response)
    for (onResponse <- responseListeners) // 循环调用监听器，在即将发送消息给其他地方之前
      onResponse(response.processor)
  }

  /** No operation to take for the request, need to read more over the network */
  def noOperation(processor: Int, request: RequestChannel.Request) {
    // 不用响应，所以在Processor中只要关注一下READ就可以了
    responseQueues(processor).put(RequestChannel.Response(processor, request, null, RequestChannel.NoOpAction))
    for (onResponse <- responseListeners)
      onResponse(processor)
  }

  /** Close the connection for the request */
  def closeConnection(processor: Int, request: RequestChannel.Request) {
    responseQueues(processor).put(RequestChannel.Response(processor, request, null, RequestChannel.CloseConnectionAction))
    for (onResponse <- responseListeners)
      onResponse(processor)
  }

  /** Get the next request or block until specified time has elapsed */
  def receiveRequest(timeout: Long): RequestChannel.Request =
    requestQueue.poll(timeout, TimeUnit.MILLISECONDS)

  /** Get the next request or block until there is one */
  def receiveRequest(): RequestChannel.Request =
    requestQueue.take()

  /** Get a response for the given processor if there is one */
  def receiveResponse(processor: Int): RequestChannel.Response = {
    val response = responseQueues(processor).poll() // 拿出一个response
    if (response != null)
      response.request.responseDequeueTimeMs = SystemTime.milliseconds
    response
  }

  def addResponseListener(onResponse: Int => Unit) {
    responseListeners ::= onResponse
  }

  def shutdown() {
    requestQueue.clear()
  }
}

object RequestMetrics {
  val metricsMap = new scala.collection.mutable.HashMap[String, RequestMetrics]
  val consumerFetchMetricName: String = ApiKeys.FETCH.name + "Consumer"
  val followFetchMetricName: String = ApiKeys.FETCH.name + "Follower"
  (ApiKeys.values().toList.map(e => e.name)
    ++ List(consumerFetchMetricName, followFetchMetricName)).foreach(name => metricsMap.put(name, new RequestMetrics(name)))
}

class RequestMetrics(name: String) extends KafkaMetricsGroup {
  val tags = Map("request" -> name)
  val requestRate: Meter = newMeter("RequestsPerSec", "requests", TimeUnit.SECONDS, tags)
  // time a request spent in a request queue
  val requestQueueTimeHist: Histogram = newHistogram("RequestQueueTimeMs", biased = true, tags)
  // time a request takes to be processed at the local broker
  val localTimeHist: Histogram = newHistogram("LocalTimeMs", biased = true, tags)
  // time a request takes to wait on remote brokers (currently only relevant to fetch and produce requests)
  val remoteTimeHist: Histogram = newHistogram("RemoteTimeMs", biased = true, tags)
  // time a request is throttled (only relevant to fetch and produce requests)
  val throttleTimeHist: Histogram = newHistogram("ThrottleTimeMs", biased = true, tags)
  // time a response spent in a response queue
  val responseQueueTimeHist: Histogram = newHistogram("ResponseQueueTimeMs", biased = true, tags)
  // time to send the response to the requester
  val responseSendTimeHist: Histogram = newHistogram("ResponseSendTimeMs", biased = true, tags)
  val totalTimeHist: Histogram = newHistogram("TotalTimeMs", biased = true, tags)
}

