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

package org.apache.kafka.common.network;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.security.Principal;
import org.apache.kafka.common.utils.Utils;

/**
 * 总的来说，就是封装了
 * nodeId
 * 认证器
 * tpLayer( selectionKey、socketChannel )
 * 收、发buffer的一个集成类
 */
public class KafkaChannel {

    // 节点id
    private final String id;

    // 封装了SocketChannel和SelectionKey
    private final Authenticator authenticator;

    private final TransportLayer transportLayer;

    private final int maxReceiveSize;

    // 底层是byteBuffer
    private NetworkReceive receive;

    // 底层是byteBuffer
    private Send send;

    public KafkaChannel(String id, TransportLayer transportLayer, Authenticator authenticator, int maxReceiveSize) throws IOException {
        this.id = id;
        this.transportLayer = transportLayer;
        this.authenticator = authenticator;
        this.maxReceiveSize = maxReceiveSize;
    }

    /**
     * closeable.close();
     *
     * 对于 PlainTextTpLayer来说：
     * 就是关闭 socketChannel，再关闭
     *
     * public void close() throws IOException {
     * 　　　try {
     * 　　　　　socketChannel.socket()
     * 　　　　　　.close();
     * 　　　　　socketChannel.close();
     * 　　　} finally {
     * 　　　　　key.attach(null);
     * 　　　　　key.cancel();
     * 　　　　　}
     * 　　　}
     */
    public void close() throws IOException {
        Utils.closeAll(transportLayer, authenticator);
    }

    /**
     * Returns the principal returned by `authenticator.principal()`.
     */
    public Principal principal() throws IOException {
        return authenticator.principal();
    }

    /**
     * Does handshake of transportLayer and authentication using configured authenticator
     * tpLayer去做握手，然后用配置的认证器去做认证
     */
    public void prepare() throws IOException {

        // 如果是 PlainTextTpLayer的话，这里是个空实现
        if (!transportLayer.ready()) {
            transportLayer.handshake();
        }

        // 对于PlainTextTpLayer，主要就是做一下认证
        if (transportLayer.ready() && !authenticator.complete()) {
            authenticator.authenticate();
        }
    }

    public static void main(String[] args) {
        System.out.println(reorganizeString("aaaabbb"));
    }

    static byte[] sBytes;

    static int length;

    public static String reorganizeString(String S) {
        sBytes = S.getBytes();

        length = sBytes.length;

        for (int i = 0; i < length - 1; i++) {
            if (sBytes[i] == sBytes[i + 1]) {
                System.out.println(new String(sBytes));
                if (!safeExchange(i, i + 1)) {
                    return "";
                }
            }
        }
        return new String(sBytes);
    }

    public static boolean safeExchange(int from, int to) {
        if (from == to) {// 实在换不了就跳出所有循环
            return false;
        }

        if (sBytes[from] != sBytes[to]) {//　不同才能换
            int exchange = 0;
            if (to > 0) {// 防止数组越界
                if (sBytes[to - 1] != sBytes[from + 1]) {// 换过去还是不符合规矩，等于白换
                    exchange++;
                }
            } else {
                exchange++;
            }

            if (to != length - 1) {// 防止数组越界
                if (sBytes[to + 1] != sBytes[from + 1]) {
                    exchange++;
                }
            } else {
                exchange++;
            }

            if (exchange == 2) {// 满足条件就换
                byte temp = sBytes[from + 1];
                sBytes[from] = sBytes[to];
                sBytes[to] = temp;
                return true;
            }
        }

        if ((to + 1) < length) {
            to = to + 1;
        } else {
            to = 0;
        }
        return safeExchange(from, to);
    }

    /**
     * 接收数据，将数据保存在 NetworkReceive
     */
    public NetworkReceive read() throws IOException {
        NetworkReceive result = null;

        if (receive == null) {
            receive = new NetworkReceive(maxReceiveSize, id);
        }

        receive(receive);
        if (receive.complete()) {
            receive.payload()
                   .rewind();
            result = receive;
            receive = null;
        }
        return result;
    }

    public void disconnect() {
        transportLayer.disconnect();
    }

    /**
     * 通过tplayer判断socketChannel是否已经连接好了，
     *
     * 如果已经连接好了
     * key 移除连接事件的监听，增加read的监听
     */
    public boolean finishConnect() throws IOException {
        return transportLayer.finishConnect();
    }

    public boolean isConnected() {
        return transportLayer.isConnected();
    }

    public String id() {
        return id;
    }

    public void mute() {
        transportLayer.removeInterestOps(SelectionKey.OP_READ);
    }

    public void unmute() {
        transportLayer.addInterestOps(SelectionKey.OP_READ);
    }

    public boolean isMute() {
        return transportLayer.isMute();
    }

    public boolean ready() {
        return transportLayer.ready() && authenticator.complete();
    }

    public boolean hasSend() {
        return send != null;
    }

    /**
     * Returns the address to which this channel's socket is connected or `null` if the socket has never been connected.
     *
     * If the socket was connected prior to being closed, then this method will continue to return the
     * connected address after the socket is closed.
     */
    public InetAddress socketAddress() {
        return transportLayer.socketChannel()
                             .socket()
                             .getInetAddress();
    }

    public String socketDescription() {
        Socket socket = transportLayer.socketChannel()
                                      .socket();
        if (socket.getInetAddress() == null) {
            return socket.getLocalAddress()
                         .toString();
        }
        return socket.getInetAddress()
                     .toString();
    }

    /**
     * 设置send字段，并关注 OP_WRITE 事件
     */
    public void setSend(Send send) {
        if (this.send != null) {
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress.");
        }
        this.send = send;

        // 关注 OP_WRITE 事件
        // key.interestOps(key.interestOps() | ops);
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }

    // 如果send 不能 空，并且发送成功了，返回send，否则返回null
    public Send write() throws IOException {
        Send result = null;
        if (send != null && send(send)) {
            result = send;
            send = null;
        }
        return result;
    }

    /**
     * 把tpLayer的数据读取到receive
     */
    private long receive(NetworkReceive receive) throws IOException {
        return receive.readFrom(transportLayer);
    }

    /**
     * 简单来说，就是将Send 里面的 buffer 转移到 TransportLayer(传输层)里的channel
     * Send 实际上就是对 bytebufer 的封装
     */
    private boolean send(Send send) throws IOException {
        send.writeTo(transportLayer);

        // 判断completed，首先要没有剩余字节，其次不在【添加中】
        if (send.completed()) {
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
        }

        return send.completed();
    }
}
