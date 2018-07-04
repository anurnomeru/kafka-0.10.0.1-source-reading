package kafka.examples.anur.selector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.Iterator;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Send;

/**
 * Created by Anur IjuoKaruKas on 7/4/2018
 */
@SuppressWarnings("Duplicates")
public class Server {

    private final java.nio.channels.Selector nioSelector;

    public Server() throws IOException {
        nioSelector = java.nio.channels.Selector.open();
    }

    public void connect(InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
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
        SelectionKey key = socketChannel.register(nioSelector, SelectionKey.OP_CONNECT);// 将当前这个socketChannel注册到nioSelector上，TODO：并关注OP_CONNECT事件

        if (connected) {
            // OP_CONNECT won't trigger for immediately connected channels
            key.interestOps(0);
        }
    }

    public void poll(long timeout) throws IOException {
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout should be >= 0");
        }

        /* check ready keys */
        int readyKeys = select(timeout);

        if (readyKeys > 0) {
            pollSelectionKeys(this.nioSelector.selectedKeys(), false);
        }
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

    private void pollSelectionKeys(Iterable<SelectionKey> selectionKeys, boolean isImmediatelyConnected) {
        Iterator<SelectionKey> iterator = selectionKeys.iterator();
        while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            iterator.remove();
            KafkaChannel channel = channel(key);

            // register all per-connection metrics at once
            sensors.maybeRegisterConnectionMetrics(channel.id());
            lruConnections.put(channel.id(), currentTimeNanos);

            try {

                /* complete any connections that have finished their handshake (either normally or immediately) */
                if (isImmediatelyConnected || key.isConnectable()) {
                    if (channel.finishConnect()) {
                        this.connected.add(channel.id());
                        this.sensors.connectionCreated.record();
                    } else {
                        continue;
                    }
                }

                /* if channel is not ready finish prepare */
                if (channel.isConnected() && !channel.ready()) {
                    channel.prepare();
                }

                /* if channel is ready read from any connections that have readable data */
                if (channel.ready() && key.isReadable() && !hasStagedReceive(channel)) {
                    NetworkReceive networkReceive;
                    while ((networkReceive = channel.read()) != null) {
                        addToStagedReceives(channel, networkReceive);
                    }
                }

                /* if channel is ready write to any sockets that have space in their buffer and for which we have data */
                if (channel.ready() && key.isWritable()) {
                    Send send = channel.write();
                    if (send != null) {
                        this.completedSends.add(send);
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
}
