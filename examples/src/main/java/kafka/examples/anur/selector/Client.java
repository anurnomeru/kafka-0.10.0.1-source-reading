package kafka.examples.anur.selector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * Created by Anur IjuoKaruKas on 7/4/2018
 *
 * 这个和kafka没关系，只是拿来接收消息并打印
 */
public class Client {

    public static void main(String[] args) throws IOException {
        //打开事件监听
        Selector selector = Selector.open();
        //获取SocketChannel
        SocketChannel socketChannel = SocketChannel.open();
        //设置连接到服务器IP和端口
        socketChannel.connect(new InetSocketAddress("127.0.0.1", 9527));
        //设置非阻塞
        socketChannel.configureBlocking(false);
        //注册连接事件
        socketChannel.register(selector, SelectionKey.OP_CONNECT);
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            if (socketChannel.isConnected()) {  //先连接上，才能发送消息
                String command = in.readLine();
                socketChannel.write(ByteBuffer.wrap(command.getBytes()));
                if ("exit".equalsIgnoreCase(command)) {
                    in.close();
                    selector.close();
                    socketChannel.close();
                    System.exit(0);
                }
            }
            int keys = selector.select(1000);
            if (keys > 0) {
                for (SelectionKey key : selector.selectedKeys()) {
                    if (key.isConnectable()) {  //连接上
                        //获取SocketChannel
                        SocketChannel channel = (SocketChannel) key.channel();
                        channel.configureBlocking(false);
                        channel.register(selector, SelectionKey.OP_READ);
                        channel.finishConnect();
                    } else if (key.isReadable()) {
                        //定义Buffer
                        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
                        //获取SocketChannel
                        SocketChannel channel = (SocketChannel) key.channel();
                        //读到buffer中
                        channel.read(byteBuffer);
                        //转为字节数组
                        byte[] array = byteBuffer.array();
                        String msg = new String(array).trim();
                        System.out.println("Receive From Server：" + msg);
                    }
                }
                //注意这里要移除键
                selector.selectedKeys()
                        .clear();
            }
        }
    }
}
