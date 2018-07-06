package kafka.examples.anur.selector;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
/**
 * Created by Anur IjuoKaruKas on 7/5/2018
 */
public class NioExample {

    public void selector() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);// 申请1k内存

        Selector selector = Selector.open();// 打开轮询器

        ServerSocketChannel ssc = ServerSocketChannel.open();//
        ssc.configureBlocking(false);// 设置为非阻塞方式
        ssc.socket()
           .bind(new InetSocketAddress(8080));
        ssc.register(selector, SelectionKey.OP_ACCEPT);// 注册监听的事件（注册key到selector）

        while (true) {
            Set<SelectionKey> selectedKeys = selector.selectedKeys();// 取得所有注册的key集合
            Iterator<SelectionKey> it = selectedKeys.iterator();
            while (it.hasNext()) {
                SelectionKey key = it.next();
                if ((key.readyOps() & SelectionKey.OP_ACCEPT) == SelectionKey.OP_ACCEPT) {
                    ServerSocketChannel ssChannel = (ServerSocketChannel) key.channel();
                    SocketChannel sc = ssChannel.accept();//接受到服务端的请求
                    sc.configureBlocking(false);
                    sc.register(selector, SelectionKey.OP_READ);
                    it.remove();
                } else if
                    ((key.readyOps() & SelectionKey.OP_READ) == SelectionKey.OP_READ) {
                    SocketChannel sc = (SocketChannel) key.channel();
                    while (true) {
                        buffer.clear();
                        int n = sc.read(buffer);//读取数据
                        if (n <= 0) {
                            break;
                        }
                        buffer.flip();
                    }
                    it.remove();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        //        File file = new File("xxx");
        //
        //        FileInputStream inputStream = new FileInputStream(file);

        RandomAccessFile aFile = new RandomAccessFile("C:/Users/Anur/Desktop/New Text Document.txt", "rw");

        FileChannel inChannel = aFile.getChannel();

        ByteBuffer buf = ByteBuffer.allocate(200);
        int bytesRead = inChannel.read(buf);
        while (bytesRead != -1) {
            System.out.println("Read " + bytesRead);
            buf.flip();
            while (buf.hasRemaining()) {
                System.out.print((char) buf.get());
            }
            buf.clear();
            bytesRead = inChannel.read(buf);
        }
        aFile.close();

        File file = new File("C:/Users/Anur/Desktop/New Text Document.txt");
        InputStream in = null;

        try {
            System.out.println("以字节为单位读取文件内容，一次读一个字节：");
            // 一次读一个字节
            in = new FileInputStream(file);
            int tempbyte;
            while ((tempbyte = in.read()) != -1) {
                System.out.write(tempbyte);
            }
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
    }
}
