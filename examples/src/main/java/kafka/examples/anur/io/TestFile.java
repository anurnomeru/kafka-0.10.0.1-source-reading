package kafka.examples.anur.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;

/**
 * Created by Anur IjuoKaruKas on 2018/10/9
 */
public class TestFile {

    public static void main(String[] args) throws IOException {

        ByteBuffer byteBuffer = ByteBuffer.allocate(100);
        byteBuffer.putInt(10);
        byteBuffer.putLong(88);

        byteBuffer.flip();
        System.out.println(byteBuffer.getLong());
        System.out.println(byteBuffer.getInt());

        RandomAccessFile randomAccessFile = new RandomAccessFile("C:\\Users\\Anur\\Desktop\\test.txt", "rw");

        GatheringByteChannel channel = randomAccessFile.getChannel();

        ((FileChannel) channel).position(((FileChannel) channel).size());
        for (int j = 0; j < 100; j++) {

            byteBuffer = ByteBuffer.allocate(100);

            for (int i = 0; i < 99; i++) {
                byteBuffer.put(new byte[] {127});
            }

            int limit = byteBuffer.position();

            byteBuffer.flip();

            int write = 0;

            while (write < limit) {
                write += channel.write(byteBuffer);
            }
        }
    }
}
