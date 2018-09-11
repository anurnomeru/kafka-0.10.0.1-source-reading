package kafka.examples.anur.compressor;

import java.nio.ByteBuffer;
import java.util.Arrays;
/**
 * Created by Anur IjuoKaruKas on 2018/9/7
 */
public class TestBuffer {

    public static ByteBuffer byteBuffer = ByteBuffer.allocate(10);

    public static void main(String[] args) {

        for (int i = 0; i < 100; i++) {
            put("TESTSEFSDFSDFSDF".getBytes());
        }
        System.out.println(Arrays.toString(byteBuffer.array()));
    }

    public static void put(byte[] bytes) {
        if (bytes.length > byteBuffer.remaining()) {
            allocate(bytes.length + byteBuffer.capacity());
        }
        byteBuffer.put(bytes);
    }

    public static void allocate(int size) {
        size = Math.max(size, (int) (byteBuffer.capacity() * 1.1));
        ByteBuffer temp = ByteBuffer.allocate(size);
        temp.put(byteBuffer.array(), byteBuffer.arrayOffset(), byteBuffer.limit());
        byteBuffer = temp;
    }
}
