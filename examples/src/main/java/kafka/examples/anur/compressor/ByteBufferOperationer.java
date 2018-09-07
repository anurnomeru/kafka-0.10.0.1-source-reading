package kafka.examples.anur.compressor;

import java.nio.ByteBuffer;
/**
 * Created by Anur IjuoKaruKas on 2018/9/7
 */
public class ByteBufferOperationer {

    private ByteBuffer buffer;

    public ByteBufferOperationer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public void write(int b) {
        buffer.put((byte) b);
    }

    public void write(byte[] bytes, int off, int len) {
        buffer.put(bytes, off, len);
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    private void expandBuffer(int size) {
    }
}
