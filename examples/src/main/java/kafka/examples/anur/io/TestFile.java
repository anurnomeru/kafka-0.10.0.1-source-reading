package kafka.examples.anur.io;

import java.io.IOException;
import java.io.RandomAccessFile;
/**
 * Created by Anur IjuoKaruKas on 2018/10/9
 */
public class TestFile {

    public static void main(String[] args) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile("C:\\Users\\Anur\\Desktop\\test.txt", "rw");
        randomAccessFile.writeInt(99);
        randomAccessFile.writeUTF("测试测试");

        randomAccessFile.seek(0);
        byte[] bytes = new byte[100];

        while (randomAccessFile.read(bytes) > 0) {
            System.out.println(new String(bytes));
        }
    }
}
