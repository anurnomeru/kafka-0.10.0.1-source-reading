package kafka.examples.anur.selector;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by Anur IjuoKaruKas on 7/6/2018
 */
public class Tester {

    //    public static void main(String[] args) throws Exception {
    //        ByteArrayInputStream inputStream = new ByteArrayInputStream("test test test!".getBytes());
    //        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    //
    //        FileOutputStream fileOutputStream = new FileOutputStream("C:\\Users\\Anur\\Desktop\\test.test");
    //
    //        int count = 0;
    //
    //        int result;
    //        while ((result = inputStream.read()) != -1) {
    //            fileOutputStream.write(result);
    //            System.out.println(count++);
    //        }
    //
    //        System.out.println(outputStream.toString());
    //
    //        fileOutputStream.close();
    //        outputStream.close();
    //        inputStream.close();
    //    }

    //    public static void main(String[] args) throws Exception {
    //        FileInputStream fileInputStream = new FileInputStream("C:\\Users\\Anur\\Desktop\\test.test");
    //
    //        FileOutputStream fileOutputStream = new FileOutputStream("C:\\Users\\Anur\\Desktop\\test.test1");
    //
    //        byte[] data = new byte[1024 * 2014];
    //
    //        int counter = 0;
    //
    //        while (fileInputStream.available() > 0) {
    //            fileInputStream.read(data);
    //            fileOutputStream.write(data);
    //            System.out.println(counter++);
    //        }
    //
    //        fileInputStream.close();
    //        fileOutputStream.close();
    //    }

    public static void main(String[] args) throws Exception {

        long sta = System.currentTimeMillis();

        //        RandomAccessFile aFile = new RandomAccessFile("C:\\Users\\Anur\\Desktop\\centos.iso", "rw");
        RandomAccessFile bFile = new RandomAccessFile("C:\\Users\\Anur\\Desktop\\test.txt", "rw");

        //        FileChannel fileChannela = aFile.getChannel();
        FileChannel fileChannelb = bFile.getChannel();
        long size = fileChannelb.size();

        ByteBuffer byteBuffer = ByteBuffer.allocate(200);
        byteBuffer.put("这是一个测试".getBytes("utf-8"));

        int count = 0;
        byteBuffer.flip();
        //        fileChannelb.write(byteBuffer, size);
        //        byteBuffer.rewind();
        while (byteBuffer.hasRemaining()) {
            System.out.println(count++);
            fileChannelb.write(byteBuffer, size);
        }
        //        fileChannelb.transferFrom(fileChannela, 0, size);
        //
        long e = System.currentTimeMillis();

        System.out.println(e - sta);


        if (permission.has(select)){
            return;
        } else {
            permission.add(select);
        }

        //        long sta = System.currentTimeMillis();
        //        FileInputStream fileInputStream = new FileInputStream("C:\\Users\\Anur\\Desktop\\centos.iso");
        //
        //        FileOutputStream fileOutputStream = new FileOutputStream("C:\\Users\\Anur\\Desktop\\centos.bak.iso");
        //        int result = fileInputStream.read();
        //
        //        while (result != -1) {
        //            fileOutputStream.write(result);
        //            result = fileInputStream.read();
        //        }
        //        long e = System.currentTimeMillis();
        //
        //        System.out.println(sta - e);
        //
        //        long start = System.currentTimeMillis();
        //        BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream("C:\\Users\\Anur\\Desktop\\test.rar"));
        //
        //        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream("C:\\Users\\Anur\\Desktop\\test1.rar"));
        //
        //        byte[] result = new byte[1];
        //
        //        int counter = 0;
        //
        //        while (inputStream.available() > 0) {
        //            inputStream.read(result);
        //            outputStream.write(result);
        //        }
        //
        //        inputStream.close();
        //        outputStream.close();
        //
        //        long end = System.currentTimeMillis();
        //
        //        System.out.println(end - start);
        //
        //        start = System.currentTimeMillis();
        //
        //        FileInputStream fileInputStream = new FileInputStream("C:\\Users\\Anur\\Desktop\\test.rar");
        //        FileOutputStream fileOutputStream = new FileOutputStream("C:\\Users\\Anur\\Desktop\\test2.rar");
        //
        //        byte[] result1 = new byte[1];
        //
        //        int counter1 = 0;
        //
        //        while (fileInputStream.available() > 0) {
        //            fileInputStream.read(result1);
        //            fileOutputStream.write(result1);
        //        }
        //
        //        fileInputStream.close();
        //        fileOutputStream.close();
        //
        //        end = System.currentTimeMillis();
        //
        //        System.out.println(end - start);
    }
}
