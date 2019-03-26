package test

import java.io.{File, RandomAccessFile}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.locks.ReentrantLock

import kafka.utils.{CoreUtils, Logging}


/**
  * Created by Anur IjuoKaruKas on 2018/9/20
  */
object test {


    class OffsetIndex(@volatile private[this] var num: Int) extends Logging {

        private val lock = new ReentrantLock

        /* initialize the memory mapping for this index */
        @volatile
       var mmap: Int = num;

        /* the number of eight-byte entries currently in the index */
        @volatile
        var _entries = mmap

        /* The maximum number of eight-byte entries this index can hold */
        @volatile
        var _maxEntries = mmap


        def test(): Unit = {
            mmap = mmap + 1
        }

    }

    def main(args: Array[String]): Unit = {
        val t = new OffsetIndex(100)
        println(t._entries)
        t.test()
        t.test()
        t.test()
        t.test()
        println(t._entries)
    }

}
