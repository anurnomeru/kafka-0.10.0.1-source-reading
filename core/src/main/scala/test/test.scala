package test

import scala.collection.mutable

/**
  * Created by Anur IjuoKaruKas on 2018/9/20
  */
object test {

  def main(args: Array[String]): Unit = {
    val emptyMap: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
    emptyMap.+=(("1",1))
    val emptyMap2: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
    emptyMap2.+=(("2",1))

    emptyMap2
  }

}
