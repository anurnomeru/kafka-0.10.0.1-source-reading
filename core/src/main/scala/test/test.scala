package test

/**
  * Created by Anur IjuoKaruKas on 2018/9/20
  */
object test {

  def main(args: Array[String]): Unit = {

    val fun: Int => Long = f => f + 5
    println(fun.compose[String](String => String.toInt).andThen(l => l * 5).apply("10"))

    //    testConsole.operationAnother()
    //    testConsole.printAnother()
  }

}
