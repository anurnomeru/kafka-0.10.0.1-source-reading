package test

/**
  * Created by Anur IjuoKaruKas on 2018/9/20
  */
class TestConsole {
  var function: Int => Long = _

  def initFunction(function: Int => Long): Int => Long = {
    this.function = function
    function
  }

  def compose[A](composer: A => Int): Unit = {
    function.compose(composer)
  }

  def apply(num: Int): Unit = {
    println(function.apply(num))
  }

  var testListAnother: List[Int] = Nil

  def operationAnother(): Unit = {
    testListAnother ::= 1
    testListAnother ::= 2
    testListAnother ::= 4
    testListAnother ::= 3
  }

  def printAnother(): Unit = {
    for (elem <- testListAnother) {
      println("item: " + elem)
    }
  }
}
