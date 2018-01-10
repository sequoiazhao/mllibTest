package com.other

/**
  * @author zhaoming on 2017-12-28 19:01
  **/
object TestScala {
  def main(args: Array[String]): Unit = {
    val response: String = "dddd"
    response match {
      case s if s != null => println("received " + s)
      case s => println("s is null")
    }

    println(power(2, 8))

    println(max(20, 12, 2))

    println(sum(10,2,3,3,4,5,5))

    println(identity[String]("hello"))

    val doubler = (x:Int)=>x*2

    println(doubler(20))

    println(safeStringOp("Resadf",(s:String)=>s.reverse))

    println(safeStringOp("fwfwefweef",s => s.reverse))

    println(safeStringOp("FEfewfwef", _.reverse))
  }

  @annotation.tailrec
  def power(x: Int, n: Int, t: Int = 1): Int = {
    if (n < 1) t
    else power(x, n - 1, x * t)
  }

  def max(a: Int, b: Int, c: Int) = {
    def max(x: Int, y: Int) = if (x > y) x else y

    max(a, max(b, c))
  }

  def sum(items: Int*): Int = {
    var total = 0
    for (i <- items) total += i
    total
  }

  def identity[A](a:A):A =a


  def safeStringOp(s:String, f:String=>String)={
    if(s!=null) f(s) else s
  }

  def operate(x:Int,y:Int,f:(Int,Int)=>Int ) = f(x,y)
}
