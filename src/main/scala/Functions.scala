object Functions {

  def main(args: Array[String]): Unit = {

//    def sum(a:Int, b:Int):Int={
//      a=a+10
//      b=b-3
//      a+b
//    }

    def doubler(a: Int): (Int, Double) = {
      (a * 2, a*0.1)
    }
    println(doubler(10))

//    def func(x: Int, f: Int => Int): Int = {
//      f(x)
//    }
//
//    print(func(10,doubler))

    def sum(arr:Array[Int]): Unit = {
      arr(0)=200
      for(i<-0 to arr.length-1){
        println(arr(i))
      }
    }

    sum(Array(12,23,45,67,89,87))
//    print(a)

//    def func(a:Int,b:Int): Int = {
//      a+b
//    }

    def func(a: Int, b: Int): Int = {
      a * b
    }
//    print(func(10,20))

    val arr=Array(10,23,45,67,89,90)
    arr.filter(x=>x%2==0)

    implicit val defaultval="Fahima"
    def greet (implicit name:String):Unit={
      println(name)

    }
    greet

    implicit def inttostring(x:Int):String={
      x.toString
    }

    val a:String=123
  }

}
