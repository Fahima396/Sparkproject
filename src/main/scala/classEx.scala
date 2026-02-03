//class classEx {
//
//    var a=10
//    var b=20
//    var c=a+b
//
//    def sum(x:Int,y:Int):Int={
//      x+y
//    }
//
//  }

//class classEx(val x:Int,val y:Int){
class classEx(val x:Int,val y:Int){

  var a=10
  var b=20
  var c=a+b
x+y
  def sum(d:Int,e:Int):Int= {
    d+e

  }


}

// Inheritance: i have sum function in class A and in class B as well i need the same function too so i will not
//create it instead i will acquire the property of class A so for this i will use the extends keywoed.
class A{
  def sum(d:Int,e:Int):Int= {
    d+e

  }
}

class B extends A{

  def sub(a:Int,b:Int):Int={
    a-b
  }

}

class C extends B{

}
object A{
  def main(args:Array[String]):Unit={


//    val obj=new classEx(20,30)
//    println(obj.a)
//
//    println(obj.b)
//
//    println(obj.c)
//
//    println(obj.x)
//
//    println(obj.y)
//    println(obj.sum(300,200))

    val obj=new C()
      println(obj.sum(20,20))
    println(obj.sub(20,10))


  }


}
