object ScalaDemo {
  def main(args:Array[String]):Unit= {


    var a=10
    var b=20
    a=a+b
    b=a-b
    a=a-b

//    print("with airthmetic operators: ",a,b)


    a=a^b
      b=a^b
      a=a^b
      print("with XOR operators: ", a,b)





  }

}
