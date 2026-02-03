object Loops {

  def main(args: Array[String]): Unit = {

    var num = 1

    for (i <- 1 to 5 by 1) {

      for (j <- 1 to i) {

        print(num + " ")
        num = num + 1
      }

      println(" ")
    }


    for (i <- 1 to 5 by 1) {
      for (j <- 1 to i) {
        print(j + " ")
      }
      println(" ")
    }
    print("-------------------------------------------------------")
    for (i <- 1 to 5 by 1) {
      for (j <- 1 to i) {
        print(i + " ")
      }
      println(" ")
    }

    print("Generate a pyramid only on Odd rows")
    println(" ")
    var n=5
    for (i <-1 to n) {
      if (i % 2 != 0) {
        for (j <- 1 to n-i) {
          print(" ")
        }
        for (_ <- 1 to 2*i-1) {
          print("*")
        }
        println(" ")
      }
    }

    print("Alphabet Triangle")
    println(" ")
    var a=5
    for (i <-1 to a) {
      if (i % 2 != 0) {
        for (_ <- 1 to (a - i - 1)) {
          print("")
        }
        for (_ <- 1 to (i)) {
          print("A")
        }
        println(" ")
      }
    }

    for (i <- 1 to 5){
      val chCaps = ('A' + (i-1)).toChar
      val chLower = ('a' + (i-1)).toChar
      for (j  <- 1 to i){
        if (i % 2 != 0){
//          val ch=(ch +"")
          print(chCaps+ " ")
        }
        else {
//          val ch=('a' +j).toChar
          print(chLower+ " ")
         }

      }
      println()
    }


    var d=1
    for (i <- 1 to  5) {
      for (j <- 1 to i) {
        if (j%2 != 0){
          print(d + " ")
          d+=2
        }
        else{
          print("_")
        }
//        d=d+1

      }
      println(" ")
    }

    println("========================================================")
    val c=5
    for (i <- c to 1 by -1){
      for(j <- 1 to (c-i)){
        print(" ")
      }
      for (_ <-1 to 2*i-1){
        print("*")
      }
      println(" ")
    }
    for(i <-2 to c){
      for (j<-1 to c-i){
        print(" ")
      }
      for (_ <-1 to 2*i-1){
        print("*")
      }
      println(" ")
    }


    println("========================================================")
    val s=5
    for (i <-1 until 2 * s){
      val spaces=math.abs(s - i)
      val stars=s - spaces
      print(" " * spaces)

      for (j <- 1 to stars){
        if (j == 1 || j == stars){
          print("* ")
        }
        else{
          print(" ")
        }
      }
      println()

    }

    // total rows = 2n - 1
    for (i <- 1 until 2 * s) {
      val spaces = math.abs(s - i)         // leading spaces
      val stars = s - spaces               // number of stars in row

      // print leading spaces
      print(" " * spaces)

      // print stars with hollow inside
      for (j <- 1 to stars) {
        if (j == 1 || j == stars) print("* ")
        else print("  ")
      }
      println()
    }

  }
}