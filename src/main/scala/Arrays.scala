object Arrays {
  def main(args: Array[String]): Unit = {

    val arr = Array(10, 20, 30, 40, 50, 60, 70)

    print(arr(5))

    print(arr.length)

    // 1 method to print the entire element of an array
    for(i <-0 to arr.length-1)
      println(arr(i))

    // 2 method to print the entire element of an array
    arr.foreach(println)

    var list=List(10, 20, 30, 40, 50, 60, 70)

    // print the entire element in the list
    print(list)

    print(list.length)

    for (i <- 0 to list.length-1)
      println(list(i))


    val tup=(10, "Fahima", true, 20.44)

    println(tup._2)

    var myset=Set(10,20,30,40,50)
    println(myset)

    val map=Map(1-> "Apple", 2-> "Mango" , 3-> "Orange")
    print(map(1))

    print(map.getOrElse(2, "not found"))
    for(key<-map.keys){
      print(key)
    }
    for(value<-map.values){
      print(value)
    }

  }

}
