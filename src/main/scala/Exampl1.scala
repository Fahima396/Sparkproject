import org.apache.spark.SparkContext

object Exampl1 {
  def main(args:Array[String]):Unit={

    val sc = new SparkContext ("local[4]", "spark-program")
//    val data=List(1,2,3,4,56,7,89,4)
//    val rdd=sc.parallelize(data)
//    val d=rdd.filter(x=>x%2==0)
//    d.collect.foreach(println)

//    find average of the given data 1.method
//    val avg=rdd.mean()
//    println(avg)
//
//    // 2. by using reduce
//    val sum = rdd.reduce((x,y)=>x+y)
//    val c=rdd.count()
//    val aveg=sum/c.toDouble
//    println(aveg)

////    remove duplicates
//    val udata=rdd.distinct()
//    udata.collect().foreach(println)

    // Filter
//    val data1=List("apple", "orange", "banana", "mango")
//    val rdd1=sc.parallelize(data1)
//    val searchelement="an"
//    val expectData=rdd1.filter(x=>x.contains(searchelement))
//    expectData.collect.foreach(println)

    val data1=List(1,2,3,4,6)
    val data2=List(3,4,5,6,7,8)
    val rdd=sc.parallelize(data1)
    val rdd1=sc.parallelize(data2)
//    val rdd3=rdd.union(rdd1)
//    val rdd3=rdd.intersection(rdd1)
//    val rdd3=rdd.subtract(rdd1)
    val rdd3=rdd.cartesian(rdd1)
    rdd3.coalesce(1).saveAsTextFile("file:///C:/Users/hp/Documents/file8/output")
//    rdd3.saveAsTextFile("file:///C:/Users/hp/Documents/file6")
//    sc.hadoopConfiguration.set("fs.permissions.enabled", "false")
//    rdd3.coalesce(1)
//      .saveAsTextFile("C:/Users/hp/Documents/file2")
//    rdd3.collect().foreach(println)



  }


}
