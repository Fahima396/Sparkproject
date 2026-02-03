import org.apache.spark.SparkContext

object FirstSpark {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[4]", "spark-program")

    val rdd1 = sc.textFile("D://DataEng Material/Fahi.txt")

    val rdd2 = rdd1.flatMap(x => x.split(" "))

    rdd2.foreach(println)

    val rdd3 = rdd2.map(x => (x, 1))

    rdd3.foreach(println)

    val rdd4 = rdd3.reduceByKey((x, y) => x + y)

    rdd4.foreach(println)

    val rdd5 = rdd4.collect()

    rdd5.foreach(println)


    scala.io.StdIn.readInt()

  }

}
