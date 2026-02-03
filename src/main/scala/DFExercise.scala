import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DFExercise {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
                          .appName("spark-program")
                          .master("local[*]")
                           .getOrCreate()

        val ddlschema="id Int,Name String,age Int"

//    val pgschema=StructType(List(
//      StructField("id",IntegerType,nullable=true),
//      StructField("Name", StringType, nullable = true),
//      StructField("age",IntegerType,nullable=true)
////      StructField("mean",IntegerType,nullable=true),
////      StructField("Name",StringType,nullable=true)
//    )
//    )


//    val df=spark.read
//      .format("csv")
//      .option("header",true)
//      .schema(ddlschema)
//      .option("path","C:/Users/Karthik Kondpak/Documents/info.csv")
//      .load()
    val df=spark.read
    .format("csv")
    .option("header",true)
    .schema(ddlschema)
    .option("mode","FAILFAST")
    .option("path","C://Users/hp/Downloads/customers-100.csv")
    .load()

    df.select(col("id"), col("name")).show()
    df.select(col("id"),col("name"),col("age"),
      when(col("age")>30 , lit("eligible"))
        .when(col("age")>10 && col("age")<30, lit("becoming eligible"))
        .otherwise(lit("not eligible")).alias("status")

    ).show()

    val mylist=List(
      (1,"Mohan",56),
      (2,"Veer", 67),
      (3,"viny",55)
    )

    val df1=spark.createDataFrame(mylist)

    val df2=df1.toDF("id","name","age")
    df2.show()


//    val df=spark.read
//      .format("json")
//      .option("multiline", true)
//      .option("path","C:/Users/hp/Downloads/file2json.json")
//      .load()

//    df.show()
    df.printSchema()

    val employees = List(
      (1, "AJAY", 28),
      (2, "VIJAY", 35),
      (3, "MANOJ", 22)
    )
    val df3=spark.createDataFrame(employees)
      val df4=df3.toDF("id", "name", "age")

//    Question: How would you add a new column is_adult which is true if the age is greater than or equal
//    to 18, and false otherwise?
    val df5=df4.select(
      col("id"),
      col("name"),
      col("age"),
      when(col("age")>=18,"true").otherwise("false").alias("is_adult"))
    df5.show()

    df.createOrReplaceTempView("karthik")

    spark.sql(

      """
       select * ,
       case
       when age>55 then "eligible"
       else "not-eligible"

       end as status
       from
       karthik



        """
    ).show()

  }


}
