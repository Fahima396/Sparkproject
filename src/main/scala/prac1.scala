import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{add_months, coalesce, col, column, date_add, date_format, date_sub, datediff, dayofmonth, initcap, lit, month, to_date, to_timestamp, when, year}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import shaded.parquet.org.apache.thrift.Option.some

import java.io.FileNotFoundException

object prac1 {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("spark-program")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val ddlschema = "id Int,age Int,mean Int,Name String"


    try {

      print(" am entering try block ")
      val mylist = List(

        (1, "mohan", 56),
        (2, "veer", 45),
        (3, "vinay", 67)

      )


//      val df = spark.createDataFrame(mylist)
//
//      val df2 = df.toDF("id", "name", "age")
//
//
//      def isEligible(df: DataFrame): DataFrame = {
//
//
//        df.select(col("id"),
//          when(col("age") > 55, lit("eleigible")).otherwise(lit("not-eligible")).alias("status")
//        )
//
//
//      }
//
//      isEligible(df2).show()

//      1.Conditional Column: Data: A DataFrame employees with columns id, name, age.
//        val employees = List(
//          (1, "AJAY", 28),
//          (2, "VIJAY", 35),
//          (3, "MANOJ", 22)
//        )
//        val df=spark.createDataFrame(employees)
//      println(df)
//        val df3=df.toDF("id", "name", "age")
//      println(df3)
//
//        def is_adult(df: DataFrame):DataFrame={
//    //        Question: How would you add a new column is_adult which is true if the age is greater than or equal
//            //to 18, and false otherwise?
//
//            df.withColumn("is_adult", when(col("age")>=18, "true").otherwise("false"))
//        }
//
//      is_adult(df3).show()
//-----------------------------------------------------------------------------------------------------
//      2)Categorizing Values: Data: A DataFrame grades with columns student_id, score.
      val grades = List(
        (1, 85),
        (2, 42),
        (3, 73)
      )

      val gradedf=spark.createDataFrame(grades)
      val df4=gradedf.toDF("student_id", "score")
//      Question: How would you add a new column grade with values "Pass" if score is greater than or
//      equal to 50, and "Fail" otherwise?
      def passORfail(gradedf:DataFrame):DataFrame={

        gradedf.withColumn("grade", when(col("score") >= 50, "Pass").otherwise("Fail"))
      }

      passORfail(df4).show()

//      -------------------------------------------------------------------------------
//      3)Multiple Conditions: Data: A DataFrame transactions with columns transaction_id, amount.
      val transactions = List(
        (1, 1000),
        (2, 200),
        (3, 5000)
      )
      val tranDF=spark.createDataFrame(transactions)
      val df5= tranDF.toDF("transaction_id", "amount")
//      Question: How would you add a new column category with values "High" if amount is greater than
//        1000, "Medium" if amount is between 500 and 1000, and "Low" otherwise?

      def category(tranDF:DataFrame):DataFrame={

        tranDF.withColumn("category", when(col("amount")>1000, "High")
                      .when(col("amount").between(500, 1000), "Medium").otherwise("Low"))
      }

      category(df5).show()
//      ----------------------------------------------------------------------------------------------

//      Question: How would you add a new column price_range with values "Cheap" if price is less than 50,
      //"Moderate" if price is between 50 and 100, and "Expensive" otherwise?
      val products = List(
        (1, 30.5),
        (2, 150.75),
        (3, 75.25)
      ).toDF("product_id", "price")
      def cheap(products: DataFrame):DataFrame={
        products.withColumn("price_range", when(col("price") < 50, lit("Cheap"))
        .when(col("price").between(50,100),lit("Moderate")).otherwise(lit("Expensive")))
      }
      cheap(products).show()

//      ------------------------------------------------------------------------------
//      Question: How would you add a new column is_holiday which is true if the date is "2024-12-25" or
      //"2025-01-01", and false otherwise?
      val events = List(
        (1, "2024-07-27"),
        (2, "2024-12-25"),
        (3, "2025-01-01")
      ).toDF("event_id", "date")
      def is_holiday(events:DataFrame):DataFrame={
        events.withColumn("is_holiday", when(col("date") === "2024-12-25" || col("date") ==="2025-01-01", lit("true"))
          .otherwise(lit("false")))
      }
      is_holiday(events).show()

//      ---------------------------------------------------------------------------------------------
//      Question: How would you add a new column stock_level with values "Low" if quantity is less than 10,
//      "Medium" if quantity is between 10 and 20, and "High" otherwise?
      val inventory = List(
        (1, 5),
        (2, 15),
        (3, 25)
      ).toDF("item_id", "quantity")
      def stock_level(inventory:DataFrame):DataFrame={

        inventory.withColumn("stock_level", when(col("quantity")<10, lit("Low"))
          .when(col("quantity").between(10,20), lit("Medium")).otherwise(lit("High")))

      }
      stock_level(inventory).show()
//      ------------------------------------------------------------------------------------------------------
//      Question: How would you add a new column email_provider with values "Gmail" if email contains
      //"gmail", "Yahoo" if email contains "yahoo", and "Other" otherwise?
    val customers = List(
      (1, "john@gmail.com"),
      (2, "jane@yahoo.com"),
      (3, "doe@hotmail.com")
    ).toDF("customer_id", "email")

    def email_provider(customers:DataFrame):DataFrame={
      customers.withColumn("email_provider", when(col("email").contains("gmail"), lit("gmail"))
                      .when(col("email").contains("yahoo"),lit("yahoo")).otherwise("other"))
    }
    email_provider(customers).show()

//      -----------------------------------------------------------------------------------------------------
//      Question: How would you add a new column season with values "Summer" if order_date is in June,
      //July, or August, "Winter" if in December, January, or February, and "Other" otherwise?
    val orders = List(
      (1, "2024-07-01"),
      (2, "2024-12-01"),
      (3, "2024-05-01")
    ).toDF("order_id", "order_date")

    def season(orders:DataFrame):DataFrame={
      orders.withColumn("season", when(month(col("order_date")).isin(6, 7, 8),lit("Summer"))
                                  .when(month(col("order_date")).isin(12, 1, 2),lit("Winter"))
                                   .otherwise(lit("Other")))
    }
    season(orders).show()
//      ---------------------------------------------------------------------------------------------
//      Question: How would you add a new column discount with values 0 if amount is less than 200, 10 if
//      amount is between 200 and 1000, and 20 if amount is greater than 1000?
    val sales = List(
      (1, 100),
      (2, 1500),
      (3, 300)
    ).toDF("sale_id", "amount")

    def discount(sales:DataFrame):DataFrame={
      sales.withColumn("discount", when(col("amount").between(200,1000),lit(10))
                         .when(col("amount") > 1000, lit(20))
                          .otherwise(lit(0)))
    }
    discount(sales).show()

//      --------------------------------------------------------------------------------------------------
//      Question: How would you add a new column is_morning which is true if login_time is before 12:00,
      //and false otherwise?
    val logins = List(
      (1, "09:00"),
      (2, "18:30"),
      (3, "14:00")
    ).toDF("login_id", "login_time")

    def is_morning(logins:DataFrame):DataFrame={
      logins.withColumn("is_morning", when(col("login_time") < "12:00", lit("true"))
      .otherwise("false"))
    }
    is_morning(logins).show()
//      --------------------------------------------------------------------------------------------------
//      Question: How would you add a new column category with values "Young & Low Salary" if age is less
//      than 30 and salary is less than 35000, "Middle Aged & Medium Salary" if age is between 30 and 40
//      and salary is between 35000 and 45000, and "Old & High Salary" otherwise?
    val employees = List(
      (1, 25, 30000),
      (2, 45, 50000),
      (3, 35, 40000)
    ).toDF("employee_id", "age", "salary")

    def categories(employees: DataFrame):DataFrame={
      employees.withColumn("category", when(col("age") < 30 && col("salary") < 35000, lit("Young & Low Salary"))
            .when(col("age").between(30,40) && col("salary").between(35000, 45000), lit("Middle Aged & Medium Salary"))
            .otherwise("Old & High Salary"))
    }

      categories(employees).show()

//      ----------------------------------------------------------------------------------------------------
//      Question: How would you add two new columns, feedback with values "Bad" if rating is less than 3,
//      "Good" if rating is 3 or 4, and "Excellent" if rating is 5, and is_positive with values true if rating is
//        greater than or equal to 3, and false otherwise?
    val reviews = List(
      (1, 1),
      (2, 4),
      (3, 5)
    ).toDF("review_id", "rating")

    def feedback(reviews:DataFrame):DataFrame={
      reviews.select(col("review_id"),col("rating"), when(col("rating")<3, lit("Bad"))
        .when(col("rating").between(3,4),lit("Good"))
      .otherwise("Excellent").alias("feedback")
      ,when(col("rating") >=3, lit("true")).otherwise("false").alias("is_positive")
      )
    }
    feedback(reviews).show()
//      ---------------------------------------------------------------------------------
    val data = List(
      ("karthik", "Data", 130000, "2017-06-10", some(4.7)),
      ("pratik", "QA", 85000, "2020-01-15", some(3.8)),
      ("veer", "Data", 60000, "2022-09-01", None),
      ("veena", "HR", 95000, "2019-03-20", some(2.9))
    ).toDF("name", "dept", "salary", "join_date", "rating")

      val datadf=data.withColumn("name_code",
        concat(
        initcap(col("name")),
          lit("_"),
          when(col("name") === "karthick","KT")
          .when(col("name") === "pratik","PR")
          .when(col("name") === "veer","VR")
          .when(col("name") === "veena","VN")
        ))
      val datadf1=datadf.withColumn("join_dt",to_date(col("join_date")))
        .withColumn("exp_years", months_between(current_date(),col("join_dt"))/12)

      val df6 = List(
        ("2023-10-07", "15:30:00")
      )
        val df7=spark.createDataFrame(df6)
          val df8=df7.toDF("date_str","time_str")


      val df9=df8.withColumn("date", to_date(col("date_str")))
        .withColumn("time", to_timestamp(col("time_str")))

      df9.show()

      val datedf=List(("2023-10-07", "2023-10-10")).toDF("date1","date2")
//      datedf.withColumn("date_diff", datediff(col("date2"), col("date1"))).show()
//      datedf.withColumn("date_add", date_add(col("date2"), 30)).show()
//      datedf.withColumn("date_sub", date_sub(col("date2"),30)).show()
//      datedf.withColumn("add_month", add_months(col("date2"),4)).show()

      val datedf1=List(("2023-10-07",null),(null,"2023-10-10")).toDF("date1","date2")
      datedf1.withColumn("dateHandling",
        when(col("date1").isNull,lit("2023-01-01")).otherwise(col("date1"))
      )
        .withColumn("datehandling2",coalesce(col("date2"),lit("2024-01-01"))).show()


      val datedf2=List(("2023-10-07")).toDF("date1")
      datedf2.withColumn("formatted_date", date_format(col("date1"), "yyyy.MM.dd")).show()

      datedf2.withColumn("year", year(col("date1")))
        .withColumn("month",month(col("date1")))
        .withColumn("day",dayofmonth(col("date1"))).show()


      val namedf = List(("fahima"),("ansari")).toDF("name")

      // Filter rows where 'name' ends with 'f'
      val namedf1 = namedf.where(col("name").endsWith("i"))
      namedf1.show()



    }
    catch {
      case ex: Exception => {

        print("an error occured")
      }

    }
    finally {
      print("i am finally block")

    }
  }
}