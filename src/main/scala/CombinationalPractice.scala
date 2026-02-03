import org.apache.spark.sql.functions.{avg, col, concat, count, current_date, datediff, initcap, lit, max, min, months_between, regexp_extract, regexp_replace, split, substring, sum, to_date, upper, when, year}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object CombinationalPractice {

  def main(args: Array[String]):Unit={

    try {

      val spark=SparkSession.builder().appName("spark-combinational")
        .master("local[*]").getOrCreate()

      import spark.implicits._

      val data = List(
        ("karthik", "Data", 130000, "2017-06-10", Some(4.7)),
        ("pratik", "QA", 85000, "2020-01-15", Some(3.8)),
        ("veer", "Data", 60000, "2022-09-01", None),
        ("veena", "HR", 95000, "2019-03-20", Some(2.9))
      ).toDF("name", "dept", "salary", "join_date", "rating")

      def dataDF(data:DataFrame):DataFrame={
        val probdf = data.withColumn(
          "name-code",
          concat(initcap(col("name")),
            lit(" "),
            upper(substring(col("name"), 1, 2))
          )
        )
//        probdf.show()
        val probdf1 = probdf.withColumn(
            "join_dt",
            to_date(col("join_date")))
          .withColumn(
            "year_since_promotion",
            months_between(current_date(), col("join_dt")) / 12)

//        probdf1.show()
        val probdf2 = probdf1.withColumn(
          "cost_category",
          when(col("salary") * 12 >= 1500000, lit("High Cost"))
            .when(col("salary") * 12 >= 900000, lit("Mid Cost")).otherwise("Low Cost"))

//        probdf2.show()
        val probdf3=probdf2.withColumn(
          "Risk Flag",
          when(col("rating").isNull, lit("Risk Unknown"))
            .when(col("year_since_promotion") > 4 && col("rating") < 3,
              lit("High Risk"))
            .when(col("year_since_promotion").between(2, 4),
              lit("Mediun Risk")).otherwise("Low Risk"))

        probdf3


      }
      dataDF(data).show()

//      data.createOrReplaceTempView("Fahima")

//      spark.sql(
//        """
//        SELECT *,
//               concat(initcap(name), " ", upper(substring(name, 1, 2))) AS name_code,
//               (months_between(current_date, to_date(join_date)) / 12) AS exp_in_years,
//               CASE
//                   WHEN (salary * 12) >= 150000 THEN "High Cost"
//                   WHEN (salary * 12) >= 90000 THEN "Mid Cost"
//                   ELSE "Low Cost"
//               END AS cost_category,
//               CASE
//                   WHEN rating IS NULL THEN "Risk Unknown"
//                   WHEN (months_between(current_date, to_date(join_date)) / 12) > 4 AND rating < 3 THEN "High Risk"
//                   WHEN (months_between(current_date, to_date(join_date)) / 12) BETWEEN 2 AND 4 THEN "Medium Risk"
//                   ELSE "Low Risk"
//               END AS Risk_flag
//        FROM Fahima
//        """
//      ).show()

      val data1 = List(

        ("karthik", "Bangalore", 130000, "2016-06-01", 22),
        ("pratik", "Pune", 85000, "2021-01-10", 18),
        ("veer", "Delhi", 60000, "2023-02-15", 5),
        ("veena", "Chennai", 95000, "2019-09-20", 10)
      ).toDF("name","location","salary","join_date", "leaves_taken")

      def emp_stability(data1:DataFrame):DataFrame={
        val data2=data1.withColumn(
          "Emp_Tag", concat(upper(col("name")), lit("-") , substring(col("name"),1,3)))

        data2.show()

        val data3=data2.withColumn("join_dt", to_date(col("join_date")))
      .withColumn("tenure_months", months_between(current_date(), col("join_dt")))

        data3.show()

        val data4=data3.withColumn(
          "Leave_Intensity",
          when(col("tenure_months") === 0 || col("tenure_months").isNull, lit(null))
            .otherwise((col("leaves_taken") / col("tenure_months"))*12))

        data4.show()

        val data5=data4.withColumn("Attrition_Probability", when((col("tenure_months") < 24) && (col("leave_intensity") > 15), lit("VERY_HIGH"))
          .when(col("tenure_months").between(24,48), lit("MEDIUM"))
          .when((col("tenure_months") > 48) && (col("leave_intensity") < 5), lit("Low")).otherwise(lit("MODERATE")))

        data5

      }

      emp_stability(data1).show()

      val CompData = List(
        ("karthik", "Data", 110000, "2015-04-10", 200000),
        ("pratik", "QA", 75000, "2020-06-01", 100000),
        ("veer", "Data", 60000, "2022-08-15", 50000),
        ("veena", "HR", 90000, "2017-02-20", 150000)

      ).toDF("name","dept", "base_salary","join_date","variable_pay")

      def compensation(CompData:DataFrame):DataFrame={
        val Compdf1=CompData.withColumn(
          "emp_key",
          concat(initcap(col("name")), lit("#"), year(col("join_date"))))

        Compdf1.show()

        val Compdf2=Compdf1.withColumn("total_compensation", col("base_salary") + col("variable_pay"))

        Compdf2.show()

        val Compdf3=Compdf2.withColumn("join_dt", to_date(col("join_date")))
          .withColumn("exp_slab", when((months_between(current_date(), col("join_dt"))/12) < 3, lit("FRESHER"))
            .when((months_between(current_date(), col("join_dt"))/12).between(3,6), lit("EXPERIENCED"))
            .otherwise(lit("VETERAN")))

        Compdf3.show()

        val Compdf4=Compdf3.withColumn("Fairness_flag", when(col("exp_slab")==="VETERAN" && col("total_compensation") < 1200000 , lit("UNDERPAID"))
          .when(col("exp_slab")==="FRESHER" && col("total_compensation") < 1000000 , lit("OVERPAIDRPAID")).otherwise(lit("FAIR")))

        Compdf4


      }

      compensation(CompData).show()

      val workforce = List(

        ("karthik", "Migration", 240, "2018-01-10", 80),
        ("pratik", "Testing", 200, "2020-07-15", 30),
        ("veer", "Analytics", 180, "2022-10-01", 10),
        ("veena", "Recruitment", 220, "2019-03-05", 60)
      ).toDF("name", "project", "monthly_hours", "join_date","overtime_hours")

      def workforce_utlization(workforce:DataFrame):DataFrame={

        val workforcsdf1=workforce.withColumn("project_code", concat(substring(col("project"),1,3),lit("_"), substring(col("name"),1,1)))
        workforcsdf1.show()
        val workforcsdf2=workforcsdf1.withColumn("avg_week_hrs", col("monthly_hours")/4)
        workforcsdf2.show()
        val workforcsdf3=workforcsdf2.withColumn("overtime_ratio", col("overtime_hours")/col("monthly_hours"))
        workforcsdf3.show()
        val workforcsdf4 = workforcsdf3.withColumn(
          "Burnout_status",
          when((col("avg_week_hrs") > 50) && (col("overtime_ratio") > 0.25), lit("CRITICAL"))
            .when(col("avg_week_hrs").between(40, 50), lit("WARNING"))
            .otherwise("NORMAL")
        )


        workforcsdf4

      }
      workforce_utlization(workforce).show()

      val careerDF = List(
        ("karthik", "Architect", 150000, "2014-06-01", 18),
        ("pratik", "Engineer", 90000, "2019-09-10", 12),
        ("veer", "Analyst", 65000, "2022-01-20", 8),
        ("veena", "Manager", 120000, "2017-11-05", 15)
      ).toDF("name", "designation", "current_salary", "join_date","last_hike_percent")

      def career_identifier(careerDF:DataFrame):DataFrame={

        val careerDF1=careerDF.withColumn(
          "career_identifier",
          concat(upper(col("name")), lit("@"), col("designation")))
        careerDF1.show()

        val careerDF2=careerDF1.withColumn(
          "exp_year",
          months_between(current_date(), to_date(col("join_date"))) / 12 )
        careerDF2.show()

        val careerDF3=careerDF2.withColumn(
          "growth_index",
          (col("current_salary") * col("last_hike_percent")) / col("exp_year") )

        careerDF3.show()

        val careerDF4=careerDF3.withColumn(
          "growth_category",
          when(col("growth_index") > 200000 , lit("FAST_TRACK"))
          .when(col("growth_index").between(100000, 200000) , lit("STEADY"))
          .otherwise("SLOW"))

        careerDF4


      }
      career_identifier(careerDF).show()

      val employee = List(
        ("karthik", "Data", 420, "2016-05-10", 2.5),
        ("pratik", "QA", 300, "2021-01-15", 3.8),
        ("veer", "Data", 180, "2023-03-01", 4.5),
        ("veena", "HR", 260, "2019-07-20", 3.2)
      ).toDF("name","dept","tasks_completed","join_date","avg_task_time_hours")

      def emp_productivity(employee: DataFrame):DataFrame={
        val employeeDF=employee.withColumn("productivity_id", concat(col("name"), lit("-"), col("dept")))
        val employeeDF1=employeeDF.withColumn("tenure_months", months_between(current_date(), to_date(col("join_date"))))
        val employeeDF2=employeeDF1.withColumn("productivity_score", (col("tasks_completed")*30)/col("avg_task_time_hours"))
        val employeeDF3=employeeDF2.withColumn("productivity_category", when(col("tenure_months") < 12 && col("productivity_score") < 50, lit("LOW_PRODUCTIVITY"))
          .when(col("productivity_score").between(50,120), lit("MODERATE"))
          .when(col("tenure_months") > 36 && col("productivity_score") > 120, lit("HIGH"))
          .otherwise("STABLE"))

        employeeDF3
      }

      emp_productivity(employee).show()

      val promotion = List(
        ("karthik", "Data", 140000, "2019-04-15", 4.8),
        ("pratik", "QA", 90000, "2021-06-01", 3.1),
        ("veer", "Data", 65000, "2023-01-10", 2.7),
        ("veena", "HR", 115000, "2020-02-20", 4.0)
      ).toDF("name", "dept","current_salary","last_hike_date","skill_score")

      def promotion_readiness(promotion:DataFrame):DataFrame={
        val promotionDF=promotion.withColumn("promotion_key",
          concat(substring(col("name"),1,2), lit("_"), col("dept"))
        )
        val promotionDF1=promotionDF.withColumn("exp_year",
          (months_between(current_date(), to_date(col("last_hike_date")))/12)
        )
        val promotionDF2=promotionDF1.withColumn("salary_momentum",
          col("current_salary") / col("exp_year")
        )
        val promotionDF3=promotionDF2.withColumn("promotion_Readiness",
          when(col("exp_year") > 3 && col("skill_score") > 4.5 , lit("READY"))
            .when(col("exp_year").between(2,3), lit("WATCHLIST"))
            .when(col("exp_year") < 2 && col("skill_score") < 3, lit("NOT_READY"))
            .otherwise("NEUTRAL"))

        promotionDF3
      }
      promotion_readiness(promotion).show()

      val emp_cost = List(
        ("karthik", "Bangalore", 120000, "2015-08-10", 300),
        ("pratik", "Pune", 85000, "2020-03-01", 180),
        ("veer", "Delhi", 60000, "2022-06-15", 120),
        ("veena", "Chennai", 100000, "2018-11-20", 200)
      ).toDF("name", "location","monthly_salary", "join_date","monthly_output_units")

      def emp_costEff(emp_cost:DataFrame):DataFrame={
        val emp_costDF1=emp_cost.withColumn("cost_tag", concat(initcap(col("name")), lit("@"), substring(upper(col("location")),1,1)))
        val emp_costDF2=emp_costDF1.withColumn("exp_years",months_between(current_date(), to_date(col("join_date")))/12 )
        val emp_costDF3=emp_costDF2.withColumn("cost_per_unit", (col("monthly_salary") * 12) / (col("monthly_output_units") * 12))
        val emp_costDF4=emp_costDF3.withColumn("efficiency_category",
          when(col("cost_per_unit") < 500 and col("exp_years") > 5 , lit("HIGHLY_EFFICIENT"))
            .when(col("cost_per_unit").between(500, 1200), lit("MODERATE"))
            .when(col("cost_per_unit") > 1200, lit("INEFFICIENT"))
            .otherwise("REVIEW")
        )

        emp_costDF4

      }

      emp_costEff(emp_cost).show()

      val email=List(
        ("U001", "john.doe@gmail.com", "2024-01-01", "2024-03-01"),
        ("U002", "jane.smith@outlook.com", "2024-02-15", "2024-03-10"),
        ("U003", "alice.jones@yahoo.org", "2024-03-01", "2024-03-20")
      ).toDF("user_id","email","signup_date","last_login")

      val emailDF=email.withColumn("email_domain", upper(regexp_extract(col("email"), "\\@([A-Za-z0-9.]+)$",1)))
      emailDF.show()
      val emailDF1=emailDF.withColumn("org_domain", when(col("email").endsWith("org"),col("email"))
        .otherwise("not available"))
      emailDF1.show()
//      val emailDF2=emailDF1.dropDuplicates("user_id")
      val emailDF3=emailDF1.groupBy(col("email_domain")).agg(count(col("user_id")).alias("count_of_emp"), avg(datediff( col("last_login"),col("signup_date"))).alias("avg_days"))
      emailDF3.show()

      val phone_data=List(
        ("C001", "+1-415-5551234", "2024-01-10", "WEST"),
        ("C002", "+44-20-79461234", "2024-02-20", "EAST"),
        ("C003", "+91-22-23451234", "2024-03-15", "NORTH")
      ).toDF("customer_id", "phone_number", "signup_date", "region")

      val phone_data1=phone_data.withColumn("country_code", regexp_extract(col("phone_number"),"\\+([0-9]+)",1))
        .withColumn("area_code", regexp_extract(col("phone_number"), "\\-([0-9]{1,3}+)",1))
        .withColumn("local_number", regexp_extract(col("phone_number"), "\\-([0-9]+)$",1))
      phone_data1.show()

      val productDF=List(

        ("P001", "A123-456-789", "2024-04-01", "electronics"),
        ("P002", "B234-567-891", "2024-05-10", "home_appliance"),
        ("P003", "X345-678-999", "2024-06-15", "furniture")
      ).toDF("product_id", "product_code", "release_date", "category")

      val prodDF=productDF.withColumn("brand_code", regexp_extract(col("product_code"), "^([A-Z0-9]+)",1))
        .withColumn("category_code", regexp_extract(col("product_code"), "\\-([0-9]{1,3}+)",1))
        .withColumn("serial_number", regexp_extract(col("product_code"), "\\-([A-Z0-9]+)$",1))

      prodDF.show()

      val document=List(
        ("D001", "A quick brown fox jumps over the lazy dog", "John Doe", "2024-11-01"),
          ("D002", "The quick brown fox", "Jane Smith", "2024-11-05"),
            ("D003", "An apple a day keeps the doctor away", "Alice Johnson", "2024-11-10")
      ).toDF("doc_id", "content", "author", "publish_date")

      val documentDF=document.withColumn("words", split(col("content"), " "))
        val documentDF1=documentDF
        .withColumn("col1", col("words").getItem(0))
          .withColumn("col2", col("words").getItem(1))
        .withColumn("col3", col("words").getItem(2))
        .withColumn("col4", col("words").getItem(3))
        .withColumn("col5", col("words").getItem(4))
        .withColumn("col6", col("words").getItem(5))
          .withColumn("col6", col("words").getItem(6))
          .withColumn("col7", col("words").getItem(7))
          .withColumn("col8", col("words").getItem(8))
          .withColumn("col8", col("words").getItem(9))

      val documentDf2=document.withColumn("regex", regexp_extract(col("content"), "([A-Za-z\\s+]+)",1))
      documentDF1.show()
      documentDf2.show()

//Use split to extract the protocol, domain, and path from the url column.
      val url=List(
        ("S001", "https://example.com/home", "2024-07-01 10:00:00", "Chrome/90.0"),
        ("S002", "http://sample.org/contact", "2024-07-02 11:30:00", "Firefox/85.0"),
        ("S003", "https://example.com/admin", "2024-07-03 12:45:00", "Safari/14.1")
      ).toDF("session_id", "url", "timestamp", "user_agent")

      url.withColumn("protocol", regexp_extract(col("url"), "(^[A-za-z]+)",1))
        .withColumn("domain", regexp_extract(col("url"), "\\//([A-za-z.]+)",1))
        .withColumn("path", regexp_extract(col("url"), "\\/([A-za-z]+)$",1)).show()

      url.createOrReplaceTempView("url")

      spark.sql(
        """
          Select *,
          regexp_extract(url, "(^[A-za-z]+)",1) as protocol,
          regexp_extract(url, "\\//([A-za-z.]+)",1) as domain,
          regexp_extract(url, "\\/([A-za-z]+)$",1) as path
          from url
        """

      ).show()

      val address=List(
        ("A001", "123 Main St Apt 4B", "New York", "NY", "10001"),
        ("A002", "456 Elm St", "San Francisco", "CA", "94102"),
        ("A003", "789 Oak St Apt 12C", "Chicago", "IL", "60603")
      ).toDF("address_id", "full_address", "city", "state", "zipcode")

//      Use regex_extract to extract the street number, street name, and apartment number from
      //full_address.
      address.withColumn("street_number", regexp_extract(col("full_address"), "^([0-9]+)",1))
        .withColumn("street_name", regexp_extract(col("full_address"), "([A-Za-z\\s]+)",1))
      .withColumn("apartment_num", regexp_extract(col("full_address"), "([0-9A-Za-z]+)$",1)).show()

      val invoice=List(
        ("I001", "T-shirt Red Large", "10", "20", "200"),
      ("I002", "Jeans Blue Medium", "3", "50", "150"),
      ("I003", "Jacket Black Small", "5", "100", "500")
      ).toDF("invoice_id", "description", "quantity", "unit_price", "total_amount")

//Split the description into product_name, color, and size using split.
      invoice.withColumn("product_array", split(col("description")," "))
        .withColumn("product_name", col("product_array").getItem(0))
        .withColumn("color", col("product_array").getItem(1))
        .withColumn("size", col("product_array").getItem(2)).show()

      val order=List(
        ("ORD001", "P123-4567", "2024-08-01", "Delivered"),
      ("ORD002", "P234-5678", "2024-08-05","Pending"),
      ("ORD003", "P345-6789", "2024-08-10", "Delivered")
      ).toDF("order_id", "product_code", "order_date", "delivery_status")
//   Use substr to extract the first 3 characters of order_id and the last 4 characters of product_code
      order
      order.withColumn("orderid",
        concat(substring(col("order_id"),1,3),regexp_extract(col("product_code"),"([0-9]+)$",1))).show()

      val web_sessions=List(
        ("S001", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)Chrome/91.0.4472.124", "2024-12-01 08:00:00", "USA"),
      ("S002", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Firefox/89.0", "2024-12-02 09:15:00","UK"),
      ("S003", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/89.0.4389.114","2024-12-03 10:30:00", "CANADA")
      ).toDF("session_id", "user_agent", "login_time", "location")

//  Use regex_extract to parse the browser name and version from the user_agent column.
      web_sessions.withColumn("browser_name", regexp_extract(col("user_agent"),"\\)([A-Za-z\\s]+)",1))
        .withColumn("version", regexp_extract(col("user_agent"),"([0-9.]+)$",1)).show()

      val query_param=List(
        ("C001", "https://example.com/page?id=123&source=google", "2024-12-05 11:45:00", "GOOGLE"),
        ("C002", "https://sample.org/info?id=456&source=bing", "2024-12-06 12:00:00", "BING"),
        ("C003", "https://example.com/search?id=789&source=google", "2024-12-07 13:30:00", "YAHOO")
      ).toDF("click_id", "url", "click_time", "referrer")
//      Use regex_extract to extract the query parameters (e.g., ?id=123&source=google) from the
      //url column.
      query_param.withColumn("query_params",regexp_extract(col("url"), "\\?([0-9a-z=&]+)",1)).show()

      val file=List(
        ("F001", "/docs/report1.pdf", "2024-12-10", "50"),
      ("F002", "/images/picture1.jpg", "2024-12-11", "150"),
      ("F003", "/docs/manual.docx", "2024-12-12", "75")
      ).toDF("file_id", "file_path", "upload_date", "size_in_mb")

//Use split to extract the file name and extension from the file_path.
      file.withColumn("file_name", regexp_extract(col("file_path"),"([a-z0-9/]+)",1))
        .withColumn("file_extension", regexp_extract(col("file_path"),"\\.([a-z]+)",1)).show()

      val review=List(
        ("R001", "P001", "This product is excellent", "5", "2024-12-15"),
      ("R002", "P002", "The quality is poor", "2", "2024-12-16"),
        ("R003", "P003", "Excellent build quality", "4", "2024-12-17")
      ).toDF("review_id", "product_id", "review_text", "rating", "review_date")

//      Use regex_extract to identify and extract any sentiment keywords (e.g., "excellent", "poor")
      //from the review_text.
      review.withColumn("positive-keyword",regexp_extract(col("review_text"),"(excellent|good|happy|love|Excellent)",1))
        .withColumn("negative-keyword",regexp_extract(col("review_text"),"(poor|bad|sad|worst)",1)).show()

      val hashtag=List(
        ("P001", "Loving this new product! #NewProduct #Excited", "500", "100", "2024-12-20"),
      ("P002", "Amazing experience #Travel #Adventures", "1200", "250", "2024-12-21"),
      ("P003", "Can't wait for the launch! #Upcoming", "300", "75", "2024-12-22")
      ).toDF("post_id", "content", "likes" ,"shares", "post_date")

//      Use regex_extract to extract hashtags from the content column.
      hashtag.withColumn("hash-tag",regexp_extract(col("content"),"(#[A-Za-z\\s#]+)$",1)).show()

      val prod_code=List(
        ("I001", "abc-1234-x", "50", "Warehouse", "2024-12-25"),
      ("I002", "xyz-5678-y", "5", "Store", "2024-12-26"),
      ("I003", "ABC-1234-X", "100", "Store", "2024-12-27")
      ).toDF("inventory_id", "product_code", "stock", "location", "last_updated")

//      Use regex_extract to validate that the product_code follows a specific pattern (e.g., "ABC-
//      1234-X").
      val prod=prod_code.withColumn("regex_extract", regexp_extract(col("product_code"),
        "(([A-Za-z]+)-[0-9]{4}-([A-Za-z]+))",1))
      prod.select(col("inventory_id"), col("product_code"),
        when(col("product_code")=== col("regex_extract"), lit("valid"))
          .otherwise("not valid").alias("regex_validate")).show()

      val customers=List(
        ("C001", "123 Main St.", "new york", "ny", "10001"),
        ("C002", "456 Elm Ave.", "san francisco", "ca", "94102"),
        ("C003", "789 Oak St.", "chicago", "il", "60603")
      ).toDF("customer_id", "address", "city", "state", "postal_code")

//      Use regex_replace to standardize address abbreviations (e.g., "St." to "Street", "Ave." to
      //"Avenue").
      val regp=customers.withColumn("address_abbrev",regexp_extract(col("address"), "(St|Ave)",1))
        regp.withColumn("st_name",when(col("address_abbrev") === "St", lit("Street"))
        .when(regexp_extract(col("address"), "(St|Ave)",1) === "Ave", lit("Avenue"))
          .otherwise("not match")).show()

      val orderDF=List(
        ("O001", "ORD-001", "2024-12-30", "10", "15", "150"),
      ("O002", "ORD@002", "2024-12-31", "5", "20", "100"),
      ("O003", "ORD-003", "2025-01-01", "8", "25", "200")
      ).toDF("order_id", "order_number", "order_date", "quantity", "price_per_unit", "total_price")

//      Use regex_replace to remove any special characters from order_number.
      orderDF.withColumn("ord",concat(regexp_extract(col("order_number"), "^([A-Z0-9]+)",1),
        regexp_extract(col("order_number"),"([A-Z0-9]+)$",1)))
        .withColumn("regexrep", regexp_replace(col("order_number"), "[^A-Za-z0-9]","")).show()

      val sensor=List(
        ("S001", "15", "2024-12-28 08:00:00", "C"),
        ("S002", "65", "2024-12-28 09:00:00", "F"),
        ("S003", "8", "2024-12-28 10:00:00", "celsius")
      ).toDF("sensor_id", "reading_value", "timestamp", "unit")
//      Use regex_replace to standardize units (e.g., "C" to "Celsius", "F" to "Fahrenheit").
      sensor.withColumn("standard_unit",regexp_replace(
        regexp_replace(col("unit"),"C", "Celcius"),
        "F","Farenheit")
      ).show()

//      Finding the count of orders placed by each customer and the total order amount for
      //each customer.
    val orderData = Seq(
      ("Order1", "John", 100),
      ("Order2", "Alice", 200),
      ("Order3", "Bob", 150),
      ("Order4", "Alice", 300),
      ("Order5", "Bob", 250),
      ("Order6", "John", 400)
    ).toDF("OrderID", "Customer", "Amount")

      orderData.select(col("OrderID"),col("Customer"),col("Amount")).groupBy(col("Customer"))
        .agg(count("OrderID").alias("order_count"),sum(col("Amount").alias("total_order_amount"))).show()

//        Finding the average score for each subject and the maximum score for each student.
      val scoreData = Seq(
        ("Alice", "Math", 80),
        ("Bob", "Math", 90),
        ("Alice", "Science", 70),
        ("Bob", "Science", 85),
        ("Alice", "English", 75),
        ("Bob", "English", 95)
      ).toDF("Student", "Subject", "Score")

      scoreData.groupBy(col("Student")).agg(max(col("Score")).alias("Max_Score")).show()
      val ScoreDf1=scoreData.groupBy(col("Subject")).agg(avg(col("Score")).alias("Avg_Score"))
        ScoreDf1.show()

//      5.Finding the minimum, maximum, and average temperature for each city in a weather dataset.
      val weatherData = Seq(
        ("City1", "2022-01-01", 10.0),
        ("City1", "2022-01-02", 8.5),
        ("City1", "2022-01-03", 12.3),
        ("City2", "2022-01-01", 15.2),
        ("City2", "2022-01-02", 14.1),
        ("City2", "2022-01-03", 16.8)
      ).toDF("City", "Date", "Temperature")

     weatherData.groupBy(col("City")).agg(
       max(col("Temperature").alias("max_temp")),avg(col("Temperature").alias("avg_temp"))
     ,min("Temperature").alias("min_temp")).show()
}


    catch {
      case ex: Exception => {
        println("AN Error Occured")
        println(s"Exception type: ${ex.getClass.getName}")
        println(s"Error message: ${ex.getMessage}")
        ex.printStackTrace() // prints full stack trace

      }
    }

    finally {

      println("I am in finally block")
    }


  }

}
