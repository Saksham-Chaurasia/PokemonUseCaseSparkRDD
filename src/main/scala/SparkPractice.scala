package com.PokemonCase

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

case class schema2 (First_name : String, Last_name : String, Age : Int, Profile: String)
object SparkPractice {

  def main(args: Array[String]): Unit = {
  val spark = SparkSession.builder()
    .appName("DataFramePractice")
    .master("local[1]")
    .getOrCreate()

    // one way

    val data = Seq(("Saksham","Chaurasia",23,"Data Engineer"),("Sujith", "Inkulu",22, "Machine Learning"),
      ("Raja", "Deepak",21,"DataEngineer"))

    val schema = Seq("First name", "Second name", "Age", "Profile")

    import spark.sqlContext.implicits._

    val sc = spark.sparkContext

    val df= sc.parallelize(data).toDF(schema:_*)

    df.show()

    // second way while defining data type of the schema also

//    case class schema2 (First_name : String, Last_name : String, Age : Int, Profile: String)
//    import spark.sqlContext.implicits._
//
   val data2 = Seq(schema2("Saksham","Chaurasia",23,"Data Engineer"),
  schema2("Sujith", "Inkulu",22, "Machine Learning"),
      schema2("Raja", "Deepak",21,"DataEngineer"))

    val df2 = sc.parallelize(data2).toDF()
//      val df2 = spark.createDataFrame(data2)
    df2.printSchema()
   df2.show()

    // third way using struct field

     val schema3 = StructType(Array(StructField("FirstName", StringType),StructField("LastName",StringType),
       StructField("Age",IntegerType),StructField("Profile",StringType)))

      val data3 = Seq(Row("Saksham", "Chaurasia", 23, "Data Engineer"),
        Row("Sujith", "Inkulu", 22, "Machine Learning"),
        Row("Raja", "Deepak", 21, "DataEngineer"))

   val rdd = sc.parallelize(data3)

    val df3 = spark.createDataFrame(rdd,schema3)
    df3.show()




    // reading data different formats

//     val dataF:DataFrame = spark.read.csv("input/pokemon.csv") // txt, json, parquet

//    val dataF:DataFrame = spark.read.option("header",true).option("infoSchema",true).csv("input/pokemon.csv")
// read method  is define dataframe read method, which is the instance  of the Spark Session
//    val dataF:DataFrame = spark.read.format("csv")
//      .option("header",true)
//      .option("infoSchema", true)
//      .load("input/pokemon.csv")

//    reading Streaming data this method define in dataframe stream reader,  dataframe stream writer
    // dataframe read method is different from sream reader

    val dataF:DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    dataF.show(50,false )








// dataframe Datastreamreader is provided by Structured Streaming api

// read is provided by DataFrameReader api

  }

}
