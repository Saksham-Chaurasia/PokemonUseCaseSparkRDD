package com.PokemonCase

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}

import java.util.logging.Logger

object StreamingPractice {

    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("practicing")
      .getOrCreate()

    val schema = StructType(Array(StructField("id", IntegerType, nullable = false), StructField("name", StringType),
      StructField("Type1", StringType), StructField("Type2", StringType), StructField("Total", IntegerType),
      StructField("HP", IntegerType), StructField("Attack", IntegerType), StructField("Defense", IntegerType),
      StructField("Special Attack", IntegerType), StructField("Special Defence", IntegerType),
      StructField("Speed", IntegerType), StructField("Generation", IntegerType), StructField("Legendary", BooleanType)))

    val pokeDf = spark.read
      .option("header",true)
      .schema(schema)
      .csv("input/pokemon.csv")



      pokeDf.createOrReplaceTempView("poke")

    val countwater = spark.sql("select count(*) as water from poke where Type1 == 'Water' or Type2 =='Water'  " )

    
    countwater.show()

  }


}
