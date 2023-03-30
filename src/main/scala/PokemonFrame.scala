package com.PokemonCase


import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
object PokemonFrame {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("PokeDataFrame")
      .getOrCreate()


    val schema = StructType(Array(StructField("id", IntegerType, nullable = false),StructField("name",StringType),
      StructField("Type 1",StringType),StructField("Type 2",StringType),StructField("Total", IntegerType),
      StructField("HP", IntegerType),StructField("Attack",IntegerType),StructField("Defense",IntegerType),
      StructField("Special Attack", IntegerType),StructField("Special Defence",IntegerType),
      StructField("Speed",IntegerType),StructField("Generation",IntegerType),StructField("Legendary",BooleanType)))

    // it will take my schema
    val data: DataFrame = spark.read.option("header", true)
      .schema(schema).csv("input/pokemon.csv")

   data.printSchema()

    val rpard = data.repartition(2)

    val water = data.where((col("Type 1")=== "Water")or (col("Type 2") === "Water")).orderBy(col("id"))
    val fire = data.where((col("Type 1")=== "Fire") or (col("Type 2")=== "Fire")).orderBy(col("id"))
// it shows all the rows

    println("water pokemons: " + water.count())
    println("fire pokemons: " + fire.count())



    // population of the type of pokemons

    val typePopulation = data.groupBy("Type 1").count()
    val type2Popu = data.groupBy("Type 2").count()



    val combinedType = typePopulation.union(type2Popu)
//    combinedType.show()

    // filtering  out the defence column

    val defenc = data.select(col("Defense"))

    val maxDefence = data.select(max("Defense")as("maxDefence"))
    maxDefence.show()

    val minDefence = data.select(min("Defense")as("minDefence"))
    minDefence.show()


    //maxDefence and minDefence are data frames
    // to get the value we need

    val maxDefenceValue = maxDefence.first().getInt(0)
    val minDefenceValue = minDefence.first().getInt(0)

    val pokemD = data.select("name").where(col("Defense" )=== maxDefenceValue)

    val pokeminD = data.select("name").where(col("Defense")=== minDefenceValue)

    println("max Defence Pokemon : " )
    pokemD.show()
    println("min Defence Pokemon :" )
    pokeminD.show()




  }

}
