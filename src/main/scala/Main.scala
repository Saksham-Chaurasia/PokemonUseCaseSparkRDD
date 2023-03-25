package com.PokemonCase

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder()
      .master("local[1]")
      .appName("PokemonV")
      .getOrCreate()


    val sc = spark.sparkContext

    val file = sc.textFile("input/pokemon.csv",4)
//     for(num<-file)
//       println(num)

    val schema = file.first()
//    println(schema)

    //removing schema
    val datafile = file.filter( row=> row!=schema)

//      datafile.foreach(println)

    println(datafile.getNumPartitions)
    //finding the number of water and fire pokemons

    val data = datafile.map(_.split(","))
    println("................")
//    data.foreach(line =>{
//      for(arr<-line) {
//        print(arr + " ")
//
//      }
//      println()
//    })

    val types = data.map(col=>(col(2),col(3)))

    val waterType = types.filter(v=>(v._1.matches("Water") || v._2.matches("Water")))

    val FireType = types.filter(v=> (v._1.matches("Fire") || v._2.matches("Fire")))

    val countWater = waterType.count()
    val countFire = FireType.count()

    println(s"Water $countWater")
    println(s"Fire $countFire")

    // Population of all the pokemons

    val dType1 = types.map(v =>(v._1,1))
    val dType2 = types.map(v=>(v._2,1))
    val d1 = dType1.reduceByKey(_+_).sortByKey()
    val d2 = dType2.reduceByKey(_+_).sortByKey()
    d1.collect().foreach(println)
    d2.collect().foreach(println)

    val d = d1.union(d2).reduceByKey(_+_).sortByKey()
    d.collect().foreach(println)

    //filtering out the defence column

    val defence = data.map(_(7).toInt)
//    defence.foreach(println)
    val value:String = defence.max().toString
    println("maximum defence: " + value)

    //finding the pokemon who has the maximum defence

    val poke = data.map(v => (v(1),v(7))).filter(_._2.matches(value))

    poke.foreach(println)

    // least defence
    val lValue:String = defence.min().toString
    println("minimum defence: " + lValue)

    //finding the pokemon who has the minimum defence

    val poke2 =  data.map(v => (v(1),v(7))).filter(_._2.matches(lValue))

    poke2.foreach(println)

  }
}