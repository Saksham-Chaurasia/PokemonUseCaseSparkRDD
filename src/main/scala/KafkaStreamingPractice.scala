package com.PokemonCase

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_csv}
import org.apache.spark.sql.types._


object KafkaStreamingPractice extends Serializable {
  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("KafkaStream")
      .config("spark.streaming.stopGracefullyOnShutdown",true)
      .getOrCreate()

    val schema = StructType(List(
      StructField("id", StringType, nullable = false),
      StructField("name",StringType),
      StructField("type1",StringType),
      StructField("type2",StringType),
      StructField("total",IntegerType),
      StructField("hp",IntegerType),
      StructField("attack",IntegerType),
      StructField("defence",IntegerType),
      StructField("spAttack",IntegerType),
      StructField("spDefense",IntegerType),
      StructField("speed",IntegerType),
      StructField("generation",IntegerType),
      StructField("legendary",BooleanType),
    ))

    val kafkaSourceDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","pokemon")
      .option("startingOffsets","earliest")
      .load()


val valueDF = kafkaSourceDF.selectExpr("Cast(value as String)")
    .select(from_csv(col("value"),schema,Map("sep" -> ",")).alias("value"))


    valueDF.printSchema()
    val notificationDF = valueDF.select("value.id","value.name","value.type1","value.type2","value.spAttack").
      withColumn("specialAttack", expr("spAttack * 100"))



    val kafkaTargetDF = notificationDF.selectExpr("id as key",
    """to_json(named_struct('name',name,
        |'Type1',Type1,
        |'Type2',Type2,
        |'SpAttack',SpAttack
        |))as value""".stripMargin)

    val writedata = kafkaTargetDF.writeStream
      .queryName("notification writer")
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic", "poke")
      .outputMode("append")
      .option("checkpointLocation","chk-point-dir")
      .start()

//    val writedata = notificationDF.writeStream
//     .trigger(Trigger.ProcessingTime("10 seconds"))
//      .format("console")
//      .option("truncate", "false")
//      .start()


    logger.info("listening and writing to kafka")
    writedata.awaitTermination()

  }

}
