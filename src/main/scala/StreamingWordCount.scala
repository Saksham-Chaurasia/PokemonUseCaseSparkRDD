package com.PokemonCase

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object StreamingWordCount extends Serializable {

  @transient lazy val logger :Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("WordCount")
      .config("spark.streaming.stopGracefullyOnShutdown",true)
      .config("spark.sql.shuffle.partitions",3)
      .getOrCreate()

    val linesDF = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","9999")
      .load()

    linesDF.printSchema()

    val wordsDF = linesDF.select(expr("explode(split(value,' ')) as word"))
    val countsDF = wordsDF.groupBy("word").count()

    val wordCount = countsDF.writeStream
      .format("console")
      .outputMode("complete")
      .option("checkpointLocaiton","chk-point-dir")
      .start()

    logger.info("Listening to localhost: 9999")
    wordCount.awaitTermination()



  }

}
