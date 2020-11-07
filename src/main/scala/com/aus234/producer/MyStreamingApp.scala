package com.aus234.producer

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Column


object MyStreamingApp {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "MyStreamingApp"

  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder().appName(jobName).master("local[*]").getOrCreate()
      // TODO: change bootstrap servers to your kafka brokers
      val bootstrapServers = "localhost:9092"
      val df = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", "creditcardTransaction")
        .option("startingOffsets", "latest")
        .load()


      val schema = new StructType()
        .add("id",IntegerType)
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType)
        .add("dob_year",IntegerType)
        .add("dob_month",IntegerType)
        .add("gender",StringType)
        .add("salary",IntegerType)
      val personStringDF = df.selectExpr("CAST(value AS STRING)")
      val personDF = personStringDF.select(from_json(col("value"), schema).as("data"))
        .select("data.*")

      personDF.writeStream
        .format("console")
        .outputMode("append")
        .trigger(Trigger.ProcessingTime("2 minutes"))
        .start()
        .awaitTermination()

    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  def compute(df: DataFrame): DataFrame = {
    df
  }
}
