package com.labs1904.hwe.homework

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}

object HelloWorldBatchApp {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "HelloWorldBatchApp"

  def main(args: Array[String]): Unit = {
    try {
      logger.info(s"$jobName starting...")
      //TODO: What is a spark session - Why do we need this line?
      //Its our way of interacting with spark in our code
      val spark = SparkSession.builder()
        .appName(jobName)
        .config("spark.sql.shuffle.partitions", "3")
        //TODO- What is local[*] doing here?
        //the master url we connect to. local has spark run locally and * has it figure out how many cores it should use
        .master("local[*]")
        //TODO- What does Get or Create do?
        //looks for a global spark session and returns it if there is one. If there isn't it makes one
        .getOrCreate()

      import spark.implicits._
      val sentences: Dataset[String] = spark.read.csv("src/main/resources/sentences.txt").as[String]
      // print out the names and types of each column in the dataset
      sentences.printSchema
      // display some data in the console, useful for debugging
      //TODO- Make sure this runs successfully
      sentences.show(truncate = false)
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }
}
