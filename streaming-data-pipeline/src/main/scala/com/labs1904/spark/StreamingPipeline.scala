package com.labs1904.spark

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}


/**
 * Spark Structured Streaming app
 *
 */
object StreamingPipeline {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "StreamingPipeline"

  val hdfsUrl = "CHANGEME"
  val bootstrapServers = System.getenv("BOOTSTRAP")
  val username = System.getenv("USERNAME")
  val password = System.getenv("PASSWORD")
  val hdfsUsername = System.getenv("HANDLE")

  //Use this for Windows
  //val trustStore: String = "src\\main\\resources\\kafka.client.truststore.jks"
  //Use this for Mac
  //val trustStore: String = "src/main/resources/kafka.client.truststore.jks"
  val trustStore: String = System.getenv("TRUSTSTORE")
  case class EnrichedReview(user_id: String, mail: String, marketplace: String, review_id: String, product_id: String, product_parent: String, product_title: String, product_category: String, star_rating: String, helpful_votes: String, total_votes: String, vine: String, verified_purchase: String, review_headline: String, review_body: String, review_date: String)
  case class Review(marketplace: String, customer_id: String, review_id: String, product_id: String, product_parent: String, product_title: String, product_category: String, star_rating: String, helpful_votes: String, total_votes: String, vine: String, verified_purchase: String, review_headline: String, review_body: String, review_date: String)

  def main(args: Array[String]): Unit = {

    try {
      val spark = SparkSession.builder()
        .config("spark.sql.shuffle.partitions", "3")
        .appName(jobName)
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val ds = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", "reviews")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "20")
        .option("startingOffsets","earliest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option("kafka.ssl.truststore.location", trustStore)
        .option("kafka.sasl.jaas.config", getScramAuthString(username, password))
        .load()
        .selectExpr("CAST(value AS STRING)").as[String]

      // TODO: implement logic here
      val result = ds
      //result.printSchema()


      val results = result.flatMap(x=> x.split("\t"))
      val reviews = results.map(x=> Review(x(0).toString, x(1).toString, x(2).toString,x(3).toString,x(4).toString,x(5).toString,x(6).toString,x(7).toString,x(8).toString,x(9).toString,x(10).toString,x(11).toString,x(12).toString,x(13).toString,x(14).toString))
      //figure out why mapPartitions is overloaded, probably returning wrong
      val mappedReviews = reviews.mapPartitions(partition=> {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", System.getenv("HBASE"))
        val connection = ConnectionFactory.createConnection(conf)
        val table = connection.getTable(TableName.valueOf("bswyers:users"))

        val iter = partition.map(review => {
          val get = new Get(Bytes.toBytes(review.customer_id)).addFamily(Bytes.toBytes("f1"))
          val result = table.get(get)
          EnrichedReview(review.customer_id, Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("mail"))), review.marketplace, review.review_id, review.product_id, review.product_parent, review.product_title, review.product_category, review.star_rating, review.helpful_votes, review.total_votes, review.vine, review.verified_purchase, review.review_headline, review.review_body, review.review_date)
        }).toList.iterator

        connection.close()
        iter
      })
      // Write output to console
      val query = mappedReviews.writeStream
        .outputMode(OutputMode.Append())
        .format("console")
        .option("truncate", false)
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start()
      //output should be case class, not tuple
      // Write output to HDFS
//      val query = result.writeStream
//        .outputMode(OutputMode.Append())
//        .format("json")
//        .option("path", s"/user/${hdfsUsername}/reviews_json")
//        .option("checkpointLocation", s"/user/${hdfsUsername}/reviews_checkpoint")
//        .trigger(Trigger.ProcessingTime("5 seconds"))
//        .start()
      query.awaitTermination()
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  def getScramAuthString(username: String, password: String) = {
    s"""org.apache.kafka.common.security.scram.ScramLoginModule required
   username=\"$username\"
   password=\"$password\";"""
  }
//  def stringConverter(review: Dataset[String]) : Review = {
//    //Review(review(0).toString, review(0).toString, review(0).toString, review(0).toString, review(0).toString, review(0).toString, review(0).toString, review(0).toString, review(0).toString, review(0).toString, review(0).toString, review(0).toString, review(0).toString, review(0).toString, review(0).toString)
//  }
}
