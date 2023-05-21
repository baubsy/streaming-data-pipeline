package com.labs1904.spark

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Scan}
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

  val hdfsUrl = System.getenv("HDFSURL")
  val bootstrapServers = System.getenv("BOOTSTRAP")
  val username = System.getenv("USERNAME")
  val password = System.getenv("PASSWORD")
  val hdfsUsername = System.getenv("HANDLE")

  //Use this for Windows
  //val trustStore: String = "src\\main\\resources\\kafka.client.truststore.jks"
  //Use this for Mac
  //val trustStore: String = "src/main/resources/kafka.client.truststore.jks"
  val trustStore: String = System.getenv("TRUSTSTORE")

  case class User(user_id: String, mail: String, birthdate: String, name: String, sex: String, username: String)
  case class EnrichedReview(user_id: String, mail: String, marketplace: String, review_id: String, product_id: String, product_parent: String, product_title: String, product_category: String, username: String, helpful_votes: String, total_votes: String, vine: String, verified_purchase: String, review_headline: String, review_body: String, review_date: String, birthdate: String, name: String, sex: String, star_rating: String)
  case class Review(marketplace: String, customer_id: String, review_id: String, product_id: String, product_parent: String, product_title: String, product_category: String, star_rating: String, helpful_votes: String, total_votes: String, vine: String, verified_purchase: String, review_headline: String, review_body: String, review_date: String)

  def main(args: Array[String]): Unit = {

    try {
      //for writing to console
//      val spark = SparkSession.builder()
//        .config("spark.sql.shuffle.partitions", "3")
//        .appName(jobName)
//        .master("local[*]")
//        .getOrCreate()
      //to write to HDFS
    val spark = SparkSession.builder()
      .config("spark.sql.shuffle.partitions", "3")
      .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
      .config("spark.hadoop.fs.defaultFS", hdfsUrl)
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


      //val results = result.flatMap(x=> x.split("\n"))
      val results = result.map(x => x.split('\n'))
      //val reviewList = results.flatMap(x=> x.split("\t"))
      val reviewList = results.map(x => x(0).split('\t'))
      //Not Splitting results into reviews perfectly?
      val reviews = reviewList.map(x=> Review(x(0), x(1), x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14)))
      val mappedReviews = reviews.mapPartitions(partition=> {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", System.getenv("HBASE"))
        val connection = ConnectionFactory.createConnection(conf)
        val table = connection.getTable(TableName.valueOf("bswyers:users"))

        val iter = partition.map(review => {
          val get = new Get(Bytes.toBytes(review.customer_id)).addFamily(Bytes.toBytes("f1"))
//          val debugScan = new Scan().setOneRowLimit()
//          val scanResult = table.getScanner(debugScan)
//          val scanList = scanResult.iterator()
//          scanList.forEachRemaining(x=> logger.debug(x))
          val result = table.get(get)
          //logger.debug(Bytes.toString(result.value))
          EnrichedReview(review.customer_id, Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("mail"))), review.marketplace, review.review_id, review.product_id, review.product_parent, review.product_title, review.product_category,Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("username"))), review.helpful_votes, review.total_votes, review.vine, review.verified_purchase, review.review_headline, review.review_body, review.review_date, Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("birthdate"))), Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"))), Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("sex"))), review.star_rating)
        }).toList.iterator

        connection.close()
        iter
      })

      //mappedReviews.printSchema()
      // Write output to console
//      val query = mappedReviews.writeStream
//        .outputMode(OutputMode.Append())
//        .format("console")
//        .option("truncate", false)
//        .trigger(Trigger.ProcessingTime("5 seconds"))
//        .start()
      //output should be case class, not tuple
      // Write output to HDFS

    val query = mappedReviews.writeStream
      .outputMode(OutputMode.Append())
      .format("csv")
      .option("delimiter", "\t")
      .option("path", s"/user/${hdfsUsername}/reviews_csv")
      .option("checkpointLocation", s"/user/${hdfsUsername}/reviews_checkpoint")
      .partitionBy("star_rating")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
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

}
