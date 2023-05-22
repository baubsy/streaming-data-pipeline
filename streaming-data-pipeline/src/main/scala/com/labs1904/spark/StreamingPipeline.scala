package com.labs1904.spark

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Result, Scan}
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

  case class User(mail: String, birthdate: String, name: String, sex: String, username: String)
  case class EnrichedReview(user_id: String, mail: String, marketplace: String, review_id: String, product_id: String, product_parent: String, product_title: String, product_category: String, username: String, helpful_votes: String, total_votes: String, vine: String, verified_purchase: String, review_headline: String, review_body: String, review_date: String, birthdate: String, name: String, sex: String, star_rating: String)
  case class Review(marketplace: String, customer_id: String, review_id: String, product_id: String, product_parent: String, product_title: String, product_category: String, review_date: String, helpful_votes: String, total_votes: String, vine: String, verified_purchase: String, review_headline: String, review_body: String, star_rating: String)

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
      //Raw review data from kafka topic
      val result = ds

      //Splitting the review strings into arrays and then transforming them into a review case class
      val reviewList = result.map(x=> x.split('\t'))
      val reviews = reviewList.map(x=> reviewBuilder(x))

      //Using the customer IDs from the reviews to get the user information from hbase
      val enrichedReviews = reviews.mapPartitions(partition=> {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", System.getenv("HBASE"))
        val connection = ConnectionFactory.createConnection(conf)
        val table = connection.getTable(TableName.valueOf("bswyers:users"))

        val partitionedReviews = partition.map(review => {
          val get = new Get(Bytes.toBytes(review.customer_id)).addFamily(Bytes.toBytes("f1"))
          //Code to figure out all the field types of the user data from hbase
//          val debugScan = new Scan().setOneRowLimit()
//          val scanResult = table.getScanner(debugScan)
//          val scanList = scanResult.iterator()
//          scanList.forEachRemaining(x=> logger.debug(x))
          val result = table.get(get)
          val user = userBuilder(result)
          enrichedReviewBuilder(user, review)
        }).toList.iterator

        connection.close()
        partitionedReviews
      })

      // Write output to console
//      val query = enrichedReviews.writeStream
//        .outputMode(OutputMode.Append())
//        .format("console")
//        .option("truncate", false)
//        .trigger(Trigger.ProcessingTime("5 seconds"))
//        .start()

      // Write output to HDFS
    val query = enrichedReviews.writeStream
      .outputMode(OutputMode.Append())
      .format("csv")
      .option("delimiter", "\t")
      .option("path", s"/user/${hdfsUsername}/reviews_csv2")
      .option("checkpointLocation", s"/user/${hdfsUsername}/reviews_checkpoint2")
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

  //Helper functions to build the case classes and make it more readable.
  def reviewBuilder(review: Array[String]): Review = {
    Review(
      marketplace = review(0),
      customer_id = review(1),
      review_id = review(2),
      product_id = review(3),
      product_parent = review(4),
      product_title = review(5),
      product_category = review(6),
      star_rating = review(7),
      helpful_votes = review(8),
      total_votes = review(9),
      vine = review(10),
      verified_purchase = review(11),
      review_headline = review(12),
      review_body = review(13),
      review_date = review(14)
    )
  }

  def userBuilder(result : Result): User = {
    User(
      mail = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("mail"))),
      birthdate = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("birthdate"))),
      name = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"))),
      sex = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("sex"))),
      username = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("username")))
    )
  }

  def enrichedReviewBuilder(user: User, review: Review): EnrichedReview = {
    EnrichedReview(
      marketplace = review.marketplace,
      user_id = review.customer_id,
      review_id = review.review_id,
      product_id = review.product_id,
      product_parent = review.product_parent,
      product_title = review.product_title,
      product_category = review.product_category,
      star_rating = review.star_rating,
      helpful_votes = review.helpful_votes,
      total_votes = review.total_votes,
      vine = review.vine,
      verified_purchase = review.verified_purchase,
      review_headline = review.review_headline,
      review_body = review.review_body,
      review_date = review.review_date,
      mail = user.mail,
      birthdate = user.birthdate,
      name = user.name,
      sex = user.sex,
      username = user.username
    )
  }
}
