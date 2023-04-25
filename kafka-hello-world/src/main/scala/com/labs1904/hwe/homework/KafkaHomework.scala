package com.labs1904.hwe.homework

import java.time.Duration
import java.util.{Arrays, Properties, UUID}
import net.liftweb.json.DefaultFormats
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import com.labs1904.hwe.util.Constants._
import org.slf4j.LoggerFactory

object KafkaHomework {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Your task is to try to understand this code and run the consumer successfully. Follow each step below for completion.
   * Implement all the todos below
   */

    //TODO: If these are given in class, change them so that you can run a test. If not, don't worry about this step
  val Topic: String = "connection-test"

  implicit val formats: DefaultFormats.type = DefaultFormats

  def main(args: Array[String]): Unit = {

    // Create the KafkaConsumer
    //TODO: Write in a comment what these lines are doing. What are the properties necessary to instantiate a consumer?

    //gets information about the kafka server
    val properties = getProperties(BOOTSTRAP_SERVER)
    //initializing a kafka consume withe the information we just got
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](properties)


    //TODO: What does this line mean? Write your answer in a comment below
    //telling the consumer what topics to read from the kafka cluster
    consumer.subscribe(Arrays.asList(Topic))

    while (true) {
      // TODO: Change this to be every 5 seconds
      val duration: Duration = Duration.ofMillis(5000)

      //TODO: Look up the ConsumerRecords class below, in your own words what is the class designed to do?
      //hold the data for each partition of a topic
      val records: ConsumerRecords[String, String] = consumer.poll(duration)

      records.forEach((record: ConsumerRecord[String, String]) => {
        // Retrieve the message from each record
        //TODO: Describe why we need the .value() at the end of record
        //ConsumerRecord also contains metadata, .value pulls the actual value
        val message = record.value()

        //TODO: If you were given the values for the bootstrap servers in class, run the app with the green play button and make sure it runs successfully. You should see message(s) printing out to the screen
        logger.info(s"Message Received: $message")
      })
    }
  }

  def getProperties(bootstrapServer: String): Properties = {
    // Set Properties to be used for Kafka Consumer
    val properties = new Properties
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString)

    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties
  }

}
