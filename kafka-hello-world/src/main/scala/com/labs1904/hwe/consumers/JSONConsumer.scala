package com.labs1904.hwe.consumers

import com.labs1904.hwe.util.Constants._
import com.labs1904.hwe.util.Util
import net.liftweb.json.DefaultFormats
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util

object JSONConsumer {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit val formats: DefaultFormats.type = DefaultFormats

  def main(args: Array[String]): Unit = {
    // Create the KafkaConsumer
    val properties = Util.getConsumerProperties(BOOTSTRAP_SERVER)
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](properties)

    // Subscribe to the topic
    consumer.subscribe(util.Arrays.asList(DEFAULT_TOPIC))

    while ( {
      true
    }) {
      // poll for new data
      val duration: Duration = Duration.ofMillis(100)
      val records: ConsumerRecords[String, String] = consumer.poll(duration)

      records.forEach((record: ConsumerRecord[String, String]) => {
        /*
          // DEBUG Info
          logger.info(s"Key: ${record.key}. Value: ${record.value}")
          logger.info(s"Partition: ${record.partition}, Offset ${record.offset}")
        */

        /*
          1. Retrieve and print the message from each record
          2. Define a case class,  at the top of this file, that matches the message structure ( https://docs.scala-lang.org/tour/case-classes.html )
          3. Parse the JSON string into a scala case class ( https://alvinalexander.com/scala/simple-scala-lift-json-example-lift-framework/ )
         */

        val message = record.value()
        logger.info(s"Message Received: $message")
      })
    }
  }
}
