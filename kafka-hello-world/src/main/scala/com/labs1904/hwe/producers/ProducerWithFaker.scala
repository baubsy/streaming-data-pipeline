package com.labs1904.hwe.producers

import com.labs1904.hwe.util.Constants._
import com.labs1904.hwe.util.Util
import faker._
import net.liftweb.json.DefaultFormats
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory

case class User(name: String, username: String, email: String)

object ProducerWithFaker {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit val formats: DefaultFormats.type = DefaultFormats

  def main(args: Array[String]): Unit = {

    // Create the Kafka Producer
    val properties = Util.getProperties(BOOTSTRAP_SERVER)
    val producer = new KafkaProducer[String, String](properties)

    // create n fake records to send to topic
    val recordsToCreate = 10
    val range = (1 to recordsToCreate).toList

    range.map(id => {
      val key = id.toString

      // Use the faker library ( https://github.com/bitblitconsulting/scala-faker ) to generate Users
      // User, as a case class, is defined at the top of this file
      val name = Name.name
      val user = User(name, Internet.user_name(name), Internet.free_email(name))

      // Switching to use CSV instead of JSON
      //val jsonString = write(user)
      val csvString = key + "," + name.replace(",","") + "," + user.email.replace(",","")

      new ProducerRecord[String, String](DEFAULT_TOPIC, key, csvString)
    }).foreach(record => {

      // send records to topic
      producer.send(record, new Callback() {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
          if (e == null) {
            logger.info(
              s"""
                 |Sent Record: ${record.value()}
                 |Topic: ${recordMetadata.topic()}
                 |Partition: ${recordMetadata.partition()}
                 |Offset: ${recordMetadata.offset()}
                 |Timestamp: ${recordMetadata.timestamp()}
          """.stripMargin)
          } else logger.info("Error while producing", e)
        }
      })
    })

    producer.close()
  }


}
