package com.labs1904.hwe.consumers

import com.labs1904.hwe.util.Constants._
import com.labs1904.hwe.util.Util
import net.liftweb.json.DefaultFormats
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.Arrays

object HweConsumer {
  private val logger = LoggerFactory.getLogger(getClass)

  val consumerTopic: String = "question-1"
  val producerTopic: String = "question-1-output"

  implicit val formats: DefaultFormats.type = DefaultFormats

  case class RawUser(userId: Int, userName: String, name: String, email: String, birthday: String)
  case class EnrichedUser(userId: Int, userName: String, name: String, email: String, birthday: String, numberAsWord: String, hweDeveloper: String)

  def main(args: Array[String]): Unit = {

    // Create the KafkaConsumer
    val consumerProperties = Util.getConsumerProperties(BOOTSTRAP_SERVER)
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumerProperties)

    // Create the KafkaProducer
    val producerProperties = Util.getProperties(BOOTSTRAP_SERVER)
    val producer = new KafkaProducer[String, String](producerProperties)

    // Subscribe to the topic
    consumer.subscribe(Arrays.asList(consumerTopic))



    while ( {
      true
    }) {
      // poll for new data
      val duration: Duration = Duration.ofMillis(100)
      val records: ConsumerRecords[String, String] = consumer.poll(duration)


      records.forEach((record: ConsumerRecord[String, String]) => {
        // Retrieve the message from each record
        val message = record.value()
        logger.info(s"Message Received: $message")
        // TODO: Add business logic here!
        val splitMessage = message.split("\\t")
        val rUser = RawUser(splitMessage(0).toInt, splitMessage(1), splitMessage(2), splitMessage(3), splitMessage(4))
        val eUser = EnrichedUser(rUser.userId, rUser.userName, rUser.name, rUser.email, rUser.birthday, Util.mapNumberToWord(rUser.userId), "Robert Swyers")
        val csvUser: String = eUser.toString.substring(13, eUser.toString.indexOf(")"))
        val pRecord = new ProducerRecord[String, String](producerTopic, csvUser)
        //logger.info(csvUser)
        producer.send(pRecord)
      })
    }
  }
}