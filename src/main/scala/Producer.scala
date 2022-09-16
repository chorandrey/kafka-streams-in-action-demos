package com.andy

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.io.StdIn

object Producer {

  def kafkaProperties: Properties = {
    val properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("acks", "1");
    properties.put("retries", "3");
    properties.put("compression.type", "none");
    properties
  }

  val producer = new KafkaProducer[String, String](kafkaProperties)

  def main(args: Array[String]): Unit = {
    var msg = ""
    var iteration = 0
    do {
      msg = StdIn.readLine()

      val producerRecord = new ProducerRecord[String, String]("src-topic", iteration.toString, msg)
      producer.send(producerRecord)

      iteration = iteration + 1
    } while (msg != "exit" || msg != "q")

    Thread.sleep(2000)
    println("application completed it's execution")
  }

}
