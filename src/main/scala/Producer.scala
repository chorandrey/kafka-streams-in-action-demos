package com.andy

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

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

    val producerRecord = new ProducerRecord[String, String]("topic1", "Key", "Hello Andy!")
    producer.send(producerRecord)

    println("application completed it's execution")
  }

}
