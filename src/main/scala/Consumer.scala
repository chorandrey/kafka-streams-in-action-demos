package com.andy

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.regex.Pattern
import java.util.{Collections, Properties}

object Consumer {

  def kafkaProperties = {
    val properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("group.id", "simple-consumer-example");
    properties.put("auto.offset.reset", "earliest"); // or "latest"
    properties.put("enable.auto.commit", "true");
    properties.put("auto.commit.interval.ms", "3000");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties
  }

  def main(args: Array[String]): Unit = {
    val consumer = new KafkaConsumer[String, String](kafkaProperties)
    val pattern = Pattern.compile(".*")
    val rebalanceListener = new NoOpConsumerRebalanceListener()
    consumer.subscribe(pattern, rebalanceListener)

    while (true) {
      val records = consumer.poll(Duration.of(5, ChronoUnit.SECONDS))
      records.forEach { record =>
        print("[" + record.topic() + "] ")
        print("[" + record.key + "]: ")
        println(record.value())
      }
      Thread.sleep(500)
    }
  }

}
