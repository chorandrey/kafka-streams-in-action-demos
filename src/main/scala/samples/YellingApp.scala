package com.andy
package samples

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}

import java.util.Properties


object YellingApp {

  def streamsProperties: Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling-app")
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    properties
  }

  def main(args: Array[String]): Unit = {
    val stringSerde = Serdes.String()

    val streamsBuilder = new StreamsBuilder()
    val inStream = streamsBuilder.stream[String, String]("src-topic", Consumed.`with`(stringSerde, stringSerde))

    val upperCasedStream = inStream.mapValues(elem => elem.toUpperCase)
    upperCasedStream.to("dst-topic")

    val kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsProperties)
    kafkaStreams.start()
  }
}
