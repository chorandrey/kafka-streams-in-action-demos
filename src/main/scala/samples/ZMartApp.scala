package com.andy
package samples

import ProducerZmart.{Cafe, Electronix}
import model._

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}

import java.util.Properties

object ZMartApp {

  val properties: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "zmart-sample-1")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    p.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler")
    p
  }

  object TopicNames {
    val purchaseUnsafe = "purchaseUnsafe"
    val purchase = "purchase"
    val rewards = "rewards"
    val patterns = "patterns"
    val cafe = "cafe"
    val electronics = "electronics"
  }

  val exceptionHander: StreamsUncaughtExceptionHandler = new StreamsUncaughtExceptionHandler {
    override def handle(exception: Throwable): StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse = {
      println(exception.toString)
      StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION
    }
  }

  def main(args: Array[String]): Unit = {
    val stringSerde = Serdes.String()

    val streamBuilder = new StreamsBuilder()
    val purchaseUnsafeInputStream: KStream[String, Purchase] = streamBuilder.stream(TopicNames.purchaseUnsafe, Consumed.`with`(stringSerde, Purchase.purchaseSerde))

    val printAction: ForeachAction[String, Purchase] = (key, purchase) => { println("received by streams app" + purchase.toString) }
    purchaseUnsafeInputStream.peek(printAction)

    val purchaseInputStream: KStream[String, Purchase] = purchaseUnsafeInputStream.mapValues { purchase => purchase.maskCardNumber }

    val cafePredicate: Predicate[String, Purchase] = (k, p) => p.department == Cafe.toString
    val electronicsPredicate: Predicate[String, Purchase] = (k, p) => p.department == Electronix.toString

    val (cafeBranchName, electronicsBranchName) = ("cafe", "electronics")
    val branchedResult = purchaseInputStream.split(Named.as("branch-"))
      .branch(cafePredicate, Branched.as(cafeBranchName))
      .branch(electronicsPredicate, Branched.as(electronicsBranchName))
      .defaultBranch()
    val cafeStream = branchedResult.get("branch-" + cafeBranchName)
    val electronicsStream = branchedResult.get("branch-" + electronicsBranchName)

    cafeStream.to(TopicNames.cafe, Produced.`with`(stringSerde, Purchase.purchaseSerde))
    electronicsStream.to(TopicNames.electronics, Produced.`with`(stringSerde, Purchase.purchaseSerde))

    val filterPurchaseLowPrice: Predicate[String, Purchase] = (k, purchase) => { purchase.quantity * purchase.price > 15.0 }
    purchaseInputStream
      .filter(filterPurchaseLowPrice)
      .to(TopicNames.purchase, Produced.`with`(stringSerde, Purchase.purchaseSerde))

    val rewardsStream: KStream[String, Reward] = purchaseInputStream.mapValues { purchase => Reward(purchase) }
    rewardsStream.to(TopicNames.rewards, Produced.`with`(stringSerde, Reward.rewardSerde))

    val purchasePatternsStream = purchaseInputStream.mapValues { purchase => PurchasePattern(purchase) }
    purchasePatternsStream.to(TopicNames.patterns, Produced.`with`(stringSerde, PurchasePattern.purchasePatternSerde))

    val resultTopology = streamBuilder.build()

    val streamsApp = new KafkaStreams(resultTopology, properties)
    streamsApp.setUncaughtExceptionHandler(exceptionHander)
    streamsApp.start()
    shutdownAfterAWhile(streamsApp)
  }

  def shutdownAfterAWhile(kafkaStreams: KafkaStreams): Unit = {
    val sideThread: Runnable = () => {
      Thread.sleep(600000)
      kafkaStreams.close()
      System.exit(0)
    }
    val th = new Thread(sideThread)
    th.start()
  }
}
