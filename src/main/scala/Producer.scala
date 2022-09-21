package com.andy

import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.andy.Producer.kafkaProperties
import com.andy.samples.ZMartApp
import com.andy.samples.ZMartApp.{Item, Purchase}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import play.api.libs.json.Json

import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
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

      val producerRecord = new ProducerRecord[String, String]("purchaseUnsafe", iteration.toString, msg)
      producer.send(producerRecord)

      iteration = iteration + 1
    } while (msg != "exit" || msg != "q")

    Thread.sleep(2000)
    println("application completed it's execution")
  }

}

object ProducerZmart extends IOApp {
  import fs2._

  val producerResource: Resource[IO, KafkaProducer[String, String]] = Resource.make(IO(new KafkaProducer[String, String](kafkaProperties)))(producer => IO(producer.close()))
  override def run(args: List[String]): IO[ExitCode] = {

    val stream = for {
      producer <- Stream.resource(producerResource)
      n <- Stream.iterate[IO, Int](0)(n => n + 1)
      purchase = Purchase(
        creditCard = "0000-0000-0000-0000-123" + n,
        customerId = n.toString,
        itemQty = Map(Item("item-id-" + n, 50.0 + n.toDouble * 0.5) -> n),
        zipCode = "55202"
      )
      purchaseStr = Json.toJson(purchase).toString()
      producerRecord = new ProducerRecord[String, String]("purchaseUnsafe", n.toString, purchaseStr)
      _ = producer.send(producerRecord)
      _ = println(purchaseStr)
      _ <- Stream.sleep[IO](FiniteDuration(30, TimeUnit.SECONDS))
    } yield ()

    stream.compile.drain.as(ExitCode.Success)
  }
}
