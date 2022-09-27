package com.andy

import cats.effect.{ExitCode, IO, IOApp, Resource}
import Producer.kafkaProperties
import model.Purchase

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import play.api.libs.json.Json

import java.time.Instant
import java.util.{Date, Properties}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.io.StdIn
import scala.util.Random

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

  sealed trait Department
  object Department {
    def byIndex(i: Int): Department = i match {
      case 0 => Store
      case 1 => Electronix
      case 2 => Cafe
    }
  }
  case object Store extends Department
  case object Electronix extends Department
  case object Cafe extends Department

  val producerResource: Resource[IO, KafkaProducer[String, String]] = Resource.make(IO(new KafkaProducer[String, String](kafkaProperties)))(producer => IO(producer.close()))
  override def run(args: List[String]): IO[ExitCode] = {

    val stream = for {
      producer <- Stream.resource(producerResource)
      n <- Stream.iterate[IO, Int](0)(n => n + 1).flatMap(n => Stream.iterable(List.tabulate(5)(p => n)))
      department = Department.byIndex(Random.nextInt(3))
      zipCode = Random.nextInt(4) + 55202
      itemPurchased = Random.nextInt(20).toString
      quantity = Random.nextInt(10) + 1
      price = Random.nextInt(100).toDouble + Random.nextInt(100).toDouble / 100.0
      date = Instant.now()
      storeId = Random.nextInt(5) + 1
      employeeId = Random.nextInt(1000)
      purchase = Purchase(
        creditCardNumber = "0000-0000-0000-0" + n,
        customerId = n.toString,
        itemPurchased = itemPurchased,
        zipCode = zipCode.toString,
        quantity = quantity,
        price = price,
        purchaseTime = date,
        department = department.toString,
        storeId = storeId.toString,
        employeeId = Some(employeeId.toString)
      )
      purchaseStr = Json.toJson(purchase).toString()
      producerRecord = new ProducerRecord[String, String]("purchaseUnsafe", n.toString, purchaseStr)
      _ = producer.send(producerRecord)
      _ = println(purchaseStr)
      _ <- Stream.sleep[IO](FiniteDuration(15, TimeUnit.SECONDS))
    } yield ()

    stream.compile.drain.as(ExitCode.Success)
  }
}
