package com.andy
package samples

import org.apache.kafka.common.serialization.Serdes.WrapperSerde
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG
import org.apache.kafka.streams.errors.{LogAndFailExceptionHandler, StreamsUncaughtExceptionHandler}
import org.apache.kafka.streams.kstream.{Consumed, ForeachAction, KStream, Produced}
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import play.api.libs.json.{Format, JsArray, JsError, JsNumber, JsObject, JsResult, JsString, JsSuccess, JsValue, Json, OFormat}

import java.nio.charset.StandardCharsets
import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

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
  }

  case class Item(id: String, price: Double)
  object Item {
    implicit val itemFormat: OFormat[Item] = Json.format[Item]
    implicit val mapFormat: Format[Map[Item, Int]] = new Format[Map[Item, Int]] {
      override def writes(o: Map[Item, Int]): JsValue = JsArray(
        o.map { case (item, qty) => JsObject(List(
          "item" -> itemFormat.writes(item),
          "qty" -> JsNumber(qty)
        ))}.toList
      )

      override def reads(json: JsValue): JsResult[Map[Item, Int]] = {
        json.validate[JsArray].flatMap { jsArray =>

          val listBufJsResult = jsArray.value.foldLeft(ListBuffer[JsResult[(Item, Int)]]()){ case (acum, currentObj) =>
            val currentRes = currentObj.validate[JsObject].flatMap { jsObj =>
              Try {
                for {
                  item <- jsObj.value("item").validate[Item]
                  qty <- jsObj.value("qty").validate[Int]
                } yield (item -> qty)
              } match {
                case Success(value) => JsSuccess(value)
                case Failure(exception) => JsError(exception.getMessage)
              }
            }.flatMap(identity)
            acum.appended(currentRes)
          }

          listBufJsResult.find(_.isError) match {
            case Some(err) => JsError()
            case None => JsSuccess(listBufJsResult.map(_.get).toMap)
          }
        }
      }
    }
  }
  case class Purchase(creditCard: String, customerId: String, itemQty: Map[Item, Int], zipCode: String)
  object Purchase {
    import Item.itemFormat
    implicit val purchaseFormat: OFormat[Purchase] = Json.format[Purchase]
    val purchaseSerde: Serde[Purchase] = {
      val purcaseSerializer = new Serializer[Purchase] {
        override def serialize(topic: String, data: Purchase): Array[Byte] = {
          Json.toJson(data).toString().getBytes(StandardCharsets.UTF_8)
        }
      }
      val purchaseDeserializer = new Deserializer[Purchase] {
        override def deserialize(topic: String, data: Array[Byte]): Purchase = {
          try {
            Json.parse(data).as[Purchase]
          } catch {
            case ex: Exception =>
              println("Exception on deserialization")
              println(ex.toString)
              throw ex
          }
        }
      }
      new WrapperSerde[Purchase](purcaseSerializer, purchaseDeserializer)
    }
  }
  case class PurchasePattern(zipCode: String, items: Map[Item, Int])
  object PurchasePattern {
    implicit val purchasePatternFormat: OFormat[PurchasePattern] = Json.format[PurchasePattern]
    def apply(purchase: Purchase): PurchasePattern = PurchasePattern(
      purchase.zipCode,
      purchase.itemQty
    )
  }

  def maskCardNumber(in: String): String = {
    in.foldLeft((0, "")){ case ((count, acum), currentCh) =>
      if (count >= 16) (count + 1, acum + currentCh)
      else (count + 1, acum + '*')
    }._2
  }

  def main(args: Array[String]): Unit = {
    val stringSerde = Serdes.String()

    val streamBuilder = new StreamsBuilder()
    val purchaseUnsafeInputStream: KStream[String, Purchase] = streamBuilder.stream(TopicNames.purchaseUnsafe, Consumed.`with`(stringSerde, Purchase.purchaseSerde))

    val printAction: ForeachAction[String, Purchase] = (key, purchase) => { println("received by streams app" + purchase.toString) }
    purchaseUnsafeInputStream.peek(printAction)

    val purchaseInputStream: KStream[String, Purchase] = purchaseUnsafeInputStream.mapValues { purchase =>
      purchase.copy(creditCard = maskCardNumber(purchase.creditCard))
    }
    purchaseInputStream.to(TopicNames.purchase, Produced.`with`(stringSerde, Purchase.purchaseSerde))

    val rewardsStream: KStream[String, String] = purchaseInputStream.mapValues { purchase =>
      val customerId = purchase.customerId
      val amountSpend = purchase.itemQty.map { case (item, qty) => item.price * qty }.sum
      val jsRes = JsObject(List(
        "customerId" -> JsString(customerId),
        "amountSpend" -> JsNumber(amountSpend)
      ))
      Json.stringify(jsRes)
    }
    rewardsStream.to(TopicNames.rewards, Produced.`with`(stringSerde, stringSerde))

    val purchasePatternsStream = purchaseInputStream.mapValues { purchase =>
      Json.stringify {
        val pp = PurchasePattern(purchase)
        PurchasePattern.purchasePatternFormat.writes(pp)
      }
    }
    purchasePatternsStream.to(TopicNames.patterns, Produced.`with`(stringSerde, stringSerde))

    val resultTopology = streamBuilder.build()

    val exceptionHander: StreamsUncaughtExceptionHandler = new StreamsUncaughtExceptionHandler {
      override def handle(exception: Throwable): StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse = {
        println(exception.toString)
        StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION
      }
    }

    val streamsApp = new KafkaStreams(resultTopology, properties)
    streamsApp.setUncaughtExceptionHandler(exceptionHander)
    streamsApp.start()

    val sideThread: Runnable = () => {
      Thread.sleep(600000)
      streamsApp.close()
      System.exit(0)
    }
    val th = new Thread(sideThread)
    th.start()
  }
}
