package com.andy

import org.apache.kafka.common.serialization.Serdes.WrapperSerde
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import play.api.libs.json.{Format, JsArray, JsError, JsNumber, JsObject, JsResult, JsString, JsSuccess, JsValue, Json, OFormat}

import java.nio.charset.StandardCharsets
import java.time.temporal.{ChronoUnit, TemporalField, TemporalUnit}
import java.time.{Duration, Instant}
import java.util.Date
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

package object model {

  def genSerde[T : OFormat]: Serde[T] = {
    val purcaseSerializer = new Serializer[T] {
      override def serialize(topic: String, data: T): Array[Byte] = {
        Json.toJson(data).toString().getBytes(StandardCharsets.UTF_8)
      }
    }
    val purchaseDeserializer = new Deserializer[T] {
      override def deserialize(topic: String, data: Array[Byte]): T = {
        try {
          Json.parse(data).as[T]
        } catch {
          case ex: Exception =>
            println("Exception on deserialization")
            println(ex.toString)
            throw ex
        }
      }
    }
    new WrapperSerde[T](purcaseSerializer, purchaseDeserializer)
  }

  case class Purchase(
    creditCardNumber: String,
    customerId: String,
    itemPurchased: String,
    quantity: Int,
    price: Double,
    purchaseTime: Instant,
    zipCode: String,
    department: String,
    storeId: String,
    employeeId: Option[String]
  ) {
    def maskCardNumber: Purchase = {
      val newCreditCard = creditCardNumber.foldLeft((0, "")) { case ((count, acum), currentCh) =>
        if (count >= 16) (count + 1, acum + currentCh)
        else (count + 1, acum + '*')
      }._2
      this.copy(creditCardNumber = newCreditCard)
    }
  }

  object Purchase {
    implicit val purchaseFormat: OFormat[Purchase] = Json.format[Purchase]
    val purchaseSerde: Serde[Purchase] = genSerde[Purchase]
  }

  case class PurchasePattern(zipCode: String, item: String, purchaseTime: Instant, amount: Int)
  object PurchasePattern {
    implicit val purchasePatternFormat: OFormat[PurchasePattern] = Json.format[PurchasePattern]
    def apply(purchase: Purchase): PurchasePattern = PurchasePattern(
      purchase.zipCode,
      purchase.itemPurchased,
      purchase.purchaseTime,
      purchase.quantity
    )

    val purchasePatternSerde: Serde[PurchasePattern] = genSerde[PurchasePattern]
  }

  case class Reward(customerId: String, amountSpend: Double)
  object Reward {
    def apply(purchase: Purchase): Reward = Reward(purchase.customerId, purchase.price * purchase.quantity.toDouble)
    implicit val rewardFormat: OFormat[Reward] = Json.format[Reward]
    val rewardSerde: Serde[Reward] = genSerde[Reward]
  }
  case class RewardAccumulator(
    customerId: String,
    purchaseTotal: Double,
    totalRewardPoints: Int,
    currentRewardPoints: Int,
    mostRecentOperationTimestamp: Instant,
    daysSinceLastPurchase: Option[Long],
  ) {
    def daysSincePurchase(atTime: Instant): Long = {
      Duration.between(mostRecentOperationTimestamp, atTime).toHours / 24
    }
  }
  object RewardAccumulator {
    implicit val rewardAccumulatorFormat: OFormat[RewardAccumulator] = Json.format[RewardAccumulator]
    val rewardAccumulatorSerde: Serde[RewardAccumulator] = genSerde[RewardAccumulator]
  }

}
