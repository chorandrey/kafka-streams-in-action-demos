package com.andy
package samples

import cats.effect.{ExitCode, IO, IOApp}
import com.andy.samples.ZMartApp.{Item, Purchase}
import org.scalatest.wordspec.AnyWordSpec

import java.nio.charset.StandardCharsets
import scala.collection.immutable.Map

class ZMartDeserializationSpec extends AnyWordSpec {
  val purchase = Purchase("0000_0000_0000_0000_1234", "customer1", Map(Item("item_id_1", 50.23) -> 2), "50202")
  val purchaseSerde = ZMartApp.Purchase.purchaseSerde

  "purchaseSerde" should {
    "serialize purchase" in {
      val serialized = purchaseSerde.serializer().serialize("a", purchase)
      val result = new String(serialized)
      println(result)
    }

    "deserialize purchase" in {
      val raw = "{\"creditCard\":\"0000_0000_0000_0000_1234\",\"customerId\":\"customer1\",\"itemQty\":[{\"item\":{\"id\":\"item_id_1\",\"price\":50.23},\"qty\":2}],\"zipCode\":\"50202\"}"
      val result = purchaseSerde.deserializer().deserialize("a", raw.getBytes(StandardCharsets.UTF_8))
      assert(result == purchase)
    }
  }

}
