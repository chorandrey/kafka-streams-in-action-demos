package com.andy
package samples

import model.{Purchase, RewardAccumulator}

import samples.ZMartApp.Constants
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore

class RewardsKeyValueTransformer extends Transformer[String, Purchase, KeyValue[String, RewardAccumulator]]{
  var stateStore: KeyValueStore[String, RewardAccumulator] = _
  var processorContext: ProcessorContext = _

  override def init(context: ProcessorContext): Unit = {
    this.processorContext = context
    stateStore = context.getStateStore[KeyValueStore[String, RewardAccumulator]](Constants.rewardsAccumulator)
  }

  override def transform(key: String, value: Purchase): KeyValue[String, RewardAccumulator] = {
    val purchase = value
    val customerId = purchase.customerId
    val accumulated = Option(stateStore.get(customerId))

    val currentMoneySpent = purchase.price * purchase.quantity.toDouble
    val currentPoints = (currentMoneySpent * 0.1).toInt

    val rewardAccumulatorRet = accumulated match {
      case Some(prevAcum) =>
        val updatedReward = prevAcum.copy(
          purchaseTotal = currentMoneySpent,
          totalRewardPoints = currentPoints + prevAcum.totalRewardPoints,
          currentRewardPoints = currentPoints,
          mostRecentOperationTimestamp = purchase.purchaseTime,
          daysSinceLastPurchase = Some(prevAcum.daysSincePurchase(purchase.purchaseTime))
        )
        stateStore.put(customerId, updatedReward)
        updatedReward
      case None =>
        val newReward = RewardAccumulator(
          purchase.customerId,
          currentMoneySpent,
          currentPoints,
          currentPoints,
          purchase.purchaseTime,
          None
        )
        stateStore.put(customerId, newReward)
        newReward
    }
    new KeyValue(key, rewardAccumulatorRet)
  }

  override def close(): Unit = ()

}
