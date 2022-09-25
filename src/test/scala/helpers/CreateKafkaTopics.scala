package com.andy
package helpers

import cats.effect.{ExitCode, IO, IOApp}
import samples.ZMartApp.TopicNames
import fs2._

object CreateKafkaTopics extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val stream = for {
      topicName <- Stream.apply[IO, String](
        TopicNames.purchaseUnsafe, TopicNames.purchase,
        TopicNames.rewards, TopicNames.patterns,
        TopicNames.cafe, TopicNames.electronics
      )
      command = ("bash ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic " + topicName).split(" ")
      res <- Stream.eval(
        IO(os.proc(command).call(cwd = os.home / "projects" / "kafka_2.13-3.2.1" / "bin", stdin = os.Inherit, stdout = os.Inherit))
      )
    } yield res.chunks.foreach(println)

    stream.compile.drain.as(ExitCode.Success)
  }
}
