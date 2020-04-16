package com.example

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.typed.Cluster
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

object Main {

  val text: List[String] =
    """Lorem ipsum dolor sit amet, consetetur sadipscing elitr
      |sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat
      |sed diam voluptua At vero eos et accusam et justo duo dolores et ea rebum
      |Stet clita kasd gubergren no sea takimata sanctus est Lorem ipsum dolor sit amet"""
      .stripMargin
      .toLowerCase
      .split(' ')
      .toList

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    ActorSystem[Nothing](
      Behaviors.setup[Nothing] { context =>
        context.log.info("Starting up system")

        val cluster = Cluster(context.system)

        if (cluster.selfMember.hasRole("producer")) {
          context.log.info("Starting up producer guardian {}", context.self)
          context.spawn[Nothing](JobPinger(), "pinger")
        }

        if (cluster.selfMember.hasRole("master")) {
          context.log.info("Starting up master guardian {}", context.self)
          context.spawn(JobReceptionist(3, 10.seconds, 3), "receptionist")
        }

        if (cluster.selfMember.hasRole("worker")) {
          context.log.info("Starting up worker guardian {}", context.self)
          for (i <- 1 to args.headOption.fold(1)(_.toInt)) context.spawn(JobWorker(500.millis), s"worker-$i")
        }

        Behaviors.empty[Nothing]
      },
      name = "word-count",
      config = config
    )
  }
}
