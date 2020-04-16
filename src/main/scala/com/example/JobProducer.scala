package com.example

import akka.actor.typed.Behavior
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.{Behaviors, Routers}

object JobProducer {

  val serviceKey: ServiceKey[JobReceptionist.Command] = ServiceKey[JobReceptionist.Command]("receptionist-service-key")

  def apply(text: List[String]): Behavior[JobReceptionist.Event] =
    Behaviors.setup[JobReceptionist.Event] { context =>
      context.log.info("Starting up {}", context.self)
      val router = context.spawn(Routers.group(serviceKey), "receptionist-router")
      router ! JobReceptionist.RequestJob(1L, text, context.self)

      Behaviors.receiveMessage[JobReceptionist.Event] {
        case JobReceptionist.JobFailed(id) =>
          context.log.error("Job {} failed", id)
          Behaviors.stopped

        case JobReceptionist.JobSucceeded(id, count) =>
          context.log.info("Job {} succeeded", id)
          context.log.info(count.toString)
          Behaviors.stopped
      }
    }
}
