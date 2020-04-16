package com.example

import akka.actor.typed.Behavior
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.Behaviors

object JobPinger {
  def apply(): Behavior[Nothing] =
    Behaviors.setup[Receptionist.Listing] { context =>
      context.system.receptionist ! Receptionist.Subscribe(JobProducer.serviceKey, context.self)

      Behaviors.receiveMessage[Receptionist.Listing] {
        case JobProducer.serviceKey.Listing(listings) =>
          context.log.info("Received {}", listings)
          Behaviors.same
      }
    }.narrow
}
