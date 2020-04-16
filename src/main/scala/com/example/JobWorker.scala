package com.example

import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, Terminated}

import scala.concurrent.duration._

object JobWorker {
  sealed trait Command extends CborSerializable
  final case class Recruit(id: Long, replyTo: ActorRef[Event]) extends Command
  final case class Work(input: List[String]) extends Command
  final case object Stop extends Command

  // Timeouts
  private final case object HandleIdleTimeout extends Command

  sealed trait Event extends CborSerializable
  final case class Recruited(handle: ActorRef[Command]) extends Event
  final case class Ready(handle: ActorRef[Command]) extends Event
  final case class Completed(count: Map[String, Int]) extends Event

  def apply(timeout: FiniteDuration): Behavior[Command] =
    Behaviors.setup[Command] { context =>
      context.log.info("Starting up {}", context.self)
      context.system.receptionist ! Receptionist.Register(JobMaster.serviceKey, context.self)
      idle(timeout)
    }

  def idle(timeout: FiniteDuration): Behavior[Command] =
    Behaviors.setup[Command] { context =>
      Behaviors.receiveMessagePartial[Command] {
        case Recruit(id, replyTo) =>
          context.log.info("Received recruitment request for {} from {}", id, replyTo)
          replyTo ! Recruited(context.self)
          replyTo ! Ready(context.self)
          context.watch(replyTo)

          working(timeout, id, replyTo)
      }
    }

  def working(timeout: FiniteDuration, id: Long, replyTo: ActorRef[Event]): Behavior[Command] =
    Behaviors.setup[Command] { context =>
      Behaviors.withTimers[Command] { timers =>
        timers.startSingleTimer(HandleIdleTimeout, timeout)

        Behaviors
          .receiveMessagePartial[Command] {
            case HandleIdleTimeout =>
              replyTo ! Ready(context.self)
              Behaviors.same

            case Work(input) =>
              context.log.info("Received work request for {} from {}", id, replyTo)
              val counts = input.groupMapReduce(identity)(_ => 1)(_ + _)
              replyTo ! Completed(counts)
              replyTo ! Ready(context.self)
              Behaviors.same

            case Stop =>
              context.log.info("Received stop request for {} from {}", id, replyTo)
              timers.cancelAll()
              idle(timeout)
          }
          .receiveSignal {
            case (context, Terminated(ref)) =>
              context.log.info("Master terminated {}", ref)
              idle(timeout)
          }
      }
    }
}
