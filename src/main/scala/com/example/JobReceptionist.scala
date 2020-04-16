package com.example

import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.{ActorRef, Behavior, Terminated}
import akka.actor.typed.scaladsl.Behaviors

import scala.concurrent.duration.FiniteDuration

object JobReceptionist {
  sealed trait Command extends CborSerializable
  final case class RequestJob(id: Long, text: List[String], replyTo: ActorRef[Event]) extends Command
  private final case class HandleJobCompleted(id: Long, count: Map[String, Int], handle: ActorRef[Nothing]) extends Command

  sealed trait Event extends CborSerializable
  final case class JobFailed(id: Long) extends Event
  final case class JobSucceeded(id: Long, count: Map[String, Int]) extends Event

  private final case class Job(id: Long, text: List[String], replyTo: ActorRef[Event], handle: ActorRef[Nothing], retries: Int)

  def apply(maxRetries: Int, timeout: FiniteDuration, packageSize: Int): Behavior[Command] =
    Behaviors.setup[Command] { context =>
      context.log.info("Starting up {}", context.self)
      context.system.receptionist ! Receptionist.Register(JobProducer.serviceKey, context.self)
      working(maxRetries, timeout, packageSize, Map.empty, Map.empty)
    }

  def working(
    maxRetries: Int,
    timeout: FiniteDuration,
    packageSize: Int,
    idToJob: Map[Long, Job],
    masterToJob: Map[ActorRef[Nothing], Job],
  ): Behavior[Command] =
    Behaviors.setup[Command] { context =>

      val masterAdapter = context.messageAdapter[JobMaster.Event] {
        case JobMaster.JobCompleted(id, count, handle) => HandleJobCompleted(id, count, handle)
      }

      Behaviors
        .receiveMessage[Command] {
          case RequestJob(id, text, replyTo) =>
            context.log.info("Received job request {}", id)

            val master = context.spawn[Nothing](JobMaster(id, masterAdapter, text, timeout, packageSize), s"master-$id-0")
            val job = Job(id, text, replyTo, master, 0)
            context.watch(master)

            working(maxRetries, timeout, packageSize, idToJob + (id -> job), masterToJob + (master -> job))

          case HandleJobCompleted(id, count, handle) =>
            context.log.info("Received job failure {}", id)

            idToJob
              .get(id)
              .foreach(_.replyTo ! JobSucceeded(id, count))

            working(maxRetries, timeout, packageSize, idToJob - id, masterToJob - handle)
        }
        .receiveSignal {
          case (context, Terminated(ref)) =>
            context.log.info("Master terminated {}", ref)

            masterToJob.get(ref).fold(Behaviors.same[Command]) {
              case Job(id, text, replyTo, handle, retries) =>
                if (retries < maxRetries) {
                  val master = context.spawn[Nothing](JobMaster(id, masterAdapter, text, timeout, packageSize), s"master-$id-${retries + 1}")
                  context.watch(master)

                  working(
                    maxRetries,
                    timeout,
                    packageSize,
                    idToJob + (id -> Job(id, text, replyTo, master, retries + 1)),
                    (masterToJob - handle) + (master -> Job(id, text, replyTo, master, retries + 1))
                  )
                } else {
                  working(maxRetries, timeout, packageSize, idToJob - id, masterToJob - handle)
                }
            }
        }
    }
}
