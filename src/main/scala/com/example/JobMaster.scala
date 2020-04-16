package com.example

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior, Terminated}

import scala.concurrent.duration._

object JobMaster {
  private sealed trait Command extends CborSerializable

  // Worker Adapter
  private final case class HandleReady(handle: ActorRef[JobWorker.Command]) extends Command
  private final case class HandleRecruited(handle: ActorRef[JobWorker.Command]) extends Command
  private final case class HandleTaskCompleted(count: Map[String, Int]) extends Command

  // Receptionist Adapter
  private final case class HandleWorkerListing(listing: Receptionist.Listing) extends Command

  // Timeout
  private final case object HandleRecruitmentTimeout extends Command

  sealed trait Event extends CborSerializable
  final case class JobCompleted(id: Long, count: Map[String, Int], handle: ActorRef[Nothing]) extends Event

  val serviceKey: ServiceKey[JobWorker.Command] = ServiceKey("worker-service-key")

  def apply(id: Long, replyTo: ActorRef[Event], text: List[String], timeout: FiniteDuration, packageSize: Int): Behavior[Nothing] =
    Behaviors
      .setup[Command] { context =>
        context.log.info("Starting up {}", context.self)

        val packages = text.grouped(packageSize).toList
        context.system.receptionist ! Receptionist.Find(serviceKey, receptionistAdapter(context))

        Behaviors.withTimers[Command] { scheduler =>
          scheduler.startSingleTimer(HandleRecruitmentTimeout, timeout)

          Behaviors.receiveMessagePartial[Command] {
            case HandleWorkerListing(serviceKey.Listing(workers)) =>
              context.log.info("Received available workers {}", workers)

              val adapter = workerAdapter(context)
              workers.foreach(_ ! JobWorker.Recruit(id, adapter))

              working(id, replyTo, packages.size, packages, Nil, workers, scheduler)

            case HandleRecruitmentTimeout =>
              context.log.info("Received recruitment timeout")
              Behaviors.stopped
          }
        }
      }
      .narrow

  private def working(
    id: Long,
    replyTo: ActorRef[Event],
    numberOfPackages: Int,
    workPackages: List[List[String]],
    donePackages: List[Map[String, Int]],
    workers: Set[ActorRef[JobWorker.Command]],
    scheduler: TimerScheduler[Command]
  ): Behavior[Command] =
    Behaviors.setup[Command] { context =>
      Behaviors
        .receiveMessagePartial[Command] {
          case HandleRecruited(handle) =>
            context.log.info("Received recruitment acknowledgement from {}", handle)
            context.watch(handle)
            working(id, replyTo, numberOfPackages, workPackages, donePackages, workers + handle, scheduler)

          case HandleReady(handle) =>
            context.log.info("Received ready acknowledgement from {}", handle)
            workPackages match {
              case Nil =>
                handle ! JobWorker.Stop
                Behaviors.same

              case next :: tail =>
                handle ! JobWorker.Work(next)
                working(id, replyTo, numberOfPackages, tail, donePackages, workers, scheduler)
            }

          case HandleTaskCompleted(count) =>
            context.log.info("Received partial solution from {}", count.toString)
            val packages = count :: donePackages

            if (workPackages.isEmpty && packages.size == numberOfPackages) {
              val count = aggregatePackages(packages)
              replyTo ! JobCompleted(id, count, context.self)
              Behaviors.stopped[Command] { () => stopWorkers(workers) }

            } else {
              working(id, replyTo, numberOfPackages, workPackages, packages, workers, scheduler)
            }

          case HandleRecruitmentTimeout =>
            if (workers.isEmpty)
              Behaviors.stopped

            else
              Behaviors.same

        }.receiveSignal {
          case (context, Terminated(ref)) =>
            context.log.info("Worker terminated {}", ref)
            Behaviors.stopped[Command] { () => stopWorkers(workers) }
        }
    }

  private def receptionistAdapter(context: ActorContext[Command]): ActorRef[Receptionist.Listing] = {
    context.messageAdapter[Receptionist.Listing](HandleWorkerListing)
  }

  private def workerAdapter(context: ActorContext[Command]): ActorRef[JobWorker.Event] = {
    import JobWorker._

    context.messageAdapter[JobWorker.Event] {
      case Recruited(handle) => HandleRecruited(handle)
      case Ready(handle) => HandleReady(handle)
      case Completed(count) => HandleTaskCompleted(count)
    }
  }

  private def aggregatePackages(packages: List[Map[String, Int]]): Map[String, Int] = {
    def addCount(counts: Map[String, Int], merged: Map[String, Int]): Map[String, Int] = {
      (merged -- counts.keys) ++ counts.map { case (word, count) =>
        word -> (merged.getOrElse(word, 0) + count)
      }
    }

    packages.foldLeft[Map[String, Int]](Map.empty)(addCount)
  }

  private def stopWorkers(workers: Set[ActorRef[JobWorker.Command]]): Unit =
    workers.foreach(_ ! JobWorker.Stop)
}
