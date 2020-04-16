package com.example

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

class JobWorkerSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike {
  "A Worker" must {
    "accept work when idle" in {
      val probe = createTestProbe[JobWorker.Event]
      val worker = spawn(JobWorker(500.millis))

      val text = List("Hello", "world", "Goodbye", "world")
      val count = Map("Hello" -> 1, "world" -> 2, "Goodbye" -> 1)

      worker ! JobWorker.Recruit(1L, probe.ref)
      probe.expectMessage(JobWorker.Recruited(worker.ref))
      probe.expectMessage(JobWorker.Ready(worker.ref))

      worker ! JobWorker.Work(text)
      probe.expectMessage(JobWorker.Completed(count))
      probe.expectMessage(JobWorker.Ready(worker.ref))

      worker ! JobWorker.Stop
      testKit.stop(worker)
    }

    "request work when working" in {
      val probe = createTestProbe[JobWorker.Event]
      val worker = spawn(JobWorker(500.millis))

      val text = List("Hello", "world", "Goodbye", "world")
      val count = Map("Hello" -> 1, "world" -> 2, "Goodbye" -> 1)

      worker ! JobWorker.Recruit(1L, probe.ref)
      probe.expectMessage(JobWorker.Recruited(worker.ref))
      probe.expectMessage(JobWorker.Ready(worker.ref))
      probe.expectMessage(JobWorker.Ready(worker.ref))

      worker ! JobWorker.Work(text)
      probe.expectMessage(JobWorker.Completed(count))
      probe.expectMessage(JobWorker.Ready(worker.ref))

      worker ! JobWorker.Stop
      testKit.stop(worker)
    }
  }
}
