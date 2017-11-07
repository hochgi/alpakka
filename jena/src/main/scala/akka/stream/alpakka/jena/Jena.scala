/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jena

import java.io._
import java.nio.charset.StandardCharsets

import akka.stream._
import akka.stream.alpakka.jena.Jena.RDFEvent
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import org.apache.commons.io.input.ReaderInputStream
import org.apache.jena.sparql.core.{Quad => JQuad}
import org.apache.jena.graph.{Triple => JTriple}
import org.apache.jena.riot.RDFParser
import org.apache.jena.riot.system.StreamRDF

import scala.collection.mutable.{Queue => MQueue}

object Jena {
  sealed trait RDFEvent
  final case class Base(base: String) extends RDFEvent
  final case class Prefix(prefix: String, iri: String) extends RDFEvent
  final case class Quad(quad: JQuad) extends RDFEvent
  final case class Triple(triple: JTriple) extends RDFEvent
}
import Jena._

class ParseRDF(lang: org.apache.jena.riot.Lang) extends GraphStage[FlowShape[ByteString, RDFEvent]] {
  val in = Inlet[ByteString]("ParseRDF.in")
  val out = Outlet[RDFEvent]("ParseRDF.out")

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {

      val reader = new PipedReader()
      val writer = new PipedWriter(reader)
      val events = MQueue.empty[RDFEvent]

      def doCompleteStage() = {
        writer.close()
        reader.close()
        completeStage()
      }

      override def preStart(): Unit = {
        val acb: RDFEvent => Unit = getAsyncCallback[RDFEvent] { rdfEvent =>
          if (events.isEmpty && isAvailable(out)) {
            push(out, rdfEvent)
            if (!hasBeenPulled(in))
              pull(in)
          } else {
            events.enqueue(rdfEvent)
            if (isAvailable(out))
              push(out, events.dequeue())
          }
        }.invoke

        val streamRDF = new StreamRDF {
          override def start(): Unit = getAsyncCallback[Unit](_ => tryPull(in)).invoke(())
          override def finish(): Unit =
            getAsyncCallback[Unit] { _ =>
              if (events.isEmpty) doCompleteStage()
              else emitMultiple(out, events.toVector, () => doCompleteStage())
            }.invoke(())
          override def prefix(prefix: String, iri: String): Unit = acb(Prefix(prefix, iri))
          override def quad(quad: JQuad): Unit = acb(Quad(quad))
          override def triple(triple: JTriple): Unit = acb(Triple(triple))
          override def base(base: String): Unit = acb(Base(base))
        }

        RDFParser
          .source(new ReaderInputStream(reader, StandardCharsets.UTF_8))
          .lang(lang)
          .parse(streamRDF)

        super.preStart()
      }

      override def onPush(): Unit = {
        writer.append(grab(in).utf8String)
        if (events.isEmpty && isAvailable(out)) pull(in)
      }

      override def onPull(): Unit = {
        if (events.nonEmpty) push(out, events.dequeue())
        if (events.isEmpty && !hasBeenPulled(in)) pull(in)
      }

      setHandlers(in, out, this)
    }
}
