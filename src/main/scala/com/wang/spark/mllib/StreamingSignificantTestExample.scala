package com.wang.spark.mllib

/**
  * Created by root on 4/14/16.
  */

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.mllib.stat.test.{BinarySample, StreamingTest}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

class StreamingSignificantTestExample {
    val logger = Logger.getLogger(this.getClass)

    val batchDuration = Seconds(5)
    val numBatchesTimeout = 2
    val checkpoint = "/tmp/spark/checkpoint"

    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamingTestExample")
    conf.set("spark.ui.view.acls", "root")
    conf.set("spark.modify.acls", "root")

    val ssc = new StreamingContext(conf, batchDuration)
    ssc.checkpoint({
        val dir = checkpoint
        dir.toString
    })

    def test(data: DStream[BinarySample], windowSize: Int) {
        import scala.util.control.Exception._
        import org.apache.commons.math3.exception.NumberIsTooSmallException
        catching(classOf[NumberIsTooSmallException])
            .andFinally(
                println("how can I close the stream ?")
            )
            .either(
                println("either")
            ) match {
            case e:Exception =>
                logger.error(e.getMessage, e)
            case Left(e) =>
                logger.error(e.getMessage, e)
            case Right(v) => v
            case _ => _
        }

        // $example on$
        val streamingTest = new StreamingTest()
            .setPeacePeriod(0)
            .setWindowSize(windowSize)
            .setTestMethod("welch")

        val out = streamingTest.registerStream(data)
        out.print()
        // $example off$

        // Stop processing if test becomes significant
        out.foreachRDD {
            rdd => {
                val anySignificant = rdd.map(_.pValue < 0.05).fold(false)(_ || _)
                if (anySignificant) {
                    rdd.context.stop()
                    //System.exit(0)
                }
            }
        }

        // Start
        ssc.start()
        try {
            // until termination signal received.
            ssc.awaitTermination()
        }
        catch {
            case e => logger.error(e.getMessage, e)
        }
    }

    def fileStreamingData(input: String): DStream[BinarySample] = {
        // textFileStream(folder) will check the folder until new file has been generated.
        val data: DStream[BinarySample] = ssc.textFileStream(input).map(line => line.split(",") match {
            case Array(label, value) => BinarySample(label.toBoolean, value.toDouble)
        })

        data
    }

    def randomData(): DStream[BinarySample] = {
        import akka.actor.Props
        val lines: DStream[BinarySample] = ssc.actorStream(Props(new CustomActor()), "CustomReceiver")
        lines
    }

    def main(args: Array[String]) {
        if (args.length != 1) {
            // scalastyle:off println
            System.err.println(
                "Usage: StreamingTestExample " +
                    "<dataDir>")
            // scalastyle:on println
            System.exit(1)
        }

        val streamingTest: StreamingSignificantTestExample = new StreamingSignificantTestExample()
        streamingTest.test(streamingTest.fileStreamingData(args(0)), 0)
    }
}


import akka.actor.Actor
import org.apache.spark.streaming.receiver.ActorHelper

class CustomActor extends Actor with ActorHelper {

    import scala.util.Random

    /*(1 to 5000).foreach {
        i => {
            val experiment = Random.nextBoolean
            dataActor.sender() ! BinarySample(experiment, experiment match {
                case true => Random.nextDouble
                case false => i
            })
            wait(1000)
        }
    }*/

    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global

    context.system.scheduler.schedule(2 seconds, 500 millis) {
        val experiment = Random.nextBoolean
        val testData = BinarySample(experiment, experiment match {
            case true => Random.nextDouble
            case false => 1
        })
        println(testData)

        self ! testData
    }

    def receive = {
        case data => store(data)
    }
}