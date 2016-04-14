package com.wang.spark.mllib

/**
  * Created by root on 4/14/16.
  */

import org.apache.spark.SparkConf
import org.apache.spark.mllib.stat.test.{BinarySample, StreamingTest}
import org.apache.spark.streaming.{Seconds, StreamingContext}

class StreamingSignificantTestExample {
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
        streamingTest.test(args(0))
    }

    def test(input: String) {
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

        // $example on$
        // textFileStream(folder) will check the folder until new file has been generated.
        val data = ssc.textFileStream(input).map(line => line.split(",") match {
            case Array(label, value) => BinarySample(label.toBoolean, value.toDouble)
        })

        val streamingTest = new StreamingTest()
            .setPeacePeriod(0)
            .setWindowSize(0)
            .setTestMethod("welch")

        val out = streamingTest.registerStream(data)
        out.print()
        // $example off$

        // Stop processing if test becomes significant or we time out
        var timeoutCounter = numBatchesTimeout
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
        // until termination signal received.
        ssc.awaitTermination()
    }
}
