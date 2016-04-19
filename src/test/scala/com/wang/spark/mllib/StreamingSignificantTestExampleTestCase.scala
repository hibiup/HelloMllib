package com.wang.spark.mllib

import org.junit._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
/**
  * Created by root on 4/14/16.
  */
@RunWith(classOf[JUnitRunner])
class StreamingSignificantTestExampleTestCase extends FunSuite{
    test("file streaming test") {
        val testDataDir = "hdfs://hadoop:9000/user/root/spark/streaming/significantTest"

        val streamingTest:StreamingSignificantTestExample = new StreamingSignificantTestExample()
        streamingTest.test(streamingTest.fileStreamingData(testDataDir), 0)
    }

    test("random data streaming test") {
        val streamingTest:StreamingSignificantTestExample = new StreamingSignificantTestExample()
        streamingTest.test(streamingTest.randomData(), 0)
    }

    test("test get data") {
        val streamingTest:StreamingSignificantTestExample = new StreamingSignificantTestExample()
        println(streamingTest.randomData())
    }
}
