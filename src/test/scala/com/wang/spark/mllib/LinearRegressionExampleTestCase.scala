package com.wang.spark.mllib

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * Created by ji.wang on 2016-11-25.
  */
@RunWith(classOf[JUnitRunner])
class LinearRegressionExampleTestCase extends FunSuite{
  test("linear regression test") {
    val linearRegression:LinearRegressionExample = new LinearRegressionExample()
    val model = linearRegression.training("src/test/resources/LinearRegressionExampleDateSet-Training.txt")
    linearRegression.predict(model, "src/test/resources/LinearRegressionExampleDataSet-Testing.txt")
  }
}
