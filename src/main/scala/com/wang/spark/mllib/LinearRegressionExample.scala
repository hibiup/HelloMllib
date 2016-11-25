package com.wang.spark.mllib

/**
  * Created by ji.wang on 2016-11-25.
  */
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.util.MLUtils

object LinearRegressionExample {
  val logger = Logger.getLogger(this.getClass)
  val home = hadoopHome()

  /**
    * winutils.exe cannot found issue:
    *  http://stackoverflow.com/questions/19620642/failed-to-locate-the-winutils-binary-in-the-hadoop-binary-path
    */
  def hadoopHome() {
    if (null == System.getProperty("hadoop.home.dir"))
      System.getenv("HADOOP_HOME")
    else
      System.getProperty("hadoop.home.dir")
  }
}

class LinearRegressionExample {
  // Load training data in LIBSVM format.
  val sparkconf = new SparkConf().setAppName("LinearRegressionExample").setMaster("local")
  val sc = new SparkContext(sparkconf)

  def training(seedFile: String): LinearRegressionModel = {
    val data = sc.textFile(seedFile)
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()

    // Training.
    val numIterations = 100
    val stepSize = 0.00000001
    val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)
    model
  }

  def predict(model: LinearRegressionModel, seedFile:String) {
    val data = sc.textFile(seedFile)
    val valuesAndPreds = data.map { line =>
      val prediction = model.predict(Vectors.dense(line.split(' ').map(_.toDouble)))
      (line.toDouble, prediction)
    }.cache()

    val MSE = valuesAndPreds.map{
      case(v, p) => math.pow((v - p), 2)
    }.mean()
    println("training Mean Squared Error = " + MSE)
  }
}
