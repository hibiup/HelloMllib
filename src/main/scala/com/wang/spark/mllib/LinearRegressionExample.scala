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

  /**
    * 训练用的 Seed file 文件格式可以是例如（房屋面，价格）。在本例中故意将价格等于面积x1000是为了方便验证结果。
    * 因为最终我们要用差方（R^2）来检验结果，但是因为没有真实数据，所以假定一个价格计算公式。
    *
    * 预测文件输入房屋面积，得到价格输出。然后用假定的的计算公式获得所谓的真实价格，然后来得到R^2
    *
    * @param seedFile
    * @return
    */
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
      // line.toDouble*1000 是假定的真实售价，prediction 是预测值，返回这两个数字好做比较
      (line.toDouble*1000, prediction)
    }.cache()

    val MSE = valuesAndPreds.map{
      // 计算差方
      case(v, p) => math.pow((v - p), 2)
    }.mean()
    println("training Mean Squared Error = " + MSE)
  }
}
