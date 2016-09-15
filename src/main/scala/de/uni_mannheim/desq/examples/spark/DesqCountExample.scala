package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.mining.spark.DesqCount
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by rgemulla on 14.09.2016.
  */
object DesqCountExample extends App {
  val sparkConf = new SparkConf().setAppName(getClass.getName)
  val sc = new SparkContext(sparkConf)

  val patternExpression = "[c|d]([A^|B=^]+)e"
  val sigma = 2
  val conf = DesqCount.createConf(patternExpression, sigma)
  ExampleUtils.runIcdm16(sc, conf)
}
