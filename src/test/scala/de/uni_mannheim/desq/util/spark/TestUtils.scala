package de.uni_mannheim.desq.util.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._

/**
  * Created by rgemulla on 19.9.2016.
  */
object TestUtils {
  val sparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local")
  val sc = new SparkContext(sparkConf)
}
