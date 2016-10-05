package de.uni_mannheim.desq.util.spark

import de.uni_mannheim.desq.Desq._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by rgemulla on 19.9.2016.
  */
object TestUtils {
  val sparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local")
  initDesq(sparkConf)
  val sc = new SparkContext(sparkConf)
}
