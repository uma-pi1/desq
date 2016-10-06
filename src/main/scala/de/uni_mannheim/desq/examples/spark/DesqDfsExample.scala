package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.mining.spark.DesqDfs
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by rgemulla on 14.09.2016.
  */
object DesqDfsExample {
  def icdm16()(implicit sc: SparkContext) {
    val patternExpression = "[c|d]([A^|B=^]+)e"
    //val patternExpression = "(.)"
    val sigma = 2
    val conf = DesqDfs.createConf(patternExpression, sigma)
    conf.setProperty("desq.mining.prune.irrelevant.inputs", "true")
    ExampleUtils.runIcdm16(conf)
  }

  /*def nyt()(implicit sc: SparkContext) {
    val patternExpression = "(.^ JJ@ NN@)"
    val sigma = 1000
    val conf = DesqCount.createConf(patternExpression, sigma)
    conf.setProperty("desq.mining.prune.irrelevant.inputs", true)
    conf.setProperty("desq.mining.use.two.pass", true)
    ExampleUtils.runNyt(conf)
  }*/

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    implicit val sc = new SparkContext(conf)
    //sc.setLogLevel("INFO")
    icdm16
    //nyt
  }
}
