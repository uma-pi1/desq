package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.Desq._
import de.uni_mannheim.desq.mining.spark.{DesqCount, DesqDataset}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sulbrich on 18.09.2017
  */
object DesqItemsetExample {
  def runItemsetMiner(data: DesqDataset)(implicit sc:SparkContext) {

    //Init Desq for Itemset Mining
    val patternExpression = "[.*(.)]+"
    val sigma = 2
    val confDesq = DesqCount.createConf(patternExpression, sigma)
    confDesq.setProperty("desq.mining.prune.irrelevant.inputs", true)
    confDesq.setProperty("desq.mining.use.two.pass", true)

    ExampleUtils.runMiner(DesqDataset.buildItemsets(data), confDesq)

  }



  def main(args: Array[String]) {
    //Init SparkConf
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    initDesq(conf)
    implicit val sc = new SparkContext(conf)

    runItemsetMiner(data = DesqDataset.load("data-local/nyt-1991-data"))
  }
}
