package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.Desq._
import de.uni_mannheim.desq.mining.spark.{DesqCount, DesqDataset, DesqMiner}
import de.uni_mannheim.desq.dictionary.{Dictionary}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sulbrich on 18.09.2017
  */
object DesqItemsetExample {
  /**
    * Prototype of DESQ API for Itemset Mining
    */
  def runItemsetMiner[T](
                          rawData: T,
                          itemDef:String = ".",
                          minSupport: Int,
                          minSize: Int = 2,
                          extDict: Option[Dictionary]
                        )(implicit sc: SparkContext): (DesqMiner, DesqDataset) ={
    //Init Desq for Itemset Mining
    val patternExpression = "[.*(" + itemDef + ")]{" + minSize + ",}"
    // .* - arbitrary distance between items (= distance in itemsets is irrelevant)
    // (.) - captured item
    // [...]{n,} at least n-times (= n-tuple)
    val sigma = minSupport
    val confDesq = DesqCount.createConf(patternExpression, sigma)
    confDesq.setProperty("desq.mining.prune.irrelevant.inputs", true)
    confDesq.setProperty("desq.mining.use.two.pass", true)

    //Manage data
    var data: DesqDataset = null
    rawData match{
      case dds: DesqDataset => //Use existing DesqDataset as basis
        data = DesqDataset.buildItemsets(dds)
      case file: String => //Read from delimited file
          data = DesqDataset.buildItemsets(sc.textFile(file).map(s => s.split(" ")),extDict)
      case rdd: RDD[Array[Any]] =>
        data = DesqDataset.buildItemsets(rdd,extDict)
      case _ =>
        println("ERROR: Unsupported input type")
        return (null,null)
    }

    ExampleUtils.runVerbose(data,confDesq)

  }



  def main(args: Array[String]) {
    //Init SparkConf
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    initDesq(conf)
    implicit val sc:SparkContext = new SparkContext(conf)

    //val data = DesqDataset.buildFromStrings(sc.textFile("data-local/fimi_retail/retail.dat").map(s => s.split(" ")))
    val data = "data-local/fimi_retail/retail.dat" //"data-local/nyt-1991-data"
    //val data = 1

    runItemsetMiner(
      rawData =     data,
      itemDef =     ".",
      minSupport =  1000,
      minSize =     2,
      extDict =     None)
  }
}
