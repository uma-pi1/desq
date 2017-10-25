package de.uni_mannheim.desq.examples.spark

import java.util

import de.uni_mannheim.desq.Desq._
import de.uni_mannheim.desq.mining.spark.{DesqCount, DesqDataset, DesqMiner}
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.DesqDfs
import de.uni_mannheim.desq.patex.PatExToItemsetPatEx
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sulbrich on 18.09.2017
  */
object DesqItemsetExample {

  /** run on small icdm16 dataset **/
  def icdm16(compare: Boolean = false)(implicit sc: SparkContext) {

    val patternExpression = "(.)? . c" //"[c|d|e] (A)!*&(B)+&a1!+ [d|e]" "[c|d] (A)+ B"
    val sigma = 1
    val conf = DesqCount.createConf(patternExpression, sigma)
    if(compare){
      println("\n ==== sequence query =====")
      ExampleUtils.runIcdm16(conf)
    }
    println("\n ==== itemset query =====")
    ExampleUtils.runIcdm16(conf,asItemset = true)
  }

  /** run itemset query on fimi-retail data**/
  def fimi_retail()(implicit sc: SparkContext) {

    val patEx = "(38)"
    val minSupport = 1000

    val data = "data-local/fimi_retail/retail.dat"
    val dict = Option.apply(Dictionary.loadFrom("data-local/fimi_retail/dict.json",sc))

    runItemsetMiner(
      rawData =     data,
      patEx =       patEx,
      minSupport =  minSupport,
      extDict =     dict)
  }

  /** run itemset query on nyt91 data**/
  def nyt91()(implicit sc: SparkContext) {

    val patEx = "(..)"
    val minSupport = 1000

    val data = DesqDataset.load("data-local/nyt-1991-data")

    runItemsetMiner(
      rawData =     data,
      patEx =       patEx,
      minSupport =  minSupport,
      extDict =     None)
  }

  /**
    * Prototype of DESQ API for Itemset Mining
    */
  def runItemsetMiner[T](
                          rawData: T,
                          patEx:String,
                          minSupport: Int = 1000,
                          extDict: Option[Dictionary]
                        )(implicit sc: SparkContext): (DesqMiner, DesqDataset) ={

    // Manage different data inputs (DesqDataset, FilePath, RDD)
    var data: DesqDataset = null
    rawData match{
      case dds: DesqDataset => //Build from existing DesqDataset
        data = DesqDataset.buildItemsets(dds)
      case file: String => //Build from space delimited file
        data = DesqDataset.buildItemsets(sc.textFile(file).map(s => s.split(" ")), extDict = extDict)
      case rdd: RDD[Array[Any]] => //Build from RDD
        data = DesqDataset.buildItemsets(rdd,extDict = extDict)
      case _ =>
        println("ERROR: Unsupported input type")
        return (null,null)
    }

    //Convert PatEx
    val itemsetPatEx = new PatExToItemsetPatEx(data.dict, patEx).translate()

    //Print some information
    println("\nConverted PatEx: " + patEx +"  ->  " + itemsetPatEx)
    println("\nDictionary size: " + data.dict.size())
    //data.dict.writeJson(System.out)
    println("\nSeparatorGid: " + data.itemsetSeparatorGid + "(" + data.getCfreqOfSeparator() + ")")
    println("\nFirst 10 Input Sequences:")
    data.print(10)

    // Init Desq Miner
    val confDesq = DesqCount.createConf(itemsetPatEx, minSupport)
    confDesq.setProperty("desq.mining.prune.irrelevant.inputs", true)
    confDesq.setProperty("desq.mining.use.two.pass", false)

    //Run Miner
    val (miner, result) = ExampleUtils.runVerbose(data,confDesq)

    (miner, result)
  }

  def main(args: Array[String]) {
    //Init SparkConf
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    initDesq(conf)
    implicit val sc:SparkContext = new SparkContext(conf)

    icdm16()
    //icdm16(compare = true)
    //fimi_retail
    //nyt91
  }

}
