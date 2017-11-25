package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.Desq._
import de.uni_mannheim.desq.mining.spark.{DesqCount, DesqDataset, DesqMiner}
import de.uni_mannheim.desq.dictionary.{Dictionary, ItemsetBuilderFactory}
import de.uni_mannheim.desq.patex.PatExToItemsetPatEx
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by sulbrich on 18.09.2017
  */
object DesqItemsetExample {

  /** run on small icdm16 dataset **/
  def icdm16(compare: Boolean = false)(implicit sc: SparkContext) {

    //val patternExpression = "[c|d] (A)+ B" //"[c|d|e] (A)!*&(B)+&a1!+ [d|e]" "[c|d] (A)+ B" "(.)? . c"
    val patternExpression = "(-B=)"
    val sigma = 1
    val conf = DesqCount.createConf(patternExpression, sigma)
    if(compare){
      println("\n ==== sequence query =====")
      ExampleUtils.runIcdm16(conf)
    }
    println("\n ==== itemset query =====")
    ExampleUtils.runIcdm16(conf, asItemset = true)
  }

  /** run eval on icdm16 */
  def evalIdcm16()(implicit sc: SparkContext){
    val patternExpression = "[c|d] (A)+ B"
    val sigma = 1
    //val conf = DesqCount.createConf(patternExpression, sigma)

    val dictFile = this.getClass.getResource("/icdm16-example/dict.json")
    val dataFile = this.getClass.getResource("/icdm16-example/data.del")

    // load the dictionary & update hierarchy
    val dict = Dictionary.loadFrom(dictFile)
    val delFile = sc.parallelize(Source.fromURL(dataFile).getLines.toSeq)
    val data = DesqDataset.loadFromDelFile(delFile, dict, usesFids = false).copyWithRecomputedCountsAndFids()

    println("\n ==== sequence query =====")
    ExampleUtils.runPerformanceEval(patternExpression,sigma,data)

    println("\n ==== itemset query =====")
    ExampleUtils.runPerformanceEval(patternExpression,sigma,data,asItemset = true)
  }

  /** run/eval itemset query on fimi-retail data and stores metrics in CSVs in data-local/ **/
  def fimi_retail(eval: Boolean = false)(implicit sc: SparkContext) {

    val patEx = "(39)&(41)&.!* (.)!{2,5}&.!* "//"(39)&(41)!{0,7}&.!*"
    val minSupport = 500

    if(eval) { // Run evaluation with logging of metrics
      //val conf = DesqCount.createConf(patEx, minSupport) //do not reuse config across multiple runs!

      println("\n ==== Evaluate sequence query =====")
      ExampleUtils.runPerformanceEval(
        patEx, minSupport,
        DesqDataset.buildFromStrings(sc.textFile("data-local/fimi_retail/retail.dat").map(s => s.split(" "))),
        logFile = "data-local/testSeq.csv"
      )

      println("\n ==== Evaluate itemset query =====")
      ExampleUtils.runPerformanceEval(
        patEx, minSupport,
        DesqDataset.buildFromStrings(sc.textFile("data-local/fimi_retail/retail.dat").map(s => s.split(" "))),
        asItemset = true, logFile = "data-local/testItemset.csv"
      )
    }else {
      val data = "data-local/fimi_retail/retail.dat"
      val dict = Option.apply(Dictionary.loadFrom("data-local/fimi_retail/dict.json", sc))

      println("\n ==== FIMI - Retail: Itemset query =====")
      runItemsetMiner(
        rawData = data,
        patEx = patEx,
        minSupport = minSupport,
        extDict = dict)
    }
  }

  /** run itemset query on nyt91 data**/
  def nyt91(eval: Boolean = false)(implicit sc: SparkContext) {

    val patEx = "(.)!{3}&.!*" // PATTY: n-grams of (max?) length 3 (Up to 4 per sentence???)
    //val patEx = "(..)"
    val minSupport = 100000
    val data = DesqDataset.load("data-local/nyt-1991-data")

    if(eval){
      ExampleUtils.runPerformanceEval(
        patEx, minSupport,
        data,
        asItemset = true, //"each sentence viewed as a shopping transaction"
        logFile = "data-local/logPATTYItemset.csv"
      )
    }else {
      runItemsetMiner(
        rawData = data,
        patEx = patEx,
        minSupport = minSupport,
        extDict = None)
    }
  }

  /**
    * Generic method to run Queries on ItemSet Data
    */
  def runItemsetMiner[T](
                          rawData: T,
                          patEx:String,
                          minSupport: Int = 1000,
                          extDict: Option[Dictionary]
                        )(implicit sc: SparkContext): (DesqMiner, DesqDataset) ={

    // Manage different data inputs (DesqDataset, FilePath, RDD)
    var data: DesqDataset = null
    val factory = if(extDict.isDefined) new ItemsetBuilderFactory(extDict.get) else new ItemsetBuilderFactory()
    rawData match {
      case dds: DesqDataset => //Build from existing DesqDataset
        data = DesqDataset.buildFromStrings(dds.toSids, Option.apply(factory))
      case file: String => //Build from space delimited file
        data = DesqDataset.buildFromStrings(sc.textFile(file).map(s => s.split(" ")), Option.apply(factory))
      case _ =>
        println("ERROR: Unsupported input type")
        return (null, null)
    }

    //Convert PatEx
    val itemsetPatEx = new PatExToItemsetPatEx(patEx).translate()

    //Print some information
    println("\nConverted PatEx: " + patEx +"  ->  " + itemsetPatEx)
    println("\nDictionary size: " + data.dict.size())
    //data.dict.writeJson(System.out)
    //println("\nSeparatorGid: " + data.itemsetSeparatorGid + "(" + data.getCfreqOfSeparator + ")")
    println("\nFirst 10 (of " + data.sequences.count() + ") Input Sequences:")
    data.print(10)

    // Init Desq Miner
    val confDesq = DesqCount.createConf(itemsetPatEx, minSupport)
    confDesq.setProperty("desq.mining.prune.irrelevant.inputs", true)
    confDesq.setProperty("desq.mining.use.two.pass", false)

    //Run Miner
    val (miner, result) = ExampleUtils.runVerbose(data,confDesq)

    /*//Print relevant dict entries:
    println("Patterns (SIDs):")
    for((result, weight) <- result.toSidsWeightPairs()){
      println("[" + result.mkString(" ") + "]@" + weight)
    }*/

    (miner, result)
  }

  def main(args: Array[String]) {
    //Init SparkConf
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    initDesq(conf)
    implicit val sc:SparkContext = new SparkContext(conf)

    //icdm16()
    icdm16(compare = true)
    //evalIdcm16

    //fimi_retail()
    //fimi_retail(eval = true)

    //nyt91(eval = true)
  }

}
