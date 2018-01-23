package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.Desq._
import de.uni_mannheim.desq.mining.spark.{DesqCount, DesqDataset, DesqMiner}
import de.uni_mannheim.desq.dictionary.{DefaultBuilderFactory, Dictionary, ItemsetBuilderFactory}
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
    val factory = new ItemsetBuilderFactory(data.dict)
    val itemsetData = DesqDataset.buildFromStrings(data.toSids, Option.apply(factory))
    ExampleUtils.runPerformanceEval(patternExpression,sigma,
      itemsetData,
      Option.apply(new PatExToItemsetPatEx(patternExpression)))
  }

  /** run/eval itemset query on fimi-retail data and stores metrics in CSVs in data-local/ **/
  def fimi_retail(eval: Boolean = false)(implicit sc: SparkContext) {

    val patEx = "(39)&(41)&.!* (.)!{2,5}&.!* "//"(39)&(41)!{0,7}&.!*"
    val minSupport = 500

    if(eval) { // Run evaluation with logging of metrics

      println("\n ==== Evaluate sequence query =====")
      ExampleUtils.runPerformanceEval(
        patEx, minSupport,
        DesqDataset.buildFromStrings(sc.textFile("data-local/fimi_retail/retail.dat").map(s => s.split(" "))),
        logFile = "data-local/testSeq.csv", iterations = 10
      )

      println("\n ==== Evaluate itemset query =====")
      val factory = Option.apply(new ItemsetBuilderFactory())
      ExampleUtils.runPerformanceEval(
        patEx, minSupport,
        DesqDataset.buildFromStrings(sc.textFile("data-local/fimi_retail/retail.dat").map(s => s.split(" ")),factory),
        patExTranslator = Option.apply(new PatExToItemsetPatEx(patEx)),
        logFile = "data-local/testItemset.csv", iterations = 10
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
        DesqDataset.buildFromStrings(data.toSids, Option.apply(
          new ItemsetBuilderFactory(data.dict)
        )),
        Option.apply(new PatExToItemsetPatEx(patEx)), //"each sentence viewed as a shopping transaction"
        logFile = "data-local/logPATTYItemset.csv"
      )
    }else {
      runItemsetMiner(
        rawData = data,
        patEx = patEx,
        minSupport = minSupport
      )
    }
  }

  def sequenceOfItemsets(eval: Boolean = false,
                         dataPath: String = "data/itemset-example/data.dat",
                         dictPath: String = "data/itemset-example/dict.json",
                         logPrefix: String = "data-local/ItemsetEx_",
                         patEx: String = "[c|d] / (B)",
                         minSupport: Int = 1)(implicit sc: SparkContext): Unit = {
    //val patEx = "[c|d] / (B)"
    //val minSupport = 1

    val dict = Dictionary.loadFrom(dictPath)

    if(eval){ // Run performance evaluation (summaries + metrics in CSV)
      println("\n ==== Evaluate sequence query =====")
      val defFactory = Option.apply(new DefaultBuilderFactory(dict))
      ExampleUtils.runPerformanceEval(
        patEx, minSupport,
        DesqDataset.buildFromStrings(sc.textFile(dataPath).map(s => s.split(" ")),defFactory),
        logFile = logPrefix + "Seq.csv"
      )

      println("\n ==== Evaluate itemset query =====")
      val itemsetFactory = Option.apply(new ItemsetBuilderFactory(dict))
      ExampleUtils.runPerformanceEval(
        patEx, minSupport,
        DesqDataset.buildFromStrings(sc.textFile(dataPath).map(s => s.split(" ")),itemsetFactory ),
        Option.apply(new PatExToItemsetPatEx(patEx)),
        logFile = logPrefix + "Itemset.csv"
      )

      println("\n ==== Evaluate sequence of itemsets query =====")
      val seqItemsetFactory = Option.apply(new ItemsetBuilderFactory(dict,"/"))
      ExampleUtils.runPerformanceEval(
        patEx, minSupport,
        DesqDataset.buildFromStrings(sc.textFile(dataPath).map(s => s.split(" ")),seqItemsetFactory ),
        Option.apply(new PatExToItemsetPatEx(patEx, "/")),
        logFile = logPrefix + "SeqOfItemset.csv"
      )
    }else{ //Run the miner and show results (for functional test)
      println("\n === ItemsetExample - Itemset Query ===")
      runItemsetMiner(
        rawData = dataPath,
        patEx = patEx,
        minSupport = minSupport,
        extDict = Option.apply(dict)
      )

      println("\n === ItemsetExample - Sequence of Itemsets Query ===")
      runItemsetMiner(
        rawData = dataPath,
        patEx = patEx,
        minSupport = minSupport,
        extDict = Option.apply(dict),
        itemsetSeparator = Option.apply("/")
      )
    }
  }

  /**
    * Generic method to run Queries on ItemSet Data
    */
  def runItemsetMiner[T](
                          rawData: T,
                          patEx:String,
                          minSupport: Int = 1000,
                          extDict: Option[Dictionary] = None,
                          itemsetSeparator: Option[String] = None
                        )(implicit sc: SparkContext): (DesqMiner, DesqDataset) ={

    // Manage different data inputs (DesqDataset, FilePath, RDD)
    var data: DesqDataset = null
    val factory =
      if(extDict.isDefined && itemsetSeparator.isDefined)
        new ItemsetBuilderFactory(extDict.get, itemsetSeparator.get)
      else if (extDict.isDefined)
        new ItemsetBuilderFactory(extDict.get)
      else if (itemsetSeparator.isDefined)
        new ItemsetBuilderFactory(itemsetSeparator.get)
      else new ItemsetBuilderFactory()
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
    val itemsetPatEx =
      if (itemsetSeparator.isDefined)
        //sequence of itemsets
        new PatExToItemsetPatEx(patEx, itemsetSeparator.get).translate()
      else
        //itemset
        new PatExToItemsetPatEx(patEx).translate()

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
    confDesq.setProperty("desq.mining.optimize.permutations", false)

    //Run Miner
    val (miner, result) = ExampleUtils.runVerbose(data,confDesq)

    (miner, result)
  }


  def convertDesqDatasetToItemset(targetPath: String,
                                  sourceDataPath: String,
                                  sourceDictPath: String = "",
                                  itemsetSeparator: String = "/"
                                  )(implicit sc: SparkContext): DesqDataset = {

    //Define factory for building the itemset DesqDataset
    val factory =
      if(sourceDictPath != ""){
        new ItemsetBuilderFactory(Dictionary.loadFrom(sourceDictPath),itemsetSeparator)
      }else{
        new ItemsetBuilderFactory(itemsetSeparator)
      }

    //Convert
    println("Start conversion ... ")
    val convertedData = ExampleUtils.buildDesqDatasetFromDelFile(sc,sourceDataPath,factory)

    //Store
    println("Saving converted data to " + targetPath)
    convertedData.save(targetPath)

    //Return
    convertedData
  }

  def main(args: Array[String]) {
    //Init SparkConf
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    initDesq(conf)
    implicit val sc: SparkContext = new SparkContext(conf)
    conf.set("spark.kryoserializer.buffer.max", "1024m")

    //icdm16()
    //icdm16(compare = true)
    //evalIdcm16

    //fimi_retail()
    //fimi_retail(eval = true)

    //nyt91(eval = true)

    //sequenceOfItemsets(patEx = "[c|d] / (-/)") //on example

    /*sequenceOfItemsets(
      eval = true,
      dataPath = "data-local/fimi_retail/retail_sequences.dat",
      dictPath = "data-local/fimi_retail/dict.json",
      logPrefix = "data-local/Fimi_Seq_",
      patEx = "(-/) /{1,2} (-/)",
      minSupport = 100

    )

    convertDesqDatasetToItemset(
      "data-local/nyt_itemset/",
      "data-local/nyt/nyt-data-gid.del",
      "data-local/nyt/nyt-dict.avro.gz"
    )*/


    convertDesqDatasetToItemset(
      "data-local/amazon_itemset/",
      "data-local/amazon/amazon-data-gid.del",
      "data-local/amazon/amazon-dict.avro.gz"
    )
  }

}
