package de.uni_mannheim.desq.examples.spark

import java.io.IOException
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import de.uni_mannheim.desq.dictionary.{Dictionary, ItemsetBuilderFactory}
import de.uni_mannheim.desq.experiments.MetricLogger
import de.uni_mannheim.desq.experiments.MetricLogger.Metric
import de.uni_mannheim.desq.mining.spark.{DesqCount, DesqDataset, DesqMiner, DesqMinerContext}
import de.uni_mannheim.desq.patex.{PatExToItemsetPatEx, PatExToSequentialPatEx}
import de.uni_mannheim.desq.util.DesqProperties
import org.apache.spark.SparkContext

import scala.io.Source

/**
  * Created by rgemulla on 14.09.2016.
  */
object ExampleUtils {
  /**
    * Creates a miner, runs it on the given data, and prints running times as well as pattern statistics.
    * Result sequences are cached in memory.
    */
  def runMiner(data: DesqDataset, ctx: DesqMinerContext): (DesqMiner, DesqDataset) = {
    println("Miner properties: ")
    ctx.conf.prettyPrint()

    print("Creating miner... ")
    val prepTime = Stopwatch.createStarted
    val miner = DesqMiner.create(ctx)
    prepTime.stop
    println(prepTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    print("Mining (RDD construction)... ")
    val miningTime = Stopwatch.createStarted
    val result = miner.mine(data)
    miningTime.stop
    println(miningTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    println("Mining (persist)... ")
    val persistTime = Stopwatch.createStarted
    result.sequences.cache()

    if (result.sequences.count() > 0) { //isEmpty causes spark warning "block locks were not released by TID" because of take(1)
      val (count, support) = result.sequences.map(ws => (1, ws.weight)).reduce((cs1, cs2) => (cs1._1 + cs2._1, cs1._2 + cs2._2))
      persistTime.stop
      println("Number of patterns: " + count)
      println("TotalRuntime frequency of all patterns: " + support)
    }else{
      println("No results!")
    }
    println("PersistRuntime time: " + persistTime.elapsed(TimeUnit.MILLISECONDS) + "ms")
    println("TotalRuntime time: " + (prepTime.elapsed(TimeUnit.MILLISECONDS)
      + miningTime.elapsed(TimeUnit.MILLISECONDS) + persistTime.elapsed(TimeUnit.MILLISECONDS)) + "ms")


    (miner, result)
  }

  /**
    * Creates a miner, runs it on the given data, and prints running times as well as pattern statistics.
    * Result sequences are cached in memory.
    */
  @throws[IOException]
  def runMiner(data: DesqDataset, minerConf: DesqProperties): (DesqMiner, DesqDataset) = {
    val ctx = new DesqMinerContext(minerConf)
    runMiner(data, ctx)
  }

  /**
    * Creates a miner, runs it on the given data, and prints running times as well as all mined patterns.
    */
  def runVerbose(data: DesqDataset, minerConf: DesqProperties): (DesqMiner, DesqDataset) = {
    val (miner, result) = runMiner(data, minerConf)

    System.out.println("\nPatterns:")
    result.print()

    (miner, result)
  }

  /** Runs a miner on ICDM16 example data. */
  @throws[IOException]
  def runIcdm16(minerConf: DesqProperties, asItemset: Boolean = false)(implicit sc: SparkContext): (DesqMiner, DesqDataset) = {
    val dictFile = this.getClass.getResource("/icdm16-example/dict.json")
    val dataFile = this.getClass.getResource("/icdm16-example/data.del")

    // load the dictionary & update hierarchy
    var dict = Dictionary.loadFrom(dictFile)
    val delFile = sc.parallelize(Source.fromURL(dataFile).getLines.toSeq)
    var data = DesqDataset.loadFromDelFile(delFile, dict, usesFids = false).copyWithRecomputedCountsAndFids()

    //Convert to Itemsets?
    if(asItemset){
      //Convert data + dict
      data = DesqDataset.buildFromStrings(data.toSids,Option.apply(data.dict), Option.apply(new ItemsetBuilderFactory()))
      dict = data.dict

      //Convert PatEx
      val patEx = minerConf.getString("desq.mining.pattern.expression")
      val itemsetPatEx = new PatExToItemsetPatEx(patEx).translate()
      minerConf.setProperty("desq.mining.pattern.expression", itemsetPatEx)
      System.out.println("\nConverted PatEx: " + patEx +"  ->  " + itemsetPatEx)
    }

    println("\nDictionary with frequencies:")
    dict.writeJson(System.out)
    println()

    // print sequences
    System.out.println("\nInput sequences:")
    data.print()
    println()

    System.out.println("\nDataset properties:")
    data.context.prettyPrint()

    val (miner, result) = runVerbose(data, minerConf)
    result.sequences.unpersist()
    (miner, result)
  }


  /** Runs a miner on NYT example data. */
  @throws[IOException]
  def runNyt(minerConf: DesqProperties, verbose: Boolean = false)(implicit sc: SparkContext): (DesqMiner, DesqDataset) = {
    val dict: Dictionary = Dictionary.loadFrom("data-local/nyt-1991-dict.avro.gz")
    val delFilename = "data-local/nyt-1991-data.del"
    val delFile = sc.textFile(delFilename)
    val data = DesqDataset.loadFromDelFile(delFile, dict, usesFids = true)
    if (verbose)
      runMiner(data, minerConf)
    else
      runVerbose(data, minerConf)
  }

  /** Runs a miner on given data and logs metrics via MetricLogger*/
  def runPerformanceEval(patEx: String, minSupport: Long, inputData: DesqDataset,
                         asItemset: Boolean = false, logFile: String = "", iterations: Int = 2)
                        (implicit sc: SparkContext): Unit = {

    MetricLogger.reset() //Force re-init of logger
    val log = MetricLogger.getInstance()

    for(i <- 0 until iterations) {
      println("\n == Iteration " + i + " ==" )
      log.startIteration()

      //copy data to prevent interference between iterations
      var data = inputData.copy()

      //Calculating some KPIs before starting measurements
      println("#Dictionary entries: %s".format(log.add(Metric.NumberDictionaryItems, data.dict.size().toLong)))

      println("#Input sequences: "
        + log.add(Metric.NumberInputSequences, data.sequences.count()))

      val sum = data.sequences.map(a => a.size).reduce((a, b) => a + b)
      println("Avg length of input sequences: "
        + log.add(Metric.AvgLengthInputSequences, sum/data.sequences.count()))

      // ------- Actual Processing  -------------
      log.start(Metric.TotalRuntime)

        //Convert Data?
      print("Converting data ( " + asItemset.toString + " )... ")
      log.start(Metric.DataTransformationRuntime)
      //val data =
      if(asItemset) data = DesqDataset.buildFromStrings(data.toSids,Option.apply(data.dict),Option.apply(new ItemsetBuilderFactory()))
      println(log.stop(Metric.DataTransformationRuntime))
      //if(asItemset) println("Avg length of itemsets: " + (data.sequences.map(a => a.size).reduce((a, b) => a + b)/data.sequences.count()))

      //Convert PatEx
      print("Converting PatEx ( " + asItemset.toString + " )... ")
      log.start(Metric.PatExTransformationRuntime)

      System.out.print( patEx )

      val convPatEx = if(asItemset){
        val itemsetPatEx  = new PatExToItemsetPatEx(patEx).translate()
        print("  ->  " + itemsetPatEx)
        //new PatExToSequentialPatEx(itemsetPatEx).translate()
        itemsetPatEx
      }else{
        //new PatExToSequentialPatEx(patEx).translate()
        patEx
      }
      //print("  ->  " + convPatEx + " ")
      println(" ... " + log.stop(Metric.PatExTransformationRuntime))

      // -------- Execute mining ------------ (based on runVerbose and runMiner)
      val minerConf = DesqCount.createConf(convPatEx, minSupport)
      val ctx = new DesqMinerContext(minerConf)
      val miner = DesqMiner.create(ctx)

      print("Mining (RDD construction)... ")
      log.start(Metric.RDDConstructionRuntime)
      val result = miner.mine(data)
      println(log.stop(Metric.RDDConstructionRuntime))

      println("Mining (persist)... ") //entails FST generation as well!!!!
      log.start(Metric.PersistRuntime)
      result.sequences.cache()


      if (result.sequences.count() > 0) { //isEmpty causes spark warning "block locks were not released by TID" because of take(1)
        val (count, support) = result.sequences.map(ws => (1, ws.weight)).reduce((cs1, cs2) => (cs1._1 + cs2._1, cs1._2 + cs2._2))

        println("Number of patterns: " + count)
        println("Frequency of all patterns: " + support)
      }else{
        println("No results!")
      }
      println("PersistRuntime: " + log.stop(Metric.PersistRuntime))
      println("TotalRuntime: " + log.stop(Metric.TotalRuntime))
      if(logFile != ""){
        log.writeResult(logFile)
      }
      result.sequences.unpersist()
    }
  }
}
