package de.uni_mannheim.desq.examples.spark

import java.io.IOException
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import de.uni_mannheim.desq.dictionary.{BuilderFactory, Dictionary, DictionaryBuilder, ItemsetBuilderFactory}
import de.uni_mannheim.desq.experiments.MetricLogger
import de.uni_mannheim.desq.experiments.MetricLogger.Metric
import de.uni_mannheim.desq.io.DelSequenceReader
import de.uni_mannheim.desq.mining.{Sequence, WeightedSequence}
import de.uni_mannheim.desq.mining.spark.DesqDataset.build
import de.uni_mannheim.desq.mining.spark.{DesqCount, DesqDataset, DesqMiner, DesqMinerContext}
import de.uni_mannheim.desq.patex.{PatExToItemsetPatEx, PatExToSequentialPatEx, PatExTranslator}
import de.uni_mannheim.desq.util.DesqProperties
import it.unimi.dsi.fastutil.ints.IntArrayList
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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

    //Convert to (sequence of itemsets)?
    if(asItemset){
      //Convert data + dict
      val factory = new ItemsetBuilderFactory(data.dict)
      data = DesqDataset.buildFromStrings(data.toSids, Option.apply(factory))
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
    data.properties.prettyPrint()

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

  /** Runs a miner on given data and logs metrics via MetricLogger (in scala)
    * For java, see java.[...].ExampleUtils -> runItemsetPerfEval */
  def runPerformanceEval(patEx: String, minSupport: Long, inputData: DesqDataset,
                         patExTranslator: Option[PatExTranslator[String]] = None,
                         logFile: String = "", iterations: Int = 2)
                        (implicit sc: SparkContext): Unit = {

    MetricLogger.reset() //Force re-init of logger
    val log = MetricLogger.getInstance()

    for(i <- 0 until iterations) {
      println("\n == Iteration " + i + " ==" )
      log.startIteration()
      //unpersist sequences to ensure data load is executed again
      inputData.sequences.unpersist()
      val cachedSequences = inputData.sequences
      cachedSequences.cache()

      // ------- Actual Processing  -------------
      log.start(Metric.TotalRuntime)

      //Convert Data?
      print("Loading data (" + inputData.properties.getString("desq.dataset.builder.factory.class","No Builder") + ") ... ")
      log.start(Metric.DataLoadRuntime)
      //cache data and execute count to force data load
      val count = cachedSequences.count
      println(log.stop(Metric.DataLoadRuntime))


      //Calculating some KPIs (impact on total runtime only)
      println("#Dictionary entries: %s".format(log.add(Metric.NumberDictionaryItems, inputData.dict.size().toLong)))


      println("#Input sequences: "
        + log.add(Metric.NumberInputSequences, count))

      val sum = cachedSequences.map(a => a.size).reduce((a, b) => a + b)
      println("Avg length of input sequences: "
        + log.add(Metric.AvgLengthInputSequences, sum/count))

      //Convert PatEx
      print("Converting PatEx ( " + patExTranslator.isDefined + " )... ")
      log.start(Metric.PatExTransformationRuntime)

      System.out.print( patEx )

      val convPatEx = if(patExTranslator.isDefined){
        val tempPatEx  = patExTranslator.get.translate()
        print("  ->  " + tempPatEx)
        //new PatExToSequentialPatEx(itemsetPatEx).translate()
        tempPatEx
      }else{
        //new PatExToSequentialPatEx(patEx).translate()
        patEx
      }
      println(" ... " + log.stop(Metric.PatExTransformationRuntime))

      // -------- Execute mining ------------ (based on runVerbose and runMiner)
      val minerConf = DesqCount.createConf(convPatEx, minSupport)
      minerConf.setProperty("desq.mining.prune.irrelevant.inputs", true)
      minerConf.setProperty("desq.mining.use.two.pass", false)
      minerConf.setProperty("desq.mining.optimize.permutations", true)
      val ctx = new DesqMinerContext(minerConf)
      val miner = DesqMiner.create(ctx)

      print("Mining (RDD construction)... ")
      //log.start(Metric.RDDConstructionRuntime)
      val result = miner.mine(inputData)
      //println(log.stop(Metric.RDDConstructionRuntime))

      println("Mining (persist)... ") //entails FST generation as well
      log.start(Metric.MiningRuntime)
      val cachedResult = result.sequences.cache()


      if (cachedResult.count() > 0) { //isEmpty causes spark warning "block locks were not released by TID" because of take(1)
        val (count, support) = cachedResult.map(ws => (1, ws.weight)).reduce((cs1, cs2) => (cs1._1 + cs2._1, cs1._2 + cs2._2))

        println("Number of patterns: " + count)
        println("Frequency of all patterns: " + support)
      }else{
        println("No results!")
      }
      println("MiningRuntime: " + log.stop(Metric.MiningRuntime))
      println("TotalRuntime: " + log.stop(Metric.TotalRuntime))

      cachedSequences.unpersist()
      cachedResult.unpersist()

    }
    if(logFile != ""){
      log.writeToFile(logFile)
    }
  }

  //Helper Methods (for use in Java)

  def buildDesqDatasetFromRawFile(sc: SparkContext,
                                  dataPath: String,
                                  factory: BuilderFactory): DesqDataset ={
    //Init SparkConf
    /*val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    initDesq(conf)
    val sc:SparkContext = SparkContext.getOrCreate(conf)*/
    DesqDataset.buildFromStrings(
      sc.textFile(dataPath).map(s => s.split(" ")),
      Option.apply(factory)
    ).copyWithRecomputedCountsAndFids()
  }

  def buildDesqDatasetFromDelFile( sc: SparkContext,
                                   delFile: String,
                                   dict: Dictionary,
                                   factory: BuilderFactory): DesqDataset ={
    val file = sc.textFile(delFile) //-> RDD[String]

    val parse = (line: String, seqBuilder: DictionaryBuilder) => {
      seqBuilder.newSequence(1)
      val tokens = line.split("[\t ]")
      for (token <- tokens) {
        if (!token.isEmpty)
          seqBuilder.appendItem(token.toInt)
        }


      //val s = new IntArrayList()
      //DelSequenceReader.parseLine(line, s)
      //dict.sidsOfGids(s).forEach(sid => seqBuilder.appendItem(sid))

      //s.forEach(gid => seqBuilder.appendItem(gid))
    }
    build[String](file, parse, Option.apply(factory)).copyWithRecomputedCountsAndFids()
  }
}
