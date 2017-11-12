package de.uni_mannheim.desq.examples.spark

import java.io.IOException
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.spark.{DesqCount, DesqDataset, DesqMiner, DesqMinerContext}
import de.uni_mannheim.desq.patex.PatExToItemsetPatEx
import de.uni_mannheim.desq.patex.PatExToPatEx
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
      println("Total frequency of all patterns: " + support)
    }else{
      println("No results!")
    }
    println("Persist time: " + persistTime.elapsed(TimeUnit.MILLISECONDS) + "ms")
    println("Total time: " + (prepTime.elapsed(TimeUnit.MILLISECONDS)
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
      data = DesqDataset.buildItemsets(data)
      dict = data.dict

      //Convert PatEx
      val patEx = minerConf.getString("desq.mining.pattern.expression")
      val itemsetPatEx = new PatExToItemsetPatEx(dict,patEx).translate()
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

  def runPerformanceEval(minerConf: DesqProperties, inputData: DesqDataset, inputDict: Dictionary, asItemset: Boolean = false)
                        (implicit sc: SparkContext): (DesqMiner, DesqDataset) = {
    //Calculating some KPIs before starting measurements
    println("\n#Dictionary entries: " + inputDict.size())
    println("\n#Input sequences: " + inputData.sequences.count())
    val sum = inputData.sequences.map(a => a.size).reduce((a, b) => a + b)

    println("\nAvg length of input sequences: " + sum/inputData.sequences.count())

    // ------- Actual Processing  -------------
    val totalTime = Stopwatch.createStarted

    //Convert Data?
    print("Converting data ( " + asItemset.toString + " )... ")
    val dataTransformationTime = Stopwatch.createStarted
    val data = if(asItemset) DesqDataset.buildItemsets(inputData, inputDict) else inputData
    val dict = if(asItemset) data.dict else inputDict
    dataTransformationTime.stop
    println(dataTransformationTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    //Convert PatEx?
    print("Converting PatEx ( " + asItemset.toString + " )... ")
    val patExTransformationTime = Stopwatch.createStarted
    if(asItemset){
      val patEx = minerConf.getString("desq.mining.pattern.expression")
      val itemsetPatEx = new PatExToItemsetPatEx(dict,patEx).translate()
      minerConf.setProperty("desq.mining.pattern.expression", itemsetPatEx)
      System.out.print( patEx + "  ->  " + itemsetPatEx + " ")
    }
    patExTransformationTime.stop
    println(patExTransformationTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    // -------- Execute mining ------------ (based on runVerbose and runMiner)
    val ctx = new DesqMinerContext(minerConf)
    val miner = DesqMiner.create(ctx)

    print("Mining (RDD construction)... ")
    val miningTime = Stopwatch.createStarted
    val result = miner.mine(data)
    miningTime.stop
    println(miningTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    println("Mining (persist)... ") //entails FST generation as well!!!!
    val persistTime = Stopwatch.createStarted
    result.sequences.cache()


    if (result.sequences.count() > 0) { //isEmpty causes spark warning "block locks were not released by TID" because of take(1)
      val (count, support) = result.sequences.map(ws => (1, ws.weight)).reduce((cs1, cs2) => (cs1._1 + cs2._1, cs1._2 + cs2._2))

      println("Number of patterns: " + count)
      println("Total frequency of all patterns: " + support)
    }else{
      println("No results!")
    }
    persistTime.stop
    println("Persist time: " + persistTime.elapsed(TimeUnit.MILLISECONDS) + "ms")
    totalTime.stop
    println("Total time: " + totalTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    result.sequences.unpersist()
    (miner, result)
  }
}
