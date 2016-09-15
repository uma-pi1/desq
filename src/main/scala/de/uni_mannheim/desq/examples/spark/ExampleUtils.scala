package de.uni_mannheim.desq.examples.spark

import java.io.IOException
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.spark.{DesqDataset, DesqMiner, DesqMinerContext}
import de.uni_mannheim.desq.util.DesqProperties
import org.apache.commons.configuration2.ConfigurationConverter
import org.apache.spark.SparkContext

import scala.io.Source

/**
  * Created by rgemulla on 14.09.2016.
  */
object ExampleUtils {
  def runMiner(data: DesqDataset, ctx: DesqMinerContext): (DesqMiner, DesqDataset) = {
    print("Miner properties: ")
    println(ConfigurationConverter.getProperties(ctx.conf))

    print("Creating miner... ")
    val prepTime = Stopwatch.createStarted
    val miner = DesqMiner.create(ctx)
    prepTime.stop
    println(prepTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    System.out.print("Mining... ")
    val miningTime: Stopwatch = Stopwatch.createStarted
    val result = miner.mine(data)
    miningTime.stop
    System.out.println(miningTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    System.out.println("Total time: " + (prepTime.elapsed(TimeUnit.MILLISECONDS)
      + miningTime.elapsed(TimeUnit.MILLISECONDS)) + "ms")

    (miner, result)
  }

  def runVerbose(data: DesqDataset, minerConf: DesqProperties): (DesqMiner, DesqDataset) = {
    // create context
    val ctx = new DesqMinerContext(minerConf)
    val (miner, result) = runMiner(data, ctx)

    System.out.println("\nPatterns:")
    result.toSidRDD().map(s => (s._1.deep.mkString(" "), s._2)).collect.foreach(println)

    (miner, result)
  }

  /** Runs a miner on ICDM16 example data. */
  @throws[IOException]
  def runIcdm16(implicit sc: SparkContext, minerConf: DesqProperties): (DesqMiner, DesqDataset) = {
    val dictFile = this.getClass.getResource("/icdm16-example/dict.json")
    val dataFile = this.getClass.getResource("/icdm16-example/data.del")

    // load the dictionary & update hierarchy
    val dict = Dictionary.loadFrom(dictFile)
    val delFile = sc.parallelize(Source.fromURL(dataFile).getLines.toSeq)
    val data = DesqDataset.fromDelFile(delFile, dict, false)
    data.recomputeDictionaryCountsAndFids()
    println("\nDictionary with frequencies:")
    data.recomputeDictionaryCountsAndFids // updates contained dict = our dict
    dict.writeJson(System.out)
    println

    // print sequences
    System.out.println("\nInput sequences:")
    data.toSidRDD().map(s => (s._1.deep.mkString(" "), s._2)).collect.foreach(println)
    println

    runVerbose(data, minerConf)
  }
}
