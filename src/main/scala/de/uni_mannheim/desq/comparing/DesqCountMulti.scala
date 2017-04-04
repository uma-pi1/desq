package de.uni_mannheim.desq.comparing

import java.io.File
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch

import scala.collection.JavaConverters._
import de.uni_mannheim.desq.mining.spark.{DesqCount, DesqDataset, DesqMiner, DesqMinerContext}
import de.uni_mannheim.desq.Desq._
import de.uni_mannheim.desq.converters.nyt.ConvertNyt
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.WeightedSequence
import de.uni_mannheim.desq.util.DesqProperties
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by ivo on 28.03.17.
  */
object DesqCountMulti {
  def run()(implicit sc: SparkContext): mutable.Buffer[(String, DesqMiner, DesqDataset)] = {
//    val confSpark = new SparkConf().setAppName(getClass.getName).setMaster("local")
//    initDesq(confSpark)
//    implicit val sc = new SparkContext(confSpark)
    val patternExpression = "(.){3,5}"
    val sigma = 10;

    val confDesq = DesqCount.createConf(patternExpression, sigma)
    confDesq.setProperty("desq.mining.prune.irrelevant.inputs", true)
    confDesq.setProperty("desq.mining.use.two.pass", true)

    val rootDir = "data-local/processed/results/"
    val leafDir = ConvertNyt.getLeafSubdirs(new File(rootDir))
    val dataSets = for (dir: File <- leafDir.asScala if dir.getName.contains("left") || dir.getName.contains("right")) yield {
      val dict: Dictionary = Dictionary.loadFrom(dir.getAbsolutePath + "/dict.avro.gz")
      val delFilename = dir.getAbsolutePath + "/fid.del"
      val delFile = sc.textFile(delFilename)
      val data = DesqDataset.loadFromDelFile(delFile, dict, usesFids = true)
      (dir.getName, data)
    }
    val miningResults = for (tuple <- dataSets) yield {
      val (miner, result) = mine(confDesq, tuple._2)
      (tuple._1, miner, result)
    }
    miningResults
  }

  def mine(conf: DesqProperties, data: DesqDataset): (DesqMiner, DesqDataset) = {
    val ctx = new DesqMinerContext(conf)
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

    print("Mining (persist)... ")
    val persistTime = Stopwatch.createStarted
    result.sequences.cache()
    val (count, support) = result.sequences.map(ws => (1, ws.weight)).reduce((cs1, cs2) => (cs1._1 + cs2._1, cs1._2 + cs2._2))
    persistTime.stop
    println(persistTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    println("Total time: " + (prepTime.elapsed(TimeUnit.MILLISECONDS)
      + miningTime.elapsed(TimeUnit.MILLISECONDS) + persistTime.elapsed(TimeUnit.MILLISECONDS)) + "ms")

    println("Number of patterns: " + count)
    println("Total frequency of all patterns: " + support)

    (miner, result)
  }
}
