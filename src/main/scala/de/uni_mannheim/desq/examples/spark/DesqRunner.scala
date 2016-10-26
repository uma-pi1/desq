package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.Desq
import de.uni_mannheim.desq.mining.spark.{DesqDfs, DesqCount}
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.spark.{DesqDataset, DesqMiner, DesqMinerContext}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.log4j.{Logger, LogManager}
import scala.collection.JavaConversions._
import scala.io.Source
import java.util.Calendar

/**
  * Other way to run DesqDfs
  * Created by alexrenz on 08.10.2016.
  */
object DesqRunner {
  
  def main(args: Array[String]) {
    if(args.length < 5) {
      println("Usage: DesqRunner miner inputPath outputPath minSupport patternExpression numPartitions [skipNonPivotPartitions useMinMaxPivot]")
      System.exit(0)
    }
    val logger = LogManager.getLogger("DesqRunner")
    
    // Init Desq, build SparkContext
    val sparkConf = new SparkConf().setAppName(getClass.getName)
    Desq.initDesq(sparkConf)
    implicit val sc = new SparkContext(sparkConf)

    println("#####################")
    println(sparkConf.toDebugString)
    println("#####################")

    val path = args(1)
    val data = DesqDataset.load(path)
    
    //println("\n\nDictionary with frequencies:")
    //data.dict.writeJson(System.out)
    
    //println("\n\nInput sequences as letters:")
    //data.print()
    
    val sigma = args(3).toInt
    val patternExpression = args(4)

    
    // Build miner conf
    var minerConf = DesqDfs.createConf(patternExpression, sigma)
    if(args(0).equals("DesqCount")) {
      minerConf = DesqCount.createConf(patternExpression, sigma)
    }
    minerConf.setProperty("desq.mining.prune.irrelevant.inputs", "false")
    minerConf.setProperty("desq.mining.use.two.pass", "false")
    minerConf.setProperty("desq.mining.use.flist", "true")
    minerConf.setProperty("desq.mining.skip.non.pivot.transitions", args(5))
    minerConf.setProperty("desq.mining.use.minmax.pivot", args(6))
    //minerConf.setProperty("desq.mining.skip.non.pivot.transitions", "false")
    //minerConf.setProperty("desq.mining.use.minmax.pivot", "false")

    //minerConf.setProperty("desq.mining.num.mine.partitions", args(5).toInt)
    
    // Build miner
    val ctx = new DesqMinerContext(minerConf)
    println("Miner properties: ")
    ctx.conf.prettyPrint()
    val miner = DesqMiner.create(ctx)
    
    // Mine
    var t1 = System.nanoTime // we are not using the Guava stopwatch here due to the packaging conflicts inside Spark (Guava 14)
    print("Mining (RDD construction)... ")
    val result = miner.mine(data)
    val cal = Calendar.getInstance()
    result.sequences.saveAsTextFile(args(2)+cal.get(Calendar.HOUR_OF_DAY)+cal.get(Calendar.MINUTE)+cal.get(Calendar.SECOND));
    //result.toSidsSupportPairs().saveAsTextFile(args(2))
    val mineAndOutputTime = (System.nanoTime - t1) / 1e9d
    logger.fatal("mineAndOutputTime: " + mineAndOutputTime + "s")
  }
}
