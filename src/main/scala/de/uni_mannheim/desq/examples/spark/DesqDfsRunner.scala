package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.mining.spark.DesqDfs
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.spark.{DesqDataset, DesqMiner, DesqMinerContext}
import org.apache.spark.{SparkConf, SparkContext}



import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Other way to run DesqDfs
  * Created by alexrenz on 08.10.2016.
  */
object DesqDfsRunner {
  
  def main(args: Array[String]) {
    
    val sparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    implicit val sc = new SparkContext(sparkConf)
    
    //val patternExpression = "[c|d]([A^|B=^]+)e"
    //val patternExpression = "(.)"
    //val sigma = 2
    //val conf = DesqDfs.createConf(patternExpression, sigma)
    //conf.setProperty("desq.mining.prune.irrelevant.inputs", "true")
    //conf.setProperty("desq.mining.use.two.pass", "true")
    
    
    // Load data
    //val data = DesqDataset.load("data/icdm16-saved")
    //println("Data")
    //data.print(5)
    
    //println("Dictionary")
    //data.dict.writeJson(System.out)
    
    val dictFile = this.getClass.getResource("/icdm16-example/dict.json")
    val dataFile = this.getClass.getResource("/icdm16-example/data.del")
  
    // load the dictionary & update hierarchy
    val dict = Dictionary.loadFrom(dictFile)
    val delFile = sc.parallelize(Source.fromURL(dataFile).getLines.toSeq)
    val data = DesqDataset.loadFromDelFile(delFile, dict, usesFids = false).copyWithRecomputedCountsAndFids()
    println("\nDictionary with frequencies:")
    dict.writeJson(System.out)
    println()
  
    // print sequences
    System.out.println("\nInput sequences as letters:")
    data.print()
    println()
    
   
    // Build conf 
    var patternExpression = "[c|d]([A^|B=^]+)e"
    var sigma = 2
    
    println("args.length="+args.length) 
    
    if(args.length >=2) {
      println("Using argument pattern expression and sigma")
      sigma = args(0).toInt
      patternExpression = args(1)
    }
    
    //val patternExpression = "(.)"
    
    val minerConf = DesqDfs.createConf(patternExpression, sigma)
    minerConf.setProperty("desq.mining.prune.irrelevant.inputs", "true")
    minerConf.setProperty("desq.mining.use.two.pass", "true")
    
    // Build miner
    val ctx = new DesqMinerContext(minerConf)
    println("Miner properties: ")
    ctx.conf.prettyPrint()
    val miner = DesqMiner.create(ctx)
    
    // Mine and output
    print("Mining (RDD construction)... ")
    val result = miner.mine(data)
    System.out.println("\nPatterns:")
    result.print()
    

  }
}
