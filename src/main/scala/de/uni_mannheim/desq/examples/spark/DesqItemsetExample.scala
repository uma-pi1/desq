package de.uni_mannheim.desq.examples.spark

import java.util

import de.uni_mannheim.desq.Desq._
import de.uni_mannheim.desq.mining.spark.{DesqCount, DesqDataset, DesqMiner}
import de.uni_mannheim.desq.dictionary.Dictionary
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sulbrich on 18.09.2017
  */
object DesqItemsetExample {
  /**
    * Prototype of DESQ API for Itemset Mining
    */
  def runItemsetMiner[T](
                          rawData: T,
                          query:String = "[.*(.^)]{2,3}",
                          minSupport: Int = 1000,
                          extDict: Option[Dictionary]
                        )(implicit sc: SparkContext): (DesqMiner, DesqDataset) ={

    // Manage data
    var data: DesqDataset = null
    rawData match{
      case dds: DesqDataset => //Use existing DesqDataset as basis
        data = DesqDataset.buildItemsets(dds)
      case file: String => //Read from space delimited file
        data = DesqDataset.buildItemsets(sc.textFile(file).map(s => s.split(" ")), extDict = extDict)
      case rdd: RDD[Array[Any]] =>
        data = DesqDataset.buildItemsets(rdd,extDict = extDict)
      case _ =>
        println("ERROR: Unsupported input type")
        return (null,null)
    }
    println("\nDictionary:")
    data.dict.writeJson(System.out);
    println("\nSeparatorGid: " + data.itemsetSeparatorGid + "(" + data.getCfreqOfSeparator() + ")")

    println("\nFirst 10 Input Sequences:")
    data.print(10);

    println("\nInit/Run Miner for query: " + query);
    // Init Desq Miner
    val confDesq = DesqCount.createConf(query, minSupport)
    confDesq.setProperty("desq.mining.prune.irrelevant.inputs", true)
    confDesq.setProperty("desq.mining.is.itemset", data.containsItemsets());


    //Run Miner
    val (miner, result) = ExampleUtils.runVerbose(data,confDesq)

    (miner, result)
  }



  def main(args: Array[String]) {
    //Init SparkConf
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    initDesq(conf)
    implicit val sc:SparkContext = new SparkContext(conf)

    //val data = DesqDataset.buildFromStrings(sc.textFile("data-local/fimi_retail/retail.dat").map(s => s.split(" ")))
    //val data = "data-local/fimi_retail/retail.dat" //"data-local/nyt-1991-data"
    //val data = sc.textFile("data/icdm16-example/data.del").map(s => s.split("\t"))

    //val dict = Option.apply(Dictionary.loadFrom("data-local/fimi_retail/dict.json",sc))

    //icdm16-example
    //val dict = Option.apply(Dictionary.loadFrom("data/icdm16-example/dict.json",sc))
    //val data = DesqDataset.loadFromDelFile(delFile = "data/icdm16-example/data.del", dict = dict.get)

    //itemset-example
    val dict = Option.apply(Dictionary.loadFrom("data/itemset-example/dict.json",sc))
    val data = "data/itemset-example/data.dat"


    runItemsetMiner(
      rawData =     data,
      //query =       "unordered{(A* b11)}", //"[.*(.)]{2,3}" "[(a1).*-.*(A^|B)]" "unordered{(a1 b11)}" "unordered{(a1 [b12 | b11])}" "unordered{(a1 [ b1= | b11 b12])}"
      query =       "c <[d|e] <(A)* (B) >>", // "unordered{(A+)}" "unordered{(A* b11)}" "(A)*.*(b11).*(A)*.*" "[[(A).*]*]*.* (b11).* [[(A).*]*]*.*" "[(A).*]* (b11).* [(A).*]*"
      minSupport =  1,
      extDict =     dict)
  }
}
