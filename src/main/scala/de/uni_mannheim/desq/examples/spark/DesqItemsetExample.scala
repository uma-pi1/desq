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

    // --- Manage data
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

    val confDesq = DesqCount.createConf(query, minSupport)
    confDesq.setProperty("desq.mining.prune.irrelevant.inputs", true)
    confDesq.setProperty("desq.mining.use.two.pass", true)



    //Run Miner
    val (miner, result) = ExampleUtils.runVerbose(data,confDesq)


    //Print some information
    println("\nSeparatorGid: " + data.itemsetSeparatorGid + "(" + data.getCfreqOfSeparator() + ")")

    println("Relevant Dictionary Entries:")
    var sids_unique:List[String] = List()
    for (sids <- result.toSids.collect().toIterable) {
      for (sid <- sids) {
        if (!sids_unique.contains(sid)) {
          sids_unique = sids_unique.::(sid)
        }
      }
    }
    for(sid <- sids_unique){
      println(data.dict.toJson(data.dict.fidOf(sid)))
    }

    (miner, result)
  }



  def main(args: Array[String]) {
    //Init SparkConf
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    initDesq(conf)
    implicit val sc:SparkContext = new SparkContext(conf)

    //val data = DesqDataset.buildFromStrings(sc.textFile("data-local/fimi_retail/retail.dat").map(s => s.split(" ")))
    //val data = "data-local/fimi_retail/retail.dat" //"data-local/nyt-1991-data"
    val data = sc.textFile("data/icdm16-example/data.del").map(s => s.split("\t"))

    //val dict = Option.apply(Dictionary.loadFrom("data-local/fimi_retail/dict.json",sc))
    val dict = Option.apply(Dictionary.loadFrom("data/icdm16-example/dict.json",sc))


    runItemsetMiner(
      rawData =     data,
      query =       "[.*(.)]{2,3}",
      minSupport =  2,
      extDict =     dict)
  }
}
