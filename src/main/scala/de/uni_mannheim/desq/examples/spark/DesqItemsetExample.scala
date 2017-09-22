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
            //              itemDef:String = ".",
                          generalize: Boolean = true,
                          minSupport: Double = 0.1,
                          size: String = "2,",
                          extDict: Option[Dictionary]
                        )(implicit sc: SparkContext): (DesqMiner, DesqDataset) ={

    // --- Manage data
    var data: DesqDataset = null
    rawData match{
      case dds: DesqDataset => //Use existing DesqDataset as basis
        data = DesqDataset.buildItemsets(dds)
      case file: String => //Read from delimited file
        data = DesqDataset.buildItemsets(sc.textFile(file).map(s => s.split(" ")),extDict)
      case rdd: RDD[Array[Any]] =>
        data = DesqDataset.buildItemsets(rdd,extDict)
      case _ =>
        println("ERROR: Unsupported input type")
        return (null,null)
    }

    // --- Init Desq for Itemset Mining
    val itemDef = if (generalize) ".^" else "."
    val patternExpression = "[.*(" + itemDef + ")]{" + size + "}"
    // .* - arbitrary distance between items (= distance in itemsets is irrelevant)
    // (.) - captured item
    // [...]{n,} at least n-times (= n-tuple)
    // ^ - generalized output for matched item
    val sigma = (minSupport * data.sequences.count()).toLong //ToDo: calculation running already? -> persist?
    val confDesq = DesqCount.createConf(patternExpression, sigma)
    confDesq.setProperty("desq.mining.prune.irrelevant.inputs", true)
    confDesq.setProperty("desq.mining.use.two.pass", true)

    //Run Miner
    val (miner, result) = ExampleUtils.runVerbose(data,confDesq)

    //Print some dictionary data
    var sids_unique:List[String] = List()
    for (sids <- result.toSids.collect().toIterable) {
      for (sid <- sids) {
        if (!sids_unique.contains(sid)) {
          sids_unique = sids_unique.::(sid)
        }
      }
    }
    println("\nRelevant Dictionary Entries:")
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
    val data = "data-local/fimi_retail/retail.dat" //"data-local/nyt-1991-data"
    //val data = 1

    val dict = Option.apply(Dictionary.loadFrom("data-local/fimi_retail/dict.json",sc))


    runItemsetMiner(
      rawData =     data,
      generalize =  true,
      minSupport =  0.05,
      size    =     "2,3",
      extDict =     dict)
  }
}
