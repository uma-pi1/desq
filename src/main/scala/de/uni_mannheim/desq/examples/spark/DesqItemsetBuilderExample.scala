package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.Desq._
import de.uni_mannheim.desq.mining.spark.DesqDataset
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Testing of Itemset API
  * Created by sulbrich on 13.09.2017
  * Copy of DesqBuilderExample by rgemulla
  */
object DesqItemsetBuilderExample extends App {
  System.setProperty("hadoop.home.dir", "C:\\Program Files (x86)\\Hadoop")
  val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
  initDesq(conf)
  implicit val sc:SparkContext = new SparkContext(conf)

  /*
  //create source
  val lines = sc.textFile("data-local/small.del")
  val sourceData = DesqDataset.buildFromStrings(lines.map(s => s.split(" ")))
  sourceData.print(5)
  // save it
  sourceData.save("data-local/small")
  println("--")
  */

  //val sourceDataset = DesqDataset.load("data-local/nyt-1991-data") //data-local/small
  val sourceDataset = sc.textFile("data-local/fimi_retail/retail.dat").map(s => s.split(" "))
  val data = DesqDataset.buildItemsets(sourceDataset)
  data.print(5)
  //print fids for first example
  for (sid <- data.toSidsWeightPairs().first()._1){
    //println("sid = " + sid + ": fid = " + sourceDataset.dict.fidOf(sid))
    println("sid = " + sid + ": fid = " + data.dict.fidOf(sid))
  }


  /*// save it
  val savedData = data.save("data-local/nyt-1991-data")
  savedData.print(5)
  println("--")

  // load it
  val loadedData = DesqDataset.load("data-local/nyt-1991-data")
  loadedData.print(5)
  println("--")*/

  println("DONE")
}
