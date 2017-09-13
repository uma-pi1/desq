package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.Desq._
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.spark.DesqDataset
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Testing of Itemset API
  * Created by sulbrich on 13.09.2017
  * Copy of DesqBuilderExample by rgemulla
  */
object DesqItemsetBuilderExample extends App {
  val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
  initDesq(conf)
  implicit val sc = new SparkContext(conf)

  // create the dataset
  val lines = sc.textFile("data-local/nyt-1991-data.del")
  //val data = DesqDataset.buildItemsetsFromStrings(lines.map(s => s.split(" ")))
  val data = DesqDataset.buildItemsets(lines.map(s => s.split(" ")))

  // save it
  val savedData = data.save("data-local/nyt-1991-data")
  savedData.print(5)
  println("--")

  // load it
  val loadedData = DesqDataset.load("data-local/nyt-1991-data")
  loadedData.print(5)
  println("--")

  println("DONE")
}
