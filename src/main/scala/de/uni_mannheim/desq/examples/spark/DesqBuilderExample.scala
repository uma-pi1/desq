package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.dictionary.{Dictionary, Item}
import de.uni_mannheim.desq.mining.spark.DesqDataset
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

/**
  * Created by rgemulla on 03.10.2016.
  */
object DesqBuilderExample extends App {
  val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
  implicit val sc = new SparkContext(conf)

  // create the dataset
  val lines = sc.textFile("data-local/nyt-1991-data.del")
  val data = DesqDataset.buildFromStrings(lines.map(s => s.split(" ")))
  data.print(100)

  // test correctness of counts
  val fullDict = Dictionary.loadFrom("data-local/nyt-1991-dict.avro.gz")
  for (item <- fullDict.getItems) {
    if (item.children.size() == 0) { // only verify leafs
      val otherItem = data.dict.getItemBySid(Integer.toString(item.gid))
      if (item.cFreq != otherItem.cFreq || item.dFreq != otherItem.dFreq) {
        println(s"Incorrect item: $item != $otherItem")
      }
    }
  }
  println("DONE")
}
