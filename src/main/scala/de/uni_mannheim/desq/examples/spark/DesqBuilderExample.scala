package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.Desq._
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.spark.{DesqDataset, GenericDesqDataset}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created by rgemulla on 03.10.2016.
  */
object DesqBuilderExample extends App {
  val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
  initDesq(conf)
  implicit val sc = new SparkContext(conf)

  // create the dataset
  val lines = sc.textFile("data-local/nyt-1991-data.del")
  val data = DesqDataset.buildFromStrings(lines.map(s => s.split(" ")))

  // save it
  val savedData = data.save("data-local/nyt-1991-data")
  savedData.print(5)
  println("--")

  // load it
  val loadedData = DesqDataset.load("data-local/nyt-1991-data")
  loadedData.print(5)
  println("--")

  // test correctness of counts
  val fullDict = Dictionary.loadFrom("data-local/nyt-1991-dict.avro.gz")
  for (fid <- fullDict.fids) {
    if (fullDict.childrenOf(fid).size() == 0) { // only verify leafs
      val otherFid = loadedData.sequenceInterpreter.getDictionary.fidOf(fullDict.sidOfFid(fid))
      if (otherFid != -1 && // the created dictionary may not contain some items (those which do not occur in the data)
        (fullDict.cfreqOf(fid) != loadedData.sequenceInterpreter.getDictionary.cfreqOf(otherFid) ||
        fullDict.dfreqOf(fid) != loadedData.sequenceInterpreter.getDictionary.dfreqOf(otherFid))) {
        println("Incorrect item: " + fullDict.sidOfFid(fid) + " " + loadedData.sequenceInterpreter.getDictionary.fidOf(otherFid))
      }
    }
  }
  println("DONE")
}
