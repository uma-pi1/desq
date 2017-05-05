package de.uni_mannheim.desq.examples.spark

import java.io.FileOutputStream

import de.uni_mannheim.desq.Desq._
import de.uni_mannheim.desq.converters.nyt.{ConvertNyt, NytUtil}
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.spark.DesqDataset
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created by rgemulla on 03.10.2016.
  */
object DesqBuilderExample {

  def nyt_del()(implicit sc: SparkContext) {
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
      if (fullDict.childrenOf(fid).size() == 0) {
        // only verify leafs
        val otherFid = loadedData.dict.fidOf(fullDict.sidOfFid(fid))
        if (otherFid != -1 && // the created dictionary may not contain some items (those which do not occur in the data)
          (fullDict.cfreqOf(fid) != loadedData.dict.cfreqOf(otherFid) ||
            fullDict.dfreqOf(fid) != loadedData.dict.dfreqOf(otherFid))) {
          println("Incorrect item: " + fullDict.sidOfFid(fid) + " " + loadedData.dict.fidOf(otherFid))
        }
      }
    }
    println("DONE")
  }

  def nyt_scala()(implicit sc: SparkContext) {

    val nyt_avro = "data-local/NYTimesProcessed/results/2007/01/"
    val nytJavaOutDir = "data-local/processed/sparkconvert/200701_java/"
    val nytConverter = new ConvertNyt
    nytConverter.buildDesqDataset(nyt_avro, nytJavaOutDir, false)

    val articles_raw = NytUtil.loadArticlesFromFile(nyt_avro).flatMap(r=>NytUtil.convertToArticle(r).getSentences)
    val data = DesqDataset.buildFromSentences(articles_raw)

    val dataSaved = data.save("data-local/processed/sparkconvert/200701_scala")
    dataSaved.print(5)
    println("--")

    val loadedData = DesqDataset.load("data-local/processed/sparkconvert/200701_scala")
    loadedData.print(5)
    println("--")

    val fullDict = Dictionary.loadFrom("data-local/processed/sparkconvert/200701_java/all/dict.avro.gz")
    for (fid <- fullDict.fids) {
      if (fullDict.childrenOf(fid).size() == 0) {
        // only verify leafs
        val otherFid = loadedData.dict.fidOf(fullDict.sidOfFid(fid))
        if (otherFid != -1 && // the created dictionary may not contain some items (those which do not occur in the data)
          (fullDict.cfreqOf(fid) != loadedData.dict.cfreqOf(otherFid) ||
            fullDict.dfreqOf(fid) != loadedData.dict.dfreqOf(otherFid))) {
          println("Incorrect item: " + fullDict.sidOfFid(fid) + " " + fid + " " + otherFid)
        }
      }
    }
    println("DONE")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    initDesq(conf)
    implicit val sc = new SparkContext(conf)
    nyt_scala()
  }
}
