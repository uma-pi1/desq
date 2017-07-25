package de.uni_mannheim.desq.examples.spark

import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import de.uni_mannheim.desq.Desq._
import de.uni_mannheim.desq.converters.nyt.{ConvertNyt, NytUtil}
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.spark.{DefaultDesqDataset, IdentifiableDesqDataset}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created by rgemulla on 03.10.2016.
  */
object DesqBuilderExample {

  def nyt()(implicit sc: SparkContext) {
    // create the dataset
    val lines = sc.textFile("data-local/nyt-1991-data.del")
    val data = DefaultDesqDataset.buildFromStrings(lines.map(s => s.split(" ")))

    // save it
    val savedData = data.save("data-local/nyt-1991-data")
    savedData.print(5)
    println("--")

    // load it
    val loadedData = DefaultDesqDataset.load("data-local/nyt-1991-data")
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

    val nyt_avro = "data-local/NYTimesProcessed/results/2007/"
    val nytJavaOutDir = "data-local/processed/sparkconvert/2007_java/"
    val scalaOutDir = "data-local/processed/sparkconvert/2007_scala"
    val nytConverter = new ConvertNyt

    print("Converting the raw data using the Java NYT Converter... ")
    val javaTime = Stopwatch.createStarted()
    nytConverter.buildDesqDataset(nyt_avro, nytJavaOutDir, false)
    javaTime.stop()
    println(javaTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    print("Converting the raw data using the Scala NYT Converter... ")
    val scalaTime = Stopwatch.createStarted()
    val articles_raw = NytUtil.loadArticlesFromFile(nyt_avro).map(r => (42L, r)).flatMapValues(f => f.getSentences)
    val data = IdentifiableDesqDataset.buildFromSentencesWithID(articles_raw)
    scalaTime.stop()
    println(scalaTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    val dataSaved = data.save(scalaOutDir)
    dataSaved.print(5)
    println("--")

    val loadedData = IdentifiableDesqDataset.load("data-local/processed/sparkconvert/2007_scala")
    loadedData.print(5)
    println("--")

    val fullDict = Dictionary.loadFrom("data-local/processed/sparkconvert/2007_java/all/dict.avro.gz")
    for (fid <- fullDict.fids) {
      if (fullDict.childrenOf(fid).size() == 0) {
        // only verify leafs
        val otherFid = loadedData.dict.fidOf(fullDict.sidOfFid(fid))
        if (otherFid != -1 && // the created dictionary may not contain some items (those which do not occur in the data)
          (fullDict.cfreqOf(fid) != loadedData.dict.cfreqOf(otherFid) ||
            fullDict.dfreqOf(fid) != loadedData.dict.dfreqOf(otherFid))) {
          println(s"Incorrect item: ${fullDict.sidOfFid(fid)} ID:$fid  Other ID: $otherFid")
        }
      }
    }
    println("DONE")
  }


  def convertDesqDict()(implicit sc: SparkContext): Unit = {
    print("Loading DesqDataset from Disk\n")
    val loadedData = IdentifiableDesqDataset.load("data-local/processed/sparkconvert/2007_scala")
    print("Writing Dict of DesqDataset to Disk\n")
    val scalaDict = loadedData.dict
    print("Writing dict.json\n")
    scalaDict.write("data-local/processed/sparkconvert/2007_scala/dict.json")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    initDesq(conf)
    implicit val sc = new SparkContext(conf)
    nyt_scala()
  }
}
