package de.uni_mannheim.desq.examples.spark

import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import de.uni_mannheim.desq.Desq.initDesq
import de.uni_mannheim.desq.comparing.DesqCompare
import de.uni_mannheim.desq.converters.nyt.NytUtil
import de.uni_mannheim.desq.mining.spark.{DesqDataset, DesqMiner}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
/**
  * Created by ivo on 05.05.17.
  */
object DesqCompareExample {
  def compareDatasets()(implicit sc: SparkContext) {
    val patternExpression = "(.^ JJ NN)"
    val sigma = 1
    var k = 10

    print("Initializing Compare... ")
    val prepTime = Stopwatch.createStarted
    val compare = new DesqCompare
    prepTime.stop()
    println(prepTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    print("Loading left collection desq dataset from disk... ")
    val loadLeftTime = Stopwatch.createStarted
    val data_left = DesqDataset.load("data-local/processed/sparkconvert/left")
    //    val dict_left: Dictionary = Dictionary.loadFrom("data-local/nyt-1991-dict.avro.gz")
    //    val delFilename_left = "data-local/nyt-1991-data.del"
    //    val delFile_left = sc.textFile(delFilename_left)
    //    val data_left = DesqDataset.loadFromDelFile(delFile_left, dict_left, usesFids = true)
    loadLeftTime.stop
    println(loadLeftTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    print("Loading right collection desq dataset from disk... ")
    val loadRightTime = Stopwatch.createStarted
    val data_right = DesqDataset.load("data-local/processed/sparkconvert/right")
    //    val dict_right: Dictionary = Dictionary.loadFrom("data-local/nyt-1991-dict.avro.gz")
    //    val delFilename_right = "data-local/nyt-1991-data.del"
    //    val delFile_right = sc.textFile(delFilename_right)
    //    val data_right = DesqDataset.loadFromDelFile(delFile_right, dict_right, usesFids = true)
    loadRightTime.stop
    println(loadRightTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    print("Comparing the two collections... ")
    val compareTime = Stopwatch.createStarted
    compare.compare(data_left, data_right, patternExpression, sigma, k)
    compareTime.stop
    println(compareTime.elapsed(TimeUnit.MILLISECONDS) + "ms")
  }


  def buildAndCompare()(implicit sc: SparkContext): Unit = {
    val patternExpression = "(.^ JJ NN)"
    val sigma = 3
    val k = 5

    print("Initializing Compare... ")
    val prepTime = Stopwatch.createStarted
    val compare = new DesqCompare
    prepTime.stop()
    println(prepTime.elapsed(TimeUnit.MILLISECONDS) + "ms")


    print("Loading raw articles for left collection from disk... ")
    val loadLeftArticlesTime = Stopwatch.createStarted
    val dir_left = "data-local/NYTimesProcessed/results/2006"
    val raw_left = NytUtil.loadArticlesFromFile(dir_left).flatMap(r => NytUtil.convertToArticle(r).getSentences)
    loadLeftArticlesTime.stop
    println(loadLeftArticlesTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    print("Loading raw articles for right collection from disk... ")
    val loadRightTime = Stopwatch.createStarted
    val dir_right = "data-local/NYTimesProcessed/results/2005"
    val raw_right = NytUtil.loadArticlesFromFile(dir_right).flatMap(r => NytUtil.convertToArticle(r).getSentences)
    loadRightTime.stop
    println(loadRightTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    print("Building and Comparing the two collections... ")
    val buildCompareTime = Stopwatch.createStarted
    compare.buildCompare(raw_left, raw_right, patternExpression, sigma, k)
    buildCompareTime.stop
    println(buildCompareTime.elapsed(TimeUnit.MILLISECONDS) + "ms")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    initDesq(conf)
    implicit val sc = new SparkContext(conf)
        buildAndCompare()
//    compareDatasets()
  }
}
