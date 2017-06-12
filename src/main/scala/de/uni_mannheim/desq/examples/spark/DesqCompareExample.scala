package de.uni_mannheim.desq.examples.spark

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import de.uni_mannheim.desq.Desq.initDesq
import de.uni_mannheim.desq.comparing.DesqCompare
import de.uni_mannheim.desq.converters.nyt.NytUtil
import de.uni_mannheim.desq.elastic.NYTElasticSearchUtils
import de.uni_mannheim.desq.mining.spark.DesqDataset
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
    val sigma = 100
    val k = 5

    print("Initializing Compare... ")
    val prepTime = Stopwatch.createStarted
    val compare = new DesqCompare
    prepTime.stop()
    println(prepTime.elapsed(TimeUnit.MILLISECONDS) + "ms")


    print("Loading raw articles for left collection from disk... ")
    val loadLeftArticlesTime = Stopwatch.createStarted
    val dir_left = "data-local/NYTimesProcessed/results/2006"
    val raw_left = NytUtil.loadArticlesFromFile(dir_left).flatMap(r => r.getSentences)
    loadLeftArticlesTime.stop
    println(loadLeftArticlesTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    print("Loading raw articles for right collection from disk... ")
    val loadRightTime = Stopwatch.createStarted
    val dir_right = "data-local/NYTimesProcessed/results/2005"
    val raw_right = NytUtil.loadArticlesFromFile(dir_right).flatMap(r => r.getSentences)
    loadRightTime.stop
    println(loadRightTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    print("Building and Comparing the two collections... ")
    val buildCompareTime = Stopwatch.createStarted
    compare.buildCompare(raw_left, raw_right, patternExpression, sigma, k)
    buildCompareTime.stop
    println(buildCompareTime.elapsed(TimeUnit.MILLISECONDS) + "ms")
  }

  def searchAndCompare(path_source: String, query_left: String, query_right: String, patternExpression: String, sigma: Int = 1, k: Int = 10, index: String)(implicit sc: SparkContext): Unit = {
    print("Initializing Compare... ")
    val prepTime = Stopwatch.createStarted
    val es = new NYTElasticSearchUtils
    val compare = new DesqCompare
    val dataset = DesqDataset.load(path_source)
    //    dataset.sequences.cache()
    prepTime.stop()
    println(prepTime.elapsed(TimeUnit.MILLISECONDS) + "ms")
    print("Querying Elastic... ")
    val queryTime = Stopwatch.createStarted
    val ids_query_l = es.searchES(query_left, index)
    val ids_query_r = es.searchES(query_right, index)
    println(s"overlap ${ids_query_l.intersect(ids_query_r).size}")
    queryTime.stop()
    println(queryTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    println("Initialize Left Side Dataset with ES Query")
    val leftPrepTime = Stopwatch.createStarted
    val ids_left = sc.broadcast(ids_query_l.diff(ids_query_r))
    println(s"only left ${ids_left.value.size()}")
    val dataset_left = new DesqDataset(dataset.sequences.filter(f => ids_left.value.contains(f.id)).repartition(18), dataset.dict.deepCopy(), true)
    //    val dataset_left = new DesqDataset(dataset.sequences.filter{case (seq) => ids_left.value.contains(seq.id)}, dataset.dict.deepCopy())
    //    val dataset_left = dataset_left_.copyWithRecomputedCountsAndFids()

    leftPrepTime.stop()
    println(leftPrepTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    println("Initialize Right Side Dataset with ES Query")
    val rightPrepTime = Stopwatch.createStarted
    val ids_right = sc.broadcast(ids_query_r.diff(ids_query_l))
    println(s"only right ${ids_right.value.size}")
    val dataset_right = new DesqDataset(dataset.sequences.filter(f => ids_right.value.contains(f.id)).repartition(18), dataset.dict.deepCopy(), true)
    //    val dataset_right = new DesqDataset(dataset.sequences.filter{case (seq) => ids_right.value.contains(seq.id)}, dataset.dict.deepCopy())
    //    val dataset_right = dataset_right_.copyWithRecomputedCountsAndFids()
    rightPrepTime.stop()
    println(rightPrepTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    println("Comparing the two collections... ")
    val compareTime = Stopwatch.createStarted
    compare.compare(dataset_left, dataset_right, patternExpression, sigma, k)
    compareTime.stop
    println(compareTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

  }

  def createIndexAndDataSet(path_in: String, path_out: String, index: String)(implicit sc: SparkContext): Unit = {
    print("Indexing Articles and Creating DesqDataset... ")
    val dataTime = Stopwatch.createStarted
    val nytEs = new NYTElasticSearchUtils
    nytEs.createIndexAndDataset(path_in, path_out, index)
    dataTime.stop()
    println(dataTime.elapsed(TimeUnit.MILLISECONDS) + "ms")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    initDesq(conf)
    implicit val sc = new SparkContext(conf)
    //    buildAndCompare()
    //    compareDatasets()

    val path_in = "data-local/NYTimesProcessed/results/2006/"
    val path_out = "data-local/processed/es_2006/"
    val index = "nyt2006/article"
    if (!Files.exists(Paths.get(path_out))) {
      Files.createDirectory(Paths.get(path_out))
      createIndexAndDataSet(path_in, path_out, index)
    }

    val query_left = "\"Donald Trump\""
    val query_right = "\"Hillary Clinton\""
    val patternExpression = "(DT+? RB+ JJ+ NN+ PR+)"
    val patternExpression2 = "(RB+ MD+ VB+)"
    val patternExpression3 = "(ENTITY)"
    val patternExpression4 = "(VB)"
    val patternExpressionN1 = "ENTITY (VB+ NN+? IN?) ENTITY"
    val patternExpressionN2 = "(ENTITY^ VB+ NN+? IN? ENTITY^)"
    val patternExpressionN21 = "(ENTITY VB+ NN+? IN? ENTITY)"
    val patternExpressionN3 = "(ENTITY^ be@VB=^) DT? (RB? JJ? NN)"
    val patternExpressionN4 = "(.^){3} NN"
    val patternExpressionO1 = "(JJ NN) ."
    val patternExpressionO2 = "(RB JJ) ^NN"
    val patternExpressionO3 = "(JJ JJ) ^NN"
    val patternExpressionO4 = "(NN JJ) ^NN"
    val patternExpressionO5 = "(RB VB) ."
    val patternExpressionO1_5 = "(JJ NN .)| (RB JJ ^NN)| (JJ JJ ^NN) | (NN JJ ^NN) | (RB VB .)"
    val patternExpressionOpinion2 = "(ENTITY).^{1,3} [(JJ NN .)| (RB JJ ^NN)| (JJ JJ ^NN) | (NN JJ ^NN) | (RB VB .)]"
    val sigma = 1
    val k = 40
    searchAndCompare(path_out, query_left, query_right, patternExpressionO1, sigma, k, index)
    //    searchAndCompare(path_out, query_left, query_right, patternExpressionO2, sigma, k, index)
    //    searchAndCompare(path_out, query_left, query_right, patternExpressionO3, sigma, k, index)
    //    searchAndCompare(path_out, query_left, query_right, patternExpressionO4, sigma, k, index)
    //    searchAndCompare(path_out, query_left, query_right, patternExpressionO5, sigma, k, index)
    //    searchAndCompare(path_out, query_left, query_right, patternExpressionO1_5, sigma, k, index)
    System.in.read
    sc.stop()
  }
}
