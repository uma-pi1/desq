package de.uni_mannheim.desq.examples.spark

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import de.uni_mannheim.desq.Desq.initDesq
import de.uni_mannheim.desq.comparing.{DesqCompare, DesqCompareNaive}
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.elastic.NYTElasticSearchUtils
import de.uni_mannheim.desq.mining.spark.{DefaultDesqDataset, DesqCount, DesqMiner, DesqMinerContext}
import de.uni_mannheim.desq.mining.{AggregatedSequence, AggregatedWeightedSequence, WeightedSequence}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by ivo on 05.05.17.
  */
object DesqCompareExample {
  var wallclock: Stopwatch = Stopwatch.createUnstarted()

  /**
    * Run the First Version of the System
    *
    * @param path_source Where is the data stored?
    * @param query_L     Keyword Query for the "Left Dataset"
    * @param query_R     Keyword Query for the "Right Dataset"
    * @param patExp      PatternExpression used for mining
    * @param sigma       Minimum Support Threshold for Mining
    * @param k           Number of Pattern to be returned
    * @param index       ElasticSearch Index to query from
    * @param limit       Maximum Number of Results for each Query
    * @param sc          SparkContext
    */
  def searchAndCompareNaive(path_source: String, query_L: String, query_R: String, patExp: String, sigma: Int = 1, k: Int = 10, index: String, limit: Int, path_out: String)(implicit sc: SparkContext): Unit = {
    print("Initializing Compare... ")
    val prepTime = Stopwatch.createStarted
    val es = new NYTElasticSearchUtils
    prepTime.stop()
    println(prepTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    print("Loading Dataset")
    val dataloadTime = Stopwatch.createStarted
    val compare = new DesqCompareNaive(path_source)
    dataloadTime.stop()
    println(dataloadTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    print("Querying Elastic & Creating Ad-hoc Datasets... ")
    val (dataset_L, dataset_R) = compare.createAdhocDatasets(index, query_L, query_R, limit)

    println("Comparing the two collections... ")
    val compareTime = Stopwatch.createStarted
    compare.compare(dataset_L.toDefaultDesqDataset(), dataset_R.toDefaultDesqDataset(), patExp, sigma, k)
    compareTime.stop
    println(compareTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    val totalTime = dataloadTime.elapsed(TimeUnit.SECONDS) + compare.filterT + compare.filterT + compareTime.elapsed(TimeUnit.SECONDS)
    val times = s"DC1-$patExp-$sigma-$query_L-$query_R-${limit}_${System.currentTimeMillis / 1000}.csv"
    val times_string = s"$totalTime, ${dataloadTime.elapsed(TimeUnit.SECONDS)},${compare.filterT},${compare.queryT},${compareTime.elapsed(TimeUnit.SECONDS)}"
    writeTimesToFile(times_string, path_out, times)
  }

  /**
    * Run the improved Version of the System.
    *
    * @param data_path Where is the data stored?
    * @param query_L   Keyword Query for the "Left Dataset"
    * @param query_R   Keyword Query for the "Right Dataset"
    * @param patExp    PatternExpression used for mining
    * @param sigma     Minimum Support Threshold for Mining
    * @param k         Number of Pattern to be returned
    * @param index     ElasticSearch Index to query from
    * @param parts     Number of Partitions used for the filter
    * @param limit     Maximum Number of Results for each Query
    * @param sc        SparkContext
    */
  def searchAndCompareDesqTwoCount(data_path: String, query_L: String, query_R: String, patExp: String, sigma: Int = 1, k: Int = 10, index: String, parts: Int = 96, limit: Int = 1000)(implicit sc: SparkContext): Unit = {
    print("Initializing Compare... ")
    val prepTime = Stopwatch.createStarted
    val es = new NYTElasticSearchUtils
    prepTime.stop()
    println(prepTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    print("Loading Dataset")
    val dataloadTime = Stopwatch.createStarted
    val compare = new DesqCompare(data_path, parts)
    dataloadTime.stop()
    println(s"Loading Dataset took: ${dataloadTime.elapsed(TimeUnit.SECONDS)}s")

    print("Querying Elastic and Creating Ad-hoc Dataset... ")

    val (dataset, index_comb) = compare.createAdhocDatasets(index, query_L, query_R, limit)
    println(s"There are ${index_comb.value.size} relevant documents.")

    println(s"Querying Elastic & Creating Ad-hoc Dataset took: ${compare.filterT + compare.queryT}s")


    println("Mining interesting sequences... ")
    val compareTime = Stopwatch.createStarted
    val sequences = compare.compare(dataset, index_comb, patExp, sigma, k)
    compareTime.stop
    println(s"Mining interesting sequences took: ${compareTime.elapsed(TimeUnit.SECONDS)}s")

    //    val totalTime = dataloadTime.elapsed(TimeUnit.SECONDS) + compare.queryT + compare.filterT + compareTime.elapsed(TimeUnit.SECONDS)
    val totalTime = wallclock.stop.elapsed(TimeUnit.SECONDS)
    val filename = s"DC2-$patExp-$sigma-$query_L-$query_R-$limit-${parts}_${System.currentTimeMillis / 1000}.csv"
    val times_string = s"$totalTime, ${dataloadTime.elapsed(TimeUnit.SECONDS)},${compare.filterT},${compare.queryT},${compareTime.elapsed(TimeUnit.SECONDS)}"
    writeTimesToFile(times_string, data_path, filename)
    printAggregatedWeightedSequencesToFile(data_path, filename, sequences, dataset.dict)


  }

  /**
    * Run the improved Version of the System.
    * Compare Ad-hoc Datasets with Background
    *
    * @param data_path Where is the data stored?
    * @param queryFrom Optional: Set the Background Dataset Start Date
    * @param queryTo   Optional: Set the Background Dataset End Date
    * @param query_L   Keyword Query for the "Left Dataset"
    * @param query_R   Keyword Query for the "Right Dataset"
    * @param patExp    PatternExpression used for mining
    * @param sigma     Minimum Support Threshold for Mining
    * @param k         Number of Pattern to be returned
    * @param index     ElasticSearch Index to query from
    * @param parts     Number of Partitions used for the filter
    * @param limit     Maximum Number of Results for each Query
    * @param sc        SparkContext
    */
  def searchAndCompareDesqMultiCount(data_path: String, queryFrom: String, queryTo: String, query_L: String, query_R: String, patExp: String, sigma: Int = 1, k: Int = 10, index: String, parts: Int = 96, limit: Int = 1000)(implicit sc: SparkContext): Unit = {
    print("Initializing Compare... ")
    val prepTime = Stopwatch.createStarted
    val es = new NYTElasticSearchUtils
    prepTime.stop()
    println(prepTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    print("Loading Dataset")
    val dataloadTime = Stopwatch.createStarted
    val compare = new DesqCompare(data_path, parts)
    dataloadTime.stop()
    println(s"Loading Dataset took: ${dataloadTime.elapsed(TimeUnit.SECONDS)}s")

    print("Querying Elastic and Creating Ad-hoc Dataset... ")

    val (dataset, index_comb) = compare.createAdhocDatasets(index, query_L, query_R, limit, queryFrom, queryTo)
    println(s"There are ${index_comb.value.size} relevant documents.")

    println(s"Querying Elastic & Creating Ad-hoc Dataset took: ${compare.filterT + compare.queryT}s")


    println("Mining interesting sequences... ")
    val compareTime = Stopwatch.createStarted
    //     compare.compare(dataset, index_comb, patExp, sigma, k)
    val sequences = compare.compareWithBackground(dataset, index_comb, patExp, sigma, k)
    compareTime.stop
    println(s"Mining interesting sequences took: ${compareTime.elapsed(TimeUnit.SECONDS)}s")

    //    val totalTime = dataloadTime.elapsed(TimeUnit.SECONDS) + compare.queryT + compare.filterT + compareTime.elapsed(TimeUnit.SECONDS)
    val totalTime = wallclock.stop.elapsed(TimeUnit.SECONDS)
    val times = s"DC2-$patExp-$sigma-$query_L-$query_R-$limit-$parts-${queryFrom.replace("/", "-")}-${queryTo.replace("/", "-")}_${System.currentTimeMillis / 1000}.csv"
    val times_string = s"$totalTime, ${dataloadTime.elapsed(TimeUnit.SECONDS)},${compare.filterT},${compare.queryT},${compareTime.elapsed(TimeUnit.SECONDS)}"
    writeTimesToFile(times_string, data_path, times)
    printAggregatedSequencesToFile(data_path, times, sequences, dataset.dict)


  }


  def searchAndMine(data_path: String, queryFrom: String, queryTo: String, query_L: String, query_R: String, patExp: String, sigma: Int = 1, k: Int = 10, index: String, parts: Int = 96, limit: Int = 1000)(implicit sc: SparkContext): Unit ={
    print("Initializing Compare... ")
    val prepTime = Stopwatch.createStarted
    val es = new NYTElasticSearchUtils
    prepTime.stop()
    println(prepTime.elapsed(TimeUnit.MILLISECONDS) + "ms")

    print("Loading Dataset")
    val dataloadTime = Stopwatch.createStarted
    val dataset = DefaultDesqDataset.load(data_path)
    dataloadTime.stop()
    println(s"Loading Dataset took: ${dataloadTime.elapsed(TimeUnit.SECONDS)}s")

    print("Querying Elastic and Creating Ad-hoc Dataset... ")
//
//    val (dataset, index_comb) = compare.createAdhocDatasets(index, query_L, query_R, limit, queryFrom, queryTo)
//    println(s"There are ${index_comb.value.size} relevant documents.")
//
//    println(s"Querying Elastic & Creating Ad-hoc Dataset took: ${compare.filterT + compare.queryT}s")


    println("Mining interesting sequences... ")
    val compareTime = Stopwatch.createStarted
    val conf = DesqCount.createConf(patExp, sigma)
    conf.setProperty("desq.mining.prune.irrelevant.inputs", true)
    conf.setProperty("desq.mining.use.two.pass", true)
    val ctx = new DesqMinerContext(conf)
    val miner = DesqMiner.create(ctx)
    val result = miner.mine(dataset)
    val sequences = result.sequences.collect()
    compareTime.stop
    println(s"Mining interesting sequences took: ${compareTime.elapsed(TimeUnit.SECONDS)}s")

    val totalTime = wallclock.stop.elapsed(TimeUnit.SECONDS)

    val filename = s"DC-$patExp-$sigma-$query_L-$query_R-$limit-${queryFrom.replace("/", "-")}-${queryTo.replace("/", "-")}_${System.currentTimeMillis / 1000}_regularDC.csv"

    val times_string = s"$totalTime, ${dataloadTime.elapsed(TimeUnit.SECONDS)},0,0,${compareTime.elapsed(TimeUnit.SECONDS)}"

    writeTimesToFile(times_string, data_path, filename)

    if (!Files.exists(Paths.get(s"$data_path/experiments/"))) {
      Files.createDirectory(Paths.get(s"$data_path/experiments/"))
    }
    val file = if (!Files.exists(Paths.get(s"$data_path/experiments/sequences_$filename"))) {
      new File(s"$data_path/experiments/sequences_$filename")
    } else {
      val timestamp: Long = System.currentTimeMillis / 1000
      new File(s"$data_path/experiments/${timestamp.toString}_$filename")
    }
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("sequence, global_freq, left_freq, left_int, right_freq, right_int\n")
    val dict = result.dict
    for (s <- sequences) s match {
      case (_:WeightedSequence) => {
        val sids = for (e <- s.elements) yield {
          dict.sidOfFid(e)
        }
        bw.write(s"${sids.deep.mkString("[", " ", "]")},${s.weight}\n")
      }
    }
    bw.close()

  }


  //    println(s"Overall Runtime is ${compareTime.elapsed(TimeUnit.SECONDS)+ leftPrepTime.elapsed(TimeUnit.SECONDS) + queryTime.elapsed(TimeUnit.SECONDS)+ dataloadTime.elapsed(TimeUnit.SECONDS)}")


  /**
    * Triggers the Index and DesqDataset Creation as a Preprocessing Step for all further analysis
    *
    * @param path_in  location of the NYT Raw Data
    * @param path_out location where the DesqDataset should be stored
    * @param index    name of the elasticsearch index to be created
    * @param sc       Implicit SparkContext
    */
  def createIndexAndDataSet(path_in: String, path_out: String, index: String)(implicit sc: SparkContext): Unit = {
    print("Indexing Articles and Creating DesqDataset... ")
    val dataTime = Stopwatch.createStarted
    val nytEs = new NYTElasticSearchUtils
    nytEs.createIndexAndDataset(path_in, path_out, index)
    dataTime.stop()
    println(dataTime.elapsed(TimeUnit.MILLISECONDS) + "ms")
  }

  def writeTimesToFile(times: String, path_out: String, filename: String) = {
    if (!Files.exists(Paths.get(s"$path_out/experiments/"))) {
      Files.createDirectory(Paths.get(s"$path_out/experiments/"))
    }
    if (!Files.exists(Paths.get(s"$path_out/experiments/$filename"))) {
      val file = new File(s"$path_out/experiments/$filename")
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write("total,load,query,filter,mining \n")
      bw.write(times + "\n")
      bw.close()
    } else {
      val file = new File(s"$path_out/experiments/$filename")
      val bw = new BufferedWriter(new FileWriter(file, true))
      bw.write(times + "\n")
      bw.close()
    }
  }

  def printAggregatedSequencesToFile(path_out: String, filename: String, sequences: Array[((AggregatedSequence, Float, Float))], dict: Dictionary) {
    if (!Files.exists(Paths.get(s"$path_out/experiments/"))) {
      Files.createDirectory(Paths.get(s"$path_out/experiments/"))
    }
    val file = if (!Files.exists(Paths.get(s"$path_out/experiments/sequences_$filename"))) {
      new File(s"$path_out/experiments/sequences_$filename")
    } else {
      val timestamp: Long = System.currentTimeMillis / 1000
      new File(s"$path_out/experiments/${timestamp.toString}_$filename")
    }
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("sequence, global_freq, left_freq, left_int, right_freq, right_int\n")
    for (s <- sequences) s match {
      case ((s._1, s._2, s._3)) => {
        val sids = for (e <- s._1.elements) yield {
          dict.sidOfFid(e)
        }
        bw.write(s"${sids.deep.mkString("[", " ", "]")},${s._1.support.getLong(0)},${s._1.support.getLong(1)},${s._2},${s._1.support.getLong(2)},${s._3}\n")
      }
    }
    bw.close()
  }

  def printAggregatedWeightedSequencesToFile(path_out: String, filename: String, sequences: Array[((AggregatedWeightedSequence, Float, Float))], dict: Dictionary) {
    if (!Files.exists(Paths.get(s"$path_out/experiments/"))) {
      Files.createDirectory(Paths.get(s"$path_out/experiments/"))
    }
    val file = if (!Files.exists(Paths.get(s"$path_out/experiments/sequences_$filename"))) {
      new File(s"$path_out/experiments/sequences_$filename")
    } else {
      val timestamp: Long = System.currentTimeMillis / 1000
      new File(s"$path_out/experiments/${timestamp.toString}_$filename")
    }
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("sequence, global_freq, left_freq, left_int, right_freq, right_int\n")
    for (s <- sequences) s match {
      case ((s._1, s._2, s._3)) => {
        val sids = for (e <- s._1.elements) yield {
          dict.sidOfFid(e)
        }
        bw.write(s"${sids.deep.mkString("[", " ", "]")},${s._1.weight},${s._2},${s._1.weight_other},${s._2}\n")
      }
    }
    bw.close()
  }


  def main(args: Array[String]) {
    var path_in = "data-local/NYTimesProcessed/results/"
    var path_out = "data-local/processed/es_all_v1"
    var parts = 128
    var sigma = 10
//    var patternExp = "(ENTITY . ENTITY) | (ENTITY .. ENTITY) |(ENTITY ... ENTITY) "
    var patternExp = "(JJ NN) ."
//    var patternExp = "(.){2,6}"
    var index = "nyt_v1"
    var queryL = "George Bush"
    var queryR = "United States"
    var queryFrom = "1987/01/01"
    var queryTo = "2007/12/31"
    var limit = 1800000
    var k = 1000
    var algo = "DC"
    var master = "local[*]"

    var params = ListBuffer[Array[String]]()
    if (args.length > 0) {
      for (arg <- args) {
        val splits = arg.split("=")
        params += splits
      }
    }

    params.toList.collect {
      case Array("--master", argMaster: String) => master = argMaster
      case Array("--algo", argAlgo: String) => algo = argAlgo
      case Array("--in", argIn: String) => path_in = argIn
      case Array("--out", argOut: String) => path_out = argOut
      case Array("--parts", argParts: String) => parts = argParts.toInt
      case Array("--patexp", argPatEx: String) => patternExp = argPatEx
      case Array("--index", argIndex: String) => index = argIndex
      case Array("--left", argQL: String) => queryL = argQL
      case Array("--right", argQR: String) => queryR = argQR
      case Array("--from", argQF: String) => queryFrom = argQF
      case Array("--to", argQT: String) => queryTo = argQT
      case Array("--limit", argLimit: String) => limit = argLimit.toInt
      case Array("--k", argK: String) => k = argK.toInt
      case Array("--sigma", argSigma: String) => sigma = argSigma.toInt
    }

    val conf = new SparkConf().setAppName(getClass.getName).setMaster(master)
      .set("spark.driver.extraClassPath", sys.props("java.class.path"))
      .set("spark.executor.extraClassPath", sys.props("java.class.path"))
      .set("fs.local.block.size", "128mb")
      .set("spark.eventLog.enabled", "true")
    wallclock.start
    initDesq(conf)
    implicit val sc = new SparkContext(conf)

    if (!Files.exists(Paths.get(path_out))) {
      Files.createDirectory(Paths.get(path_out))
      createIndexAndDataSet(path_in, path_out, index)
    }

    val patternExpression = "(DT+? RB+ JJ+ NN+ PR+)"
    val patternExpression2 = "(RB+ MD+ VB+)"
    val patternExpression3 = "(ENTITY)"
    val patternExpression4 = "(VB)"
    val patternExpressionN1 = "ENTITY (VB+ NN+? IN?) ENTITY"
    val patternExpressionN2 = "(ENTITY^ VB+ NN+? IN? ENTITY^)"
    val patternExpressionN21 = "(ENTITY VB+ NN+? IN? ENTITY)"
    val patternExpressionN3 = "(ENTITY^ be@VB=^) DT? (RB? JJ? NN)"
    val patternExpressionN4 = "(.^){3} NN"
    val patternExpressionN5 = "([.^ . .]|[. .^ .]|[. . .^])"
    val patternExpressionO1 = "(JJ NN) ."
    val patternExpressionO2 = "(RB JJ) NN^"
    val patternExpressionO3 = "(JJ JJ) NN^"
    val patternExpressionO4 = "(NN JJ) NN^"
    val patternExpressionO5 = "(RB VB) ."
    val patternExpressionO1_5 = "(JJ NN .)| (RB JJ ^NN)| (JJ JJ ^NN) | (NN JJ ^NN) | (RB VB .)"
    val patternExpressionOpinion2 = "(ENTITY).^{1,3} [(JJ NN .)| (RB JJ ^NN)| (JJ JJ ^NN) | (NN JJ ^NN) | (RB VB .)]"
    val patternExpressionI1 = "(.){2,6}"



    if (algo == "DC") {
      if (queryFrom.isEmpty && queryTo.isEmpty) {
        searchAndCompareDesqTwoCount(path_out, queryL, queryR, patternExp, sigma, k, index, parts, limit)
      } else {
        searchAndCompareDesqMultiCount(path_out, queryFrom, queryTo, queryL, queryR, patternExp, sigma, k, index, parts, limit)
      }
    } else if(algo == "COUNT"){
        searchAndMine(path_out, queryFrom, queryTo, queryL, queryR, patternExp, sigma, k, index, parts, limit)
    }
    else searchAndCompareNaive(path_out, queryR, queryL, patternExp, sigma, k, index, limit, path_out)

    //    TODO: Query for Background and filter the dataset && conjunction of ad-hoc query and background query || Use all queries and simple ad-hoc queries

    println(s"System Runtime: ${
      wallclock.elapsed(TimeUnit.SECONDS)
    }")
    sc.stop()
  }
}
