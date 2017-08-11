package de.uni_mannheim.desq.comparing

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.elastic.NYTElasticSearchUtils
import de.uni_mannheim.desq.mining.spark._
import de.uni_mannheim.desq.mining.{DesqCount => _, DesqMiner => _, DesqMinerContext => _, _}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.collection.mutable

/**
  * Created by ivo on 05.07.17.
  */
class DesqCompare(data_path: String, partitions: Int = 96)(implicit sc: SparkContext) {
  val es = new NYTElasticSearchUtils
  val dataset = if (partitions == 0) {
    IdentifiableDesqDataset.load(data_path)
  } else if (!Files.exists(Paths.get(s"$data_path/partitioned/"))) {
    val dataset = IdentifiableDesqDataset.load(data_path)
    println("Dataset Partitioning unknown and will be repartitioned with 96 partitions!")
    val sequences = dataset.sequences.keyBy(_.id).partitionBy(new HashPartitioner(96))
    val datasetPartitioned = new DesqDatasetPartitionedWithID[IdentifiableWeightedSequence](sequences, dataset.dict, true)
    datasetPartitioned.save(s"$data_path/partitioned/96")
    println(s"Repartitioned Data is stored to $data_path/partitioned/96")
    datasetPartitioned
  } else {
    val datasetPartitioned: DesqDatasetPartitionedWithID[IdentifiableWeightedSequence] = {

      val datasetPartitioned = if (!Files.exists(Paths.get(s"$data_path/partitioned/$partitions"))) {
        println(s"Dataset partitioned with $partitions cannot be found! Repartitioning!")
        val dataset = DesqDatasetPartitionedWithID.load[IdentifiableWeightedSequence](s"$data_path/partitioned/96")
        val datasetRepartitioned = dataset.repartition[IdentifiableWeightedSequence](new HashPartitioner(partitions))
        datasetRepartitioned.save(s"$data_path/partitioned/$partitions")
        println(s"Repartitioned Data is stored to $data_path/partitioned/$partitions")
        datasetRepartitioned
      } else DesqDatasetPartitionedWithID.load[IdentifiableWeightedSequence](s"$data_path/partitioned/$partitions")
      datasetPartitioned
    }
    datasetPartitioned
  }

  var queryT = 0L
  var filterT = 0L

  /**
    * Create the Ad-hoc DesqDataset
    * @param index ElasticSearch Index to Query
    * @param query_B Optional: Query to specify the overall dataset
    * @param query_L Query for the "Left Dataset"
    * @param query_R Query for the "Right Dataset"
    * @param limit Limit of results for a query
    * @return (Ad-hoc Dataset, Broadcast[Combined Index]
    */
  def createAdhocDatasets(index: String, queryFrom: String, queryTo: String, query_L: String, query_R: String, limit: Int = 10000): (IdentifiableDesqDataset, Broadcast[mutable.Map[Long, mutable.BitSet]]) = {
    println("Initialize Dataset with ES Query")
    val queryTime = Stopwatch.createStarted
    val docIDMap = if (queryTo =="" && queryFrom == "") {
      es.searchESCombines(index, limit, query_L, query_R)
    } else{
      es.searchESWithDateRangeBackground(index,limit,  queryFrom, queryTo, query_L, query_R)
    }
    val ids = sc.broadcast(docIDMap)
    queryTime.stop()
    queryT = queryTime.elapsed(TimeUnit.SECONDS)

    println("Initialize Dataset with ES Query")
    val leftPrepTime = Stopwatch.createStarted

    val adhocDataset = if (partitions == 0) {
      val sequences = dataset.asInstanceOf[IdentifiableDesqDataset].sequences
      val filtered_sequences = sequences.filter(f => ids.value.contains(f.id))
      new IdentifiableDesqDataset(filtered_sequences.repartition(Math.max(Math.ceil(filtered_sequences.count / 50000.0).toInt, 32)), dataset.asInstanceOf[IdentifiableDesqDataset].dict.deepCopy(), true)
    } else {
      val sequences = dataset.asInstanceOf[DesqDatasetPartitionedWithID[IdentifiableWeightedSequence]].sequences
      val keys = ids.value.keySet
      val parts = keys.map(_.## % sequences.partitions.length)
      val filtered_sequences: RDD[IdentifiableWeightedSequence] = sequences.mapPartitionsWithIndex((i, iter) =>
        if (parts.contains(i)) iter.filter { case (k, v) => keys.contains(k) }
        else Iterator()).map(k => k._2)
      val seqs_filt =  filtered_sequences.repartition(Math.max(Math.ceil(filtered_sequences.count /50000.0).toInt, 32))
      new IdentifiableDesqDataset(
        seqs_filt
        , dataset.asInstanceOf[DesqDatasetPartitionedWithID[IdentifiableWeightedSequence]].dict.deepCopy()
        , true
      )
    }
    leftPrepTime.stop()
    filterT = leftPrepTime.elapsed(TimeUnit.SECONDS)
    println(s"Filtering Dataset took: ${leftPrepTime.elapsed(TimeUnit.SECONDS)}s")
    (adhocDataset, ids)
  }


  /**
    *
    * @param data              Original DesqDataset for left collection of articles
    * @param patternExpression Pattern of Interest
    * @param sigma             Sigma for Mining
    * @param k                 Number of Sequences to be returned
    * @param sc                Spark Context
    * @return resulting Aggregated Sequences and their Interestingness Scores
    */
  def compare(data: IdentifiableDesqDataset, docIDMap: Broadcast[mutable.Map[Long, mutable.BitSet]], patternExpression: String, sigma: Long, k: Int = 20)(implicit sc: SparkContext): Array[(AggregatedWeightedSequence, Float, Float)] = {
    val conf = DesqCount.createConf(patternExpression, sigma)
    conf.setProperty("desq.mining.prune.irrelevant.inputs", true)
    conf.setProperty("desq.mining.use.two.pass", true)
    val ctx = new DesqMinerContext(conf)
    val miner = DesqMiner.create(ctx)

    val filter = (x: (Sequence, (Long, Long))) => (x._2._1 - x._2._2) >= sigma || x._2._2 >= sigma

    val results = miner.mine(data, docIDMap, filter)

    val seq_count = results.sequences.count
    results.sequences.repartition(Math.max(Math.ceil(seq_count / 2000000.0).toInt, 32))

    //    Join the sequences of both sides and compute the interestingness values
    val global = results.sequences.mapPartitions[(AggregatedWeightedSequence, Float, Float)](rows => {
      new Iterator[(AggregatedWeightedSequence, Float, Float)] {

        override def hasNext: Boolean = rows.hasNext

        override def next(): (AggregatedWeightedSequence, Float, Float) = {
          val oldSeq = rows.next()
          val interestingness_l = (1 + oldSeq.weight) / (1 + oldSeq.weight_other).toFloat
          val interestingness_r = (1 + oldSeq.weight_other) / (1 + oldSeq.weight).toFloat
          val newSeq = oldSeq.clone()
          (newSeq, interestingness_l, interestingness_r)
        }
      }
    })

    //    Get the Top k sequences of each side
    //    val topEverything = global.filter(f => f._1.weight >= sigma || f._1.weight_other >= sigma).sortBy(ws => math.max(ws._2, ws._3), ascending = false).take(k)
    val topEverything = global.sortBy(ws => math.max(ws._2, ws._3), ascending = false).take(k)
    //    val topEverything = global.sortBy(ws => ws._1.weight_other, ascending = false ).take(k)
    printTable(topEverything, results.dict, false, k)
    topEverything
  }

  /**
    * Variant that finds interesting phrases based on the same background
    * e.g. "New York" and "Washingtion" based on "*"
    *
    * @param data              Original DesqDataset for left collection of articles
    * @param patternExpression Pattern of Interest
    * @param sigma             Sigma for Mining
    * @param k                 Number of Sequences to be returned
    * @param sc                Spark Context
    * @return resulting Aggregated Sequences and their Interestingness Scores
    */
  def compareWithBackground(data: IdentifiableDesqDataset, docIDMap: Broadcast[mutable.Map[Long, mutable.BitSet]], patternExpression: String, sigma: Long, k: Int = 20)(implicit sc: SparkContext): Array[(AggregatedSequence, Float, Float)] = {
    val conf = DesqCount.createConf(patternExpression, sigma)
    conf.setProperty("desq.mining.prune.irrelevant.inputs", true)
    conf.setProperty("desq.mining.use.two.pass", true)
    val ctx = new DesqMinerContext(conf)
    val miner = DesqMiner.create(ctx)

    val filter = (x: (Sequence, Array[Long])) => {
      var temp = false
      val bool = for (c <- x._2) {
        if (c >= sigma) temp = true
      }
      temp
    }

    val results = miner.mine(data, docIDMap, filter)
    val seq_count = results.sequences.count
    results.sequences.repartition(Math.ceil(seq_count / 2000000.0).toInt)
    val intResults = results.sequences.mapPartitions[(AggregatedSequence, Float, Float)](rows => {
      new Iterator[(AggregatedSequence, Float, Float)] {
        override def hasNext: Boolean = rows.hasNext

        override def next(): (AggregatedSequence, Float, Float) = {
          val oldSeq = rows.next
          val interestingness_l = (1 + oldSeq.support(1)) / (1 + oldSeq.support(0)).toFloat
          val interestingness_r = (1 + oldSeq.support(2)) / (1 + oldSeq.support(0)).toFloat
          val newSeq = oldSeq.clone()
          (newSeq, interestingness_l, interestingness_r)
        }
      }
    })
    //    val topEverything = intResults.sortBy(ws => math.max(ws._2, ws._3), ascending = false).take(k)
    val topEverything = intResults.sortBy(ws => ws._1.support(0), ascending = false).take(k)
    printTableWB(topEverything, results.dict, false, k)
    topEverything

  }

  /**
    * Compares two sets of mined sequences. Their Interestingess is calculated by comparing the local frequency inside the dataset
    * with the overall frequency of the pattern in both datasets. If a pattern was not mined in the other dataset the frequency is set to 0.
    *
    * @param left  Sequences that are mined by desq
    * @param right Sequences that are mined by desq
    * @param k     Number of Interesting Phrases to return
    * @return (Top-K sequences of left, Top-K sequences of right)
    */
  def findTopKPattern(left: DefaultDesqDataset, right: DefaultDesqDataset, k: Int = 20, measure: Int = 1, sigma: Long)(implicit sc: SparkContext): (Array[((WeightedSequence, Float), (WeightedSequence, Float))]) = {

    //  The Miner delivers the sequences in the form of fids which need to be converted to gids for comparability
    val temp1 = left.toGids().sequences.map(ws => (ws.getUniqueIdentifier, ws))
    val temp2 = right.toGids().sequences.map(ws => (ws.getUniqueIdentifier, ws))


    //    case class SequenceWithScores(sequence: IdentifiableWeightedSequence, leftScore: Float, leftSupport: Long, rightScore: Float, rightSupport: Long)
    //    Join the sequences of both sides and compute the interestigness values
    val global = temp1.fullOuterJoin(temp2).map[(WeightedSequence, Float, Long, Float, Long)] {
      case (k, (lv, None)) => (lv.get, (1 + lv.get.weight) / 1.toFloat, lv.get.weight, 1 / (1 + lv.get.weight).toFloat, 0)
      case (k, (None, rv)) => (rv.get, 1 / (1 + rv.get.weight).toFloat, 0, (1 + rv.get.weight) / 1.toFloat, rv.get.weight)
      case (k, (lv, rv)) => (lv.get, (1 + lv.get.weight) / (1 + rv.get.weight).toFloat, lv.get.weight, (1 + rv.get.weight) / (1 + lv.get.weight).toFloat, rv.get.weight)
    }

    val topEverything = global.filter(f => f._3 >= sigma || f._5 >= sigma).sortBy(ws => math.max(ws._2, ws._4), ascending = false).take(k).map(ws => ((ws._1.withSupport(ws._3), ws._2), (ws._1.withSupport(ws._5), ws._4)))
    topEverything
  }

  //  Merge two RDDs of WeightedSequences
  def mergeSequences(left: RDD[WeightedSequence], right: RDD[WeightedSequence]): RDD[WeightedSequence] = {
    val temp1 = left.map(ws => (ws, ws.weight))
    val temp2 = right.map(ws => (ws, ws.weight))
    val global = temp1.join(temp2).map(ws => ws._1.withSupport(ws._2._1 + ws._2._2))
    global
  }

  /**
    * Prints out the top-K sequences with item sids and interestigness
    *
    * @param topKSequences top-k sequences for the two subcollections
    * @param dict          Global Dictionary containing all items
    * @param usesFids      Boolean Flag
    * @param k             Integer
    */
  def printPattern(topKSequences: Array[(AggregatedWeightedSequence, Float, Float)], dict: Dictionary, usesFids: Boolean = false, k: Int = 10): Unit = {
    println(s"_____________________ Top ${
      k
    } Interesting Sequences for Left  _____________________")
    print(topKSequences)

    def print(sequences: Array[(AggregatedWeightedSequence, Float, Float)]) {
      for (tuple <- sequences) {
        val sids = for (element <- tuple._1.elements()) yield {
          if (usesFids) {
            dict.sidOfFid(element)
          } else {
            dict.sidOfGid(element)
          }
        }
        val output = sids.deep.mkString("[", " ", "]")
        println(output + "@left<" + tuple._2 + ">@right<" + tuple._3 + ">")
      }
    }
  }

  def printTable(topKSequences: Array[(AggregatedWeightedSequence, Float, Float)], dict: Dictionary, usesFids: Boolean = false, k: Int = 10): Unit = {

    println(s"| Top ${k.toString} Interesting Sequences | | | | |")
    println("|Seq|Freq All | Interestigness All | Freq Right| Interestingness Right ")
    println("|--------|--------|--------|--------|--------|")
    print(topKSequences)

    def print(sequences: Array[((AggregatedWeightedSequence, Float, Float))]) {

      for (s <- sequences) s match {
        case ((s._1, s._2, s._3)) => {
          val sids = for (e <- s._1.elements) yield {
            dict.sidOfFid(e)
          }
          println(s"|${sids.deep.mkString("[", " ", "]")}|${s._1.weight}|${s._2}|${s._3}|${s._1.weight_other}|")
        }
      }
    }
  }

  def printTableWB(topKSequences: Array[(AggregatedSequence, Float, Float)], dict: Dictionary, usesFids: Boolean = false, k: Int = 10): Unit = {

    println(s"| Top ${k.toString} Interesting Sequences | | | | |")
    println("|Seq |Freq All | Freq Left | Interestingness Left | Freq Right| Interestingness Right ")
    println("|--------|--------|--------|--------|--------|--------|")
    print(topKSequences)

    def print(sequences: Array[((AggregatedSequence, Float, Float))]) {

      for (s <- sequences) s match {
        case ((s._1, s._2, s._3)) => {
          val sids = for (e <- s._1.elements) yield {
            dict.sidOfFid(e)
          }
          println(s"|${sids.deep.mkString("[", " ", "]")}|${s._1.support(0)}|${s._1.support(1)}|${s._2}|${s._1.support(2)}|${s._3}|")
        }
      }
    }
  }

  def runMiner(data: DefaultDesqDataset, ctx: DesqMinerContext): (DesqMiner, DefaultDesqDataset) = {
    val miner = DesqMiner.create(ctx)
    val result = miner.mine(data)
    (miner, result)
  }
}

