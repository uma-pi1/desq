package de.uni_mannheim.desq.comparing

import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import de.uni_mannheim.desq.avro.Sentence
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.elastic.NYTElasticSearchUtils
import de.uni_mannheim.desq.mining.spark._
import de.uni_mannheim.desq.mining.{IdentifiableWeightedSequence, Sequence, WeightedSequence}
import de.uni_mannheim.desq.utilities.OutputPrinter
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by ivo on 02.05.17.
  * Class which supports building DesqDatasets from NYT Articles in Avro format, as well as comparing these datasets.
  *
  */
class DesqCompareNaive(data_path:String)(implicit sc:SparkContext) {
  val dataset: IdentifiableDesqDataset = IdentifiableDesqDataset.load(data_path)
  var queryT = 0L
  var filterT = 0L

  /**
    * Execute Queries and Create Ad-hoc Datasets
    * @param index Name of Index to query
    * @param query_L User-defined Query
    * @param query_R User-defined Query
    * @param limit Result limit
    * @return Tuple of Ad-hoc IdentifiableDesqDatasets
    */
  def createAdhocDatasets(index:String, query_L: String, query_R: String, limit:Int): (IdentifiableDesqDataset, IdentifiableDesqDataset) ={
    val queryTime = Stopwatch.createStarted
    val es = new NYTElasticSearchUtils
    val ids_query_l:Seq[Long] = es.searchES(query_L, index, limit)
    val ids_L= sc.broadcast(ids_query_l)
    val ids_query_r:Seq[Long] = es.searchES(query_R, index, limit)
    val ids_R = sc.broadcast(ids_query_r)
    queryTime.stop()
    queryT = queryTime.elapsed(TimeUnit.SECONDS)
    println(queryT + "s")
    println(ids_query_l.size + ids_query_r.size)
    val filterTime = Stopwatch.createStarted
    val data_L = filterData(ids_L)
    val data_R = filterData(ids_R)
    filterTime.stop()
    filterT = filterTime.elapsed(TimeUnit.SECONDS)

    (data_L, data_R)
  }

  /**
    * Filter the dataset based on document ids
    * @param ids Set of Document IDs (Query Result)
    * @return Ad-hoc Dataset
    */
  def filterData(ids: Broadcast[Seq[Long]]): IdentifiableDesqDataset ={
    val sequences = dataset.sequences.filter(f => ids.value.contains(f.id))
    val parts = Math.max(Math.ceil(sequences.count / 2240000.0).toInt, 32)
    new IdentifiableDesqDataset(sequences.coalesce(parts), dataset.dict.deepCopy(), true)
  }


  /**
    *
    * @param left              Original DesqDataset for left collection of articles
    * @param right             Original DesqDataset for right collection of articles
    * @param patternExpression Pattern of Interest
    * @param sigma             Sigma for Mining
    * @param k                 Number of Sequences to be returned
    * @param sc                Spark Context
    */
  def compare(left: DefaultDesqDataset, right: DefaultDesqDataset, patternExpression: String, sigma: Long, k: Int = 20)(implicit sc: SparkContext): Unit = {
    val conf = DesqCount.createConf(patternExpression, sigma)
    conf.setProperty("desq.mining.prune.irrelevant.inputs", true)
    conf.setProperty("desq.mining.use.two.pass", true)
    val ctx = new DesqMinerContext(conf)
    val miner = DesqMiner.create(ctx)

    val results = if (sigma > 1) {
      //      Mine the data and find those significant differences
      val results = mine(left, right, patternExpression, sigma, k)
      results
    } else {
      //    Mine the two datasets
      val left_result = miner.mine(left.toDefaultDesqDataset())
      val seq_countL = left_result.sequences.count
      left_result.sequences.coalesce(Math.max(Math.ceil(seq_countL / 2000000.0).toInt, 64))

      val right_result = miner.mine(right.toDefaultDesqDataset())
      val seq_countR = right_result.sequences.count
      right_result.sequences.coalesce(Math.max(Math.ceil(seq_countR / 2000000.0).toInt, 64))
      //    Compare the two results based on interestingness and return the top-K from both datasets
      val results = findTopKPattern(left_result, right_result, k, sigma = sigma)
      results
    }
    val global_dict = mergeDictionaries(left.dict, right.dict)
    OutputPrinter.printTable(results, global_dict, false, k)
  }

  /**
    *
    * @param leftRaw           RDD of Sentences which are contained by the Articles of the left Subcollection
    * @param rightRaw          RDD of Sentences which are contained by the Articles of the right Subcollection
    * @param patternExpression Pattern of Interest
    * @param sigma             Sigma for Mining
    * @param k                 Number of Sequences to be returned
    * @param sc                Spark Context
    */
  def buildCompare(leftRaw: RDD[Sentence], rightRaw: RDD[Sentence], patternExpression: String, sigma: Long, k: Int = 20)(implicit sc: SparkContext): Unit = {
    val left = DefaultDesqDataset.buildFromSentences(leftRaw)
    val savedLeft = left.save("data-local/processed/sparkconvert/left")

    val right = DefaultDesqDataset.buildFromSentences(rightRaw)
    val savedRight = right.save("data-local/processed/sparkconvert/right")

    compare(left, right, patternExpression, sigma, k)
  }

  def mine(left: DefaultDesqDataset, right: DefaultDesqDataset, patternExpression: String, sigma: Long, k: Int = 20)(implicit sc: SparkContext): Array[((WeightedSequence, Float), (WeightedSequence, Float))] = {
    val conf = DesqCount.createConf(patternExpression, sigma)
    conf.setProperty("desq.mining.prune.irrelevant.inputs", true)
    conf.setProperty("desq.mining.use.two.pass", true)
    val ctx = new DesqMinerContext(conf)
    val miner = DesqMiner.create(ctx)

    //  The Miner delivers the sequences in the form of fids which need to be converted to gids for comparability
    val result_l = miner.mine(left).toGids().sequences.map(ws => (ws.getUniqueIdentifier, ws))
    val result_r = miner.mine(right).toGids().sequences.map(ws => (ws.getUniqueIdentifier, ws))

    val seq_weight = result_l.fullOuterJoin(result_r).map[(WeightedSequence, Long, Long)] {
      case (k, (lv, None)) => (lv.get, lv.get.weight, 0)
      case (k, (None, rv)) => (rv.get, 0, rv.get.weight)
      case (k, (lv, rv)) => (lv.get, lv.get.weight, rv.get.weight)
    }.cache()

    val left_dict = left.dict
    val right_dict = right.dict

    //    Finding those sequences where one side is less than 2x of support and the other not present
    val rddSecondRun_left = seq_weight.filter(f => f._2 == 0 & f._3 <= 2 * sigma).map(f => {
      left_dict.gidsToFids(f._1)
      new Sequence(f._1).hashCode()
    }).collect.toSeq
    val rddSecondRun_right = seq_weight.filter(f => f._3 == 0 & f._2 <= 2 * sigma).map(f => {
      right_dict.gidsToFids(f._1)
      new Sequence(f._1).hashCode()
    }).collect.toSeq

    //    Create the filter functions for DesqCount
    val filter_left = (seq: (Sequence, Long)) => rddSecondRun_left.contains(seq._1.hashCode())
    val filter_right = (seq: (Sequence, Long)) => rddSecondRun_right.contains(seq._1.hashCode())

    //    Create a new Miner with 1 Support in order to get the missing sequences and their weights
    val conf2 = DesqCount.createConf(patternExpression, 1)
    conf2.setProperty("desq.mining.prune.irrelevant.inputs", true)
    conf2.setProperty("desq.mining.use.two.pass", true)
    val ctx2 = new DesqMinerContext(conf2)
    val miner2 = DesqMiner.create(ctx2)

    //  Mine those sequences
    val additional_l = miner2.mine(left, filter_left).sequences.map(ws => (ws.getUniqueIdentifier, ws))
    val additional_r = miner2.mine(right, filter_right).sequences.map(ws => (ws.getUniqueIdentifier, ws))

    //   Join the original results with the additional sequences

    val full_l = result_l.fullOuterJoin(additional_l).map[(String, WeightedSequence)] {
      case (k, (lv, None)) => (lv.get.getUniqueIdentifier, lv.get)
      case (k, (None, rv)) => (rv.get.getUniqueIdentifier, rv.get)
      case (k, (lv, rv)) => (lv.get.getUniqueIdentifier, lv.get.withSupport(lv.get.weight + rv.get.weight))
    }

    val full_r = result_r.fullOuterJoin(additional_r).map[(String, WeightedSequence)] {
      case (k, (lv, None)) => (lv.get.getUniqueIdentifier, lv.get)
      case (k, (None, rv)) => (rv.get.getUniqueIdentifier, rv.get)
      case (k, (lv, rv)) => (lv.get.getUniqueIdentifier, lv.get.withSupport(lv.get.weight + rv.get.weight))
    }


    //    Join the sequences of both sides and compute the interestingness values
    val global = full_l.fullOuterJoin(full_r).map[(WeightedSequence, Float, Long, Float, Long)] {
      case (k, (lv, None)) => (lv.get, (1 + lv.get.weight) / 1.toFloat, lv.get.weight, 1 / (1 + lv.get.weight).toFloat, 0)
      case (k, (None, rv)) => (rv.get, 1 / (1 + rv.get.weight).toFloat, 0, (1 + rv.get.weight) / 1.toFloat, rv.get.weight)
      case (k, (lv, rv)) => (lv.get, (1 + lv.get.weight) / (1 + rv.get.weight).toFloat, lv.get.weight, (1 + rv.get.weight) / (1 + lv.get.weight).toFloat, rv.get.weight)
    }.cache()

    //    Get the Top k sequences of each side
    val topEverything = global.filter(f => f._3 >= sigma || f._5 >= sigma).sortBy(ws => math.max(ws._2, ws._4), ascending = false).take(k).map(ws => ((ws._1.withSupport(ws._3), ws._2), (ws._1.withSupport(ws._5), ws._4)))
    topEverything
  }

  /**
    * Creates a new Dictionary combining two Dictionaries
    *
    * @param left  Dictionary of the left subcollection
    * @param right Dictionary of the right subcollection
    * @return Merged Dictionary containing items and counts of left and right
    */
  def createGlobalDictionary(left: Dictionary, right: Dictionary): Dictionary = {
    val dict = left.deepCopy()
    dict.mergeWith(right)
    dict
  }

  /**
    * Fills the left Dictionary with the missing values of the right dictionary.
    *
    * @param left      Dictionary of the left subcollection
    * @param right     Dictionary of the right subcollection
    * @param keepFreqs if the frequencies should be kept while filling the dictionary
    * @return
    */
  def mergeDictionaries(left: Dictionary, right: Dictionary, keepFreqs: Boolean = false): Dictionary = {
    if (!keepFreqs) {
      right.clearFreqs()
    }
    left.mergeWith(right)
    left
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
    }.cache()

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



  def runMiner(data: DefaultDesqDataset, ctx: DesqMinerContext): (DesqMiner, DefaultDesqDataset) = {
    val miner = DesqMiner.create(ctx)
    val result = miner.mine(data)
    (miner, result)
  }
}