package de.uni_mannheim.desq.comparing

import de.uni_mannheim.desq.avro.Sentence
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.{AggregatedWeightedSequence, IdentifiableWeightedSequence, Sequence, WeightedSequence}
import de.uni_mannheim.desq.mining.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by ivo on 05.07.17.
  */
class DesqCompare {
  /**
    *
    * @param data              Original DesqDataset for left collection of articles
    * @param patternExpression Pattern of Interest
    * @param sigma             Sigma for Mining
    * @param k                 Number of Sequences to be returned
    * @param sc                Spark Context
    * @return resulting Aggregated Sequences and their Interestingness Scores
    */
  def compare(data: IdentifiableDesqDataset, docIDMap: mutable.Map[Long, mutable.BitSet], patternExpression: String, sigma: Long, k: Int = 20)(implicit sc: SparkContext):  Array[(AggregatedWeightedSequence, Float, Float)] = {
    val conf = DesqCount.createConf(patternExpression, sigma)
    conf.setProperty("desq.mining.prune.irrelevant.inputs", true)
    conf.setProperty("desq.mining.use.two.pass", true)
    val ctx = new DesqMinerContext(conf)
    val miner = DesqMiner.create(ctx)

    val filter = (x:(Sequence, (Long, Long)))=>(x._2._1-x._2._2) >= sigma || x._2._2 >= sigma

    val results = miner.mine(data, docIDMap, filter)
    results

    //    Join the sequences of both sides and compute the interestingness values
    val global = results.sequences.mapPartitions[(AggregatedWeightedSequence, Float, Float)] (rows => {
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
    printTable(topEverything, results.dict, false, k)
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

  /**
    * Prints out the top-K sequences with item sids and interestigness
    *
    * @param topKSequences      top-k sequences for the two subcollections
    * @param dict               Global Dictionary containing all items
    * @param usesFids           Boolean Flag
    * @param k                  Integer
    */
  def printPattern(topKSequences: Array[(AggregatedWeightedSequence, Float, Float)], dict: Dictionary, usesFids: Boolean = false, k: Int = 10): Unit = {
    println(s"_____________________ Top ${
      k
    } Interesting Sequences for Left  _____________________")
    print(topKSequences)

    def print(sequences: Array[(AggregatedWeightedSequence, Float,Float)]) {
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

    println(s"| Top ${k.toString} Interesting Sequences | | |")
    println("|--------|--------|--------|")
    print(topKSequences)

    def print(sequences: Array[((AggregatedWeightedSequence, Float, Float))]) {

      for (s <- sequences) s match {
        case ((s._1, s._2, s._3)) => {
          val sids = for (e <- s._1.elements) yield {
            dict.sidOfFid(e)
          }
          println(s"|${sids.deep.mkString("[", " ", "]")}|${s._2}|${s._3}|")
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

