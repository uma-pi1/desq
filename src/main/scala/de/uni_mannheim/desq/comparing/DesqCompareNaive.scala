package de.uni_mannheim.desq.comparing

import de.uni_mannheim.desq.avro.Sentence
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.spark._
import de.uni_mannheim.desq.mining.{IdentifiableWeightedSequence, Sequence, WeightedSequence}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by ivo on 02.05.17.
  *
  * Class which supports building DesqDatasets from NYT Articles in Avro format, as well as comparing these datasets.
  *
  */
class DesqCompareNaive {
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
      val right_result = miner.mine(right.toDefaultDesqDataset())

      //    Compare the two results based on interestingness and return the top-K from both datasets
      val results = findTopKPattern(left_result, right_result, k, sigma = sigma)
      results
    }
    val global_dict = mergeDictionaries(left.dict, right.dict)
    printTable(results, global_dict, false, k)
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

  /**
    * Prints out the top-K sequences with item sids and interestigness
    *
    * @param topKSequencesLeft  top-k sequences of the left subcollection
    * @param topKSequencesRight top-k sequences of the left subcollection
    * @param dict               Global Dictionary containing all items
    * @param usesFids           Boolean Flag
    * @param k                  Integer
    */
  def printPattern(topKSequencesLeft: Array[(IdentifiableWeightedSequence, Float)], topKSequencesRight: Array[(IdentifiableWeightedSequence, Float)], dict: Dictionary, usesFids: Boolean = false, k: Int = 10): Unit = {
    println(s"_____________________ Top ${
      k
    } Interesting Sequences for Left  _____________________")
    print(topKSequencesLeft)

    println(s"_____________________ Top ${
      k
    } Interesting Sequences for Right _____________________")
    print(topKSequencesRight)

    def print(sequences: Array[(IdentifiableWeightedSequence, Float)]) {
      for (tuple <- sequences) {
        val sids = for (element <- tuple._1.elements()) yield {
          if (usesFids) {
            dict.sidOfFid(element)
          } else {
            dict.sidOfGid(element)
          }
        }
        val output = sids.deep.mkString("[", " ", "]")
        println(output + "@" + tuple._2)
      }
    }
  }

  def printTable(topKSequences: Array[((WeightedSequence, Float), (WeightedSequence, Float))], dict: Dictionary, usesFids: Boolean = false, k: Int = 10): Unit = {

    println(s"| Top ${k.toString} Interesting Sequences | | |")
    println("|--------|--------|--------|")
    print(topKSequences)

    def print(sequences: Array[((WeightedSequence, Float), (WeightedSequence, Float))]) {

      for (s <- sequences) s match {
        case ((s._1._1, s._1._2), (s._2._1, s._2._2)) => {
          val sids = for (e <- s._1._1.elements) yield {
            dict.sidOfGid(e)
          }
          println(s"|${sids.deep.mkString("[", " ", "]")}|${s._1._2}|${s._2._2}|")
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
