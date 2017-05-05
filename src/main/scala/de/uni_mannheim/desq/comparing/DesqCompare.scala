package de.uni_mannheim.desq.comparing

import java.io.FileOutputStream

import de.uni_mannheim.desq.Desq.initDesq
import de.uni_mannheim.desq.converters.nyt.NytUtil
import de.uni_mannheim.desq.converters.nyt.avroschema.Sentence
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.WeightedSequence
import de.uni_mannheim.desq.mining.spark.{DesqCount, DesqDataset, DesqMiner, DesqMinerContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.reflect.io.File

/**
  * Created by ivo on 02.05.17.
  *
  * Class which supports building DesqDatasets from NYT Articles in Avro format, as well as comparing these datasets.
  *
  */
class DesqCompare {
  /**
    *
    * @param left              Original DesqDataset for left collection of articles
    * @param right             Original DesqDataset for right collection of articles
    * @param patternExpression Pattern of Interest
    * @param sigma             Sigma for Mining
    * @param k                 Number of Sequences to be returned
    * @param sc                Spark Context
    */
  def compare(left: DesqDataset, right: DesqDataset, patternExpression: String, sigma: Long, k: Int = 20)(implicit sc: SparkContext): Unit = {
    val conf = DesqCount.createConf(patternExpression, sigma)
    conf.setProperty("desq.mining.prune.irrelevant.inputs", true)
    conf.setProperty("desq.mining.use.two.pass", true)
    val ctx = new DesqMinerContext(conf)
    val miner = DesqMiner.create(ctx)
    //    Mine the two datasets
    val left_result = miner.mine(left)
    val right_result = miner.mine(right)
    //    create global dict and complete the other two dictionaries
    val global_dict = createGlobalDictionary(left_result.dict, right_result.dict)
    val global_dict_zeroCounts = global_dict.deepCopy()
    global_dict_zeroCounts.clearFreqs()
    //    left_result.dict.mergeWith(global_dict_zeroCounts)
    //    right_result.dict.mergeWith(global_dict_zeroCounts)

    //    Compare the two results based on interestingness and return the top-K from both datasets
    val result = findTopKPattern(left_result, right_result, k)
    printPattern(result._1, result._2, global_dict, true, k)
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
    val left = DesqDataset.buildFromSentences(leftRaw)
    val savedLeft = left.save("data-local/processed/sparkconvert/left")

    val right = DesqDataset.buildFromSentences(rightRaw)
    val savedRight = right.save("data-local/processed/sparkconvert/right")

    compare(left, right, patternExpression, sigma, k)
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
  def findTopKPattern(left: DesqDataset, right: DesqDataset, k: Int = 20, measure: Int = 1): (Array[(WeightedSequence, Float)], Array[(WeightedSequence, Float)]) = {
    val temp1 = left.sequences.map(ws => (ws, ws.weight))
    val temp2 = right.sequences.map(ws => (ws, ws.weight))
//    simple interestingness
//    val global = temp1.fullOuterJoin(temp2).map(ws => (ws._1, ws._2._1.getOrElse(0L), ws._2._1.getOrElse(0L) / (ws._2._1.getOrElse(0L) + ws._2._2.getOrElse(0L)).toFloat, ws._2._2.getOrElse(0L), ws._2._2.getOrElse(0L) / (ws._2._1.getOrElse(0L) + ws._2._2.getOrElse(0L)).toFloat))
    val global = temp1.fullOuterJoin(temp2).map(ws => (ws._1, ws._2._1.getOrElse(0L), (1 + ws._2._1.getOrElse(0L)) / (1 + ws._2._2.getOrElse(0L)).toFloat, ws._2._2.getOrElse(0L), (1 + ws._2._2.getOrElse(0L)) / (1 + ws._2._1.getOrElse(0L)).toFloat))
    val topleft = global.sortBy(ws => (ws._3, ws._2), ascending = false).take(k).map(ws => (ws._1.withSupport(ws._2), ws._3))
    val topright = global.sortBy(ws => (ws._5, ws._4), ascending = false).take(k).map(ws => (ws._1.withSupport(ws._4), ws._5))
    (topleft, topright)
  }

  //  Merge two RDDs of WeightedSequences
  def mergeSequences(left: RDD[WeightedSequence], right: RDD[WeightedSequence]): RDD[WeightedSequence] = {
    val temp1 = left.map(ws => (ws, ws.weight))
    val temp2 = right.map(ws => (ws, ws.weight))
    //    val global = temp1.join(temp2).map(ws => (ws._1, ws._2._1 + ws._2._2)).map(tuple=>{tuple._1.withSupport(tuple._2)} )
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
  def printPattern(topKSequencesLeft: Array[(WeightedSequence, Float)], topKSequencesRight: Array[(WeightedSequence, Float)], dict: Dictionary, usesFids: Boolean = true, k: Int = 10): Unit = {
    println(s"_____________________ Top ${
      k
    } Interesting Sequences for Left  _____________________")
    print(topKSequencesLeft)

    println(s"_____________________ Top ${
      k
    } Interesting Sequences for Right _____________________")
    print(topKSequencesRight)

    def print(sequences: Array[(WeightedSequence, Float)]) {
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

  def runMiner(data: DesqDataset, ctx: DesqMinerContext): (DesqMiner, DesqDataset) = {
    val miner = DesqMiner.create(ctx)
    val result = miner.mine(data)
    (miner, result)
  }
}
