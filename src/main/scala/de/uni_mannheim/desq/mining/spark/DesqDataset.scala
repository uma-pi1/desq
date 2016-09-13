package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.io.{DelPatternReader, DelSequenceReader}
import de.uni_mannheim.desq.mining.WeightedSequence
import it.unimi.dsi.fastutil.ints.{Int2IntMap, Int2IntOpenHashMap, IntArrayList, IntOpenHashSet}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions

/**
  * Created by rgemulla on 12.09.2016.
  */
class DesqDataset(_sequences: RDD[WeightedSequence], _dict: Dictionary, _usesFids: Boolean = false) {
  val sequences: RDD[WeightedSequence] = _sequences
  val dict = _dict
  val usesFids = _usesFids


  def toSidRDD(): RDD[(Array[String],Long)] = {
    val dict = this.dict // to make it local
    if (usesFids) {
      return sequences.map(s => {
        val itemFids = s.getItems
        val itemSids = new Array[String](itemFids.size)
        for (i <- Range(0,itemFids.size)) {
          itemSids(i) = dict.getItemByFid(itemFids.get(i)).sid
        }
        (itemSids, s.getSupport)
      })
    } else {
      return sequences.map(s => {
        val itemGids = s.getItems
        val itemSids = new Array[String](itemGids.size)
        for (i <- Range(0,itemGids.size)) {
          itemSids(i) = dict.getItemByGid(itemGids.get(i)).sid
        }
        (itemSids, s.getSupport)
      })
    }
  }

  /**
    *
    * TODO: slow implementation; performance optimizations
    */
  @throws[IllegalStateException]("if usesFid is set")
  def recomputeDictionaryCountsAndFids(): Unit = {
    val usesFids = this.usesFids
    if (usesFids) {
      throw new IllegalStateException("only applicable when usesFids=false")
    }

    // compute counts
    val dict = this.dict // to make it local
    val itemCounts = sequences.flatMap(s => {
      val itemCounts = new Int2IntOpenHashMap
      dict.computeItemFrequencies(s.getItems, itemCounts, new IntOpenHashSet, usesFids)
      val result = new Array[(Int, (Long, Long))](itemCounts.size())
      var i=0
      for (entry <- JavaConversions.mapAsScalaMap(itemCounts)) {
        result(i) = (entry._1, (s.getSupport, entry._2*s.getSupport))
        i += 1
      }
      result
    }).reduceByKey((c1,c2) => (c1._1+c2._1, c1._2+c2._2)).collect

    // and put them in the dictionary
    for (itemCount <- itemCounts) {
      val item = dict.getItemByGid(itemCount._1)
      // TODO: drop toInt once items support longs
      item.dFreq = itemCount._2._1.toInt
      item.cFreq = itemCount._2._2.toInt
    }
    dict.recomputeFids
  }
}

object DesqDataset {
  def fromDelFile(delFile: RDD[String], dict: Dictionary, usesFids: Boolean): DesqDataset = {
    val sequences = delFile.map(s => {
      val items = new IntArrayList()
      DelSequenceReader.parseLine(s, items)
      new WeightedSequence(items, 1)
    })
    new DesqDataset(sequences, dict, usesFids)
  }

  def fromDelFile(implicit sc: SparkContext, delFile: String, dict: Dictionary, usesFids: Boolean = false): DesqDataset = {
    fromDelFile(sc.textFile(delFile), dict, usesFids)
  }
}