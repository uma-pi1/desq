package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.io.{DelPatternReader, DelSequenceReader}
import de.uni_mannheim.desq.mining.WeightedSequence
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.objects.ObjectIterator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions
import scala.runtime.RichInt

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
        val itemFids = s.items
        val itemSids = new Array[String](itemFids.size)
        for (i <- Range(0,itemFids.size)) {
          itemSids(i) = dict.getItemByFid(itemFids.get(i)).sid
        }
        (itemSids, s.support)
      })
    } else {
      return sequences.map(s => {
        val itemGids = s.items
        val itemSids = new Array[String](itemGids.size)
        for (i <- Range(0,itemGids.size)) {
          itemSids(i) = dict.getItemByGid(itemGids.get(i)).sid
        }
        (itemSids, s.support)
      })
    }
  }

  @throws[IllegalStateException]("if usesFid is set")
  def recomputeDictionaryCountsAndFids(): Unit = {
    val usesFids = this.usesFids
    if (usesFids) {
      throw new IllegalStateException("only applicable when usesFids=false")
    }

    // compute counts
    val dict = this.dict // to make it local
    val totalItemCounts = sequences.mapPartitions(rows => {
      new Iterator[(Int, (Long,Long))] {
        val itemCounts = new Int2IntOpenHashMap
        var currentItemCountsIterator = itemCounts.int2IntEntrySet().fastIterator()
        var currentSupport = 0L
        val ancItems = new IntAVLTreeSet

        override def hasNext: Boolean = {
          while (!currentItemCountsIterator.hasNext && rows.hasNext) {
            val sequence = rows.next
            currentSupport = sequence.support
            dict.computeItemFrequencies(sequence.items, itemCounts, ancItems, usesFids)
            currentItemCountsIterator = itemCounts.int2IntEntrySet().fastIterator()
          }
          return currentItemCountsIterator.hasNext
        }

        override def next(): (Int, (Long, Long)) = {
          val entry = currentItemCountsIterator.next()
          (entry.getIntKey, (currentSupport, entry.getIntValue*currentSupport))
        }
      }
    }).reduceByKey((c1,c2) => (c1._1+c2._1, c1._2+c2._2)).collect

    // and put them in the dictionary
    for (itemCount <- totalItemCounts) {
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
    val sequences = delFile.map(line => new WeightedSequence(DelSequenceReader.parseLine(line), 1))
    new DesqDataset(sequences, dict, usesFids)
  }

  def fromDelFile(implicit sc: SparkContext, delFile: String, dict: Dictionary, usesFids: Boolean = false): DesqDataset = {
    fromDelFile(sc.textFile(delFile), dict, usesFids)
  }
}