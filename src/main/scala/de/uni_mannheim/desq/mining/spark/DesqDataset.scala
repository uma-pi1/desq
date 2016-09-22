package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.io.DelSequenceReader
import de.uni_mannheim.desq.mining.WeightedSequence
import de.uni_mannheim.desq.util.DesqProperties
import it.unimi.dsi.fastutil.ints._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by rgemulla on 12.09.2016.
  */
class DesqDataset(val sequences: RDD[WeightedSequence], val dict: Dictionary, val usesFids: Boolean = false) {
  private var serializedDict: Broadcast[Array[Byte]] = _

  def this(sequences: RDD[WeightedSequence], source: DesqDataset, usesFids: Boolean) {
    this(sequences, source.dict, usesFids)
    serializedDict = source.serializedDict
  }

  /** Creates a copy of this DesqDataset with a deep copy of its dictionary. Useful when changes should be
    * performed to a dictionary that has been broadcasted before (and hence cannot/should not be changed). */
  def copy(): DesqDataset = {
    new DesqDataset(sequences, dict.deepCopy(), usesFids)
  }

  /** Returns a broadcast variable that can be used to access the dictionary of this dataset. The broadcast
    * variable stores the dictionary in serialized form for memory efficiency. Use
    * <code>Dictionary.fromBytes(result.value)</code> to get the dictionary at workers.
    */
  def broadcastSerializedDictionary(): Broadcast[Array[Byte]] = {
    if (serializedDict == null) {
      val dict = this.dict
      serializedDict = sequences.context.broadcast(dict.toBytes)
    }
    serializedDict
  }

  /** Returns an RDD that contains for each sequence an array of its string identifiers and its support. */
  //noinspection AccessorLikeMethodIsEmptyParen
  def toSidsSupportPairs(): RDD[(Array[String],Long)] = {
    val serializedDictionary = broadcastSerializedDictionary()
    val usesFids = this.usesFids // to localize

    sequences.mapPartitions(rows => {
      new Iterator[(Array[String],Long)] {
        val dict = Dictionary.fromBytes(serializedDictionary.value)

        override def hasNext: Boolean = rows.hasNext

        override def next(): (Array[String], Long) = {
          val s = rows.next()
          val items = s.items
          val itemSids = new Array[String](items.size)
          for (i <- Range(0,items.size)) {
            if (usesFids) {
              itemSids(i) = dict.getItemByFid(items.get(i)).sid
            } else {
              itemSids(i) = dict.getItemByGid(items.get(i)).sid
            }
          }
          (itemSids, s.support)
        }
      }
    })
  }

  /** Pretty prints up to <code>maxSequences</code> sequences contained in this dataset. */
  def print(maxSequences: Int = -1): Unit = {
    val strings = toSidsSupportPairs().map(s => {
      val sidString = s._1.deep.mkString("[", " ", "]")
      if (s._2 == 1)
        sidString
      else
        sidString + "@" + s._2
    })
    if (maxSequences < 0)
      strings.collect().foreach(println)
    else
      strings.take(maxSequences).foreach(println)
  }

  /** Returns a copy of this dataset with a new dictionary, containing updated counts and fid identifiers. The
    * original input sequences are "translated" to the new dictionary if needed. */
  def copyWithRecomputedCountsAndFids(): DesqDataset = {
    // compute counts
    val usesFids = this.usesFids
    val serializedDict = broadcastSerializedDictionary()
    val totalItemCounts = sequences.mapPartitions(rows => {
      new Iterator[(Int, (Long,Long))] {
        val dict = Dictionary.fromBytes(serializedDict.value)
        val itemCounts = new Int2IntOpenHashMap()
        var currentItemCountsIterator = itemCounts.int2IntEntrySet().fastIterator()
        var currentSupport = 0L
        val ancItems = new IntAVLTreeSet()

        override def hasNext: Boolean = {
          while (!currentItemCountsIterator.hasNext && rows.hasNext) {
            val sequence = rows.next()
            currentSupport = sequence.support
            dict.computeItemFrequencies(sequence.items, itemCounts, ancItems, usesFids)
            currentItemCountsIterator = itemCounts.int2IntEntrySet().fastIterator()
          }
          currentItemCountsIterator.hasNext
        }

        override def next(): (Int, (Long, Long)) = {
          val entry = currentItemCountsIterator.next()
          (entry.getIntKey, (currentSupport, entry.getIntValue*currentSupport))
        }
      }
    }).reduceByKey((c1,c2) => (c1._1+c2._1, c1._2+c2._2)).collect

    // and put them in the dictionary
    val newDict = dict.deepCopy()
    for (itemCount <- totalItemCounts) {
      val item = newDict.getItemByGid(itemCount._1)
      // TODO: drop toInt once items support longs
      item.dFreq = itemCount._2._1.toInt
      item.cFreq = itemCount._2._2.toInt
    }
    newDict.recomputeFids()

    // if we are not using fids, we are done
    if (!usesFids) {
      return new DesqDataset(sequences, newDict, false)
    }

    // otherwise we need to relabel the fids
    val newSerializedDict = sequences.context.broadcast(dict.toBytes)
    val newSequences = sequences.mapPartitions(rows => {
      new Iterator[WeightedSequence] {
        val dict = Dictionary.fromBytes(serializedDict.value)
        val newDict = Dictionary.fromBytes(newSerializedDict.value)

        override def hasNext: Boolean = rows.hasNext

        override def next(): WeightedSequence = {
          val old = rows.next()
          val newItems = new IntArrayList(old.items)
          dict.fidsToGids(newItems)
          newDict.gidsToFids(newItems)
          new WeightedSequence(newItems, old.support)
        }
      }
    })
    val newData = new DesqDataset(newSequences, newDict, true)
    newData.serializedDict = newSerializedDict
    newData
  }

  def mine(minerConf: DesqProperties): DesqDataset = {
    val ctx = new DesqMinerContext(minerConf)
    mine(ctx)
  }

  def mine(ctx: DesqMinerContext): DesqDataset = {
    val miner = DesqMiner.create(ctx)
    mine(miner)
  }

  def mine(miner: DesqMiner): DesqDataset = {
    miner.mine(this)
  }

}

object DesqDataset {
  /** Loads data from the specified del file */
  def fromDelFile(delFile: RDD[String], dict: Dictionary, usesFids: Boolean): DesqDataset = {
    val sequences = delFile.map(line => new WeightedSequence(DelSequenceReader.parseLine(line), 1))
    new DesqDataset(sequences, dict, usesFids)
  }

  /** Loads data from the specified del file */
  def fromDelFile(delFile: String, dict: Dictionary, usesFids: Boolean = false)(implicit sc: SparkContext): DesqDataset = {
    fromDelFile(sc.textFile(delFile), dict, usesFids)
  }
}