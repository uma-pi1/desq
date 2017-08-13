package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.mining.Sequence
import de.uni_mannheim.desq.util.DesqProperties
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.objects.{ObjectIterator, ObjectLists}
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable

/**
  * Created by rgemulla on 14.09.2016.
  */
class DesqCount(ctx: DesqMinerContext) extends DesqMiner(ctx) {

  override def mine(data: DefaultDesqDataset): DefaultDesqDataset = {
    val minSupport = ctx.conf.getLong("desq.mining.min.support")
    val filterFunction = (x: (Sequence, Long)) => x._2 >= minSupport
    mine(data, filterFunction)
  }

  override def mine(data: DefaultDesqDataset, filter: ((Sequence, Long)) => Boolean): DefaultDesqDataset = {
    // localize the variables we need in the RDD
    val dictBroadcast = data.broadcastDictionary()
    val conf = ctx.conf
    val usesFids = data.usesFids
    val minSupport = conf.getLong("desq.mining.min.support")

    // build RDD to perform the mining
    val patterns = data.sequences.mapPartitions(rows => {
      // for each row, get output of FST and produce (output sequence, 1) pair
      new Iterator[(Sequence, Long)] {
        // initialize the sequential desq miner
        val dict = dictBroadcast.value
        val baseContext = new de.uni_mannheim.desq.mining.DesqMinerContext(conf, dict)
        val baseMiner = new de.uni_mannheim.desq.mining.DesqCount(baseContext)
        var outputIterator: ObjectIterator[Sequence] = ObjectLists.emptyList[Sequence].iterator()
        var currentSupport = 0L
        val itemFids = new IntArrayList()

        // here we check if we have an output sequence from the current row; if not, we more to the next row
        // that produces an output
        override def hasNext: Boolean = {
          // do we still have an output sequence from the previous input sequence?
          while (!outputIterator.hasNext && rows.hasNext) {
            // if not, go to the next input sequence
            val s = rows.next()
            currentSupport = s.weight

            // and run sequential DesqCount to get all output sequences produced by that input
            if (usesFids) {
              outputIterator = baseMiner.mine1(s, 1L).iterator()
            } else {
              dict.gidsToFids(s, itemFids)
              outputIterator = baseMiner.mine1(itemFids, 1L).iterator()
            }
          }

          outputIterator.hasNext
        }

        override def next(): (Sequence, Long) = {
          val pattern = outputIterator.next()
          (pattern, currentSupport)
        }
      }
    }).reduceByKey(_ + _) // now sum up counts
      .filter(filter)
      //      .filter(_._2 >= minSupport) // and drop infrequent output sequences
      .map(s => s._1.withSupport(s._2)) // and pack the remaining sequences into a IdentifiableWeightedSequence
    // all done, return result (last parameter is true because mining.DesqCount always produces fids)
    new DefaultDesqDataset(patterns, data, true)
  }

  /**
    * DESQ-TwoCount
    * @param data IndentifiableDesqDataset
    * @param docIDs Corresponding CombIndex
    * @param filter Filter Function for the Results
    * @return DefaultDesqDatasetWithAggregates
    */
  override def mine(data: IdentifiableDesqDataset, docIDs: Broadcast[mutable.Map[Long, mutable.BitSet]], filter: ((Sequence, (Long, Long))) => Boolean): DefaultDesqDatasetWithAggregates = {
    // localize the variables we need in the RDD
    val dictBroadcast = data.broadcastDictionary()
    val docIDsBroadcast = docIDs
    val conf = ctx.conf
    val usesFids = data.usesFids
    val minSupport = conf.getLong("desq.mining.min.support")
    // build RDD to perform the minig
    val patterns = data.sequences.mapPartitions(rows => {
      // for each row, get output of FST and produce (output sequence, 1) pair
      new Iterator[(Sequence, (Long, Long))] {
        // initialize the sequential desq miner
        val dict = dictBroadcast.value
        val docIDs = docIDsBroadcast.value
        val baseContext = new de.uni_mannheim.desq.mining.DesqMinerContext(conf, dict)
        val baseMiner = new de.uni_mannheim.desq.mining.DesqCount(baseContext)
        var outputIterator: ObjectIterator[Sequence] = ObjectLists.emptyList[Sequence].iterator()
        var currentSupportGlobal = 0L
        var currentSupportLocal = 0L
        val itemFids = new IntArrayList()
        // here we check if we have an output sequence from the current row; if not, we more to the next row
        // that produces an output
        override def hasNext: Boolean = {
          // do we still have an output sequence from the previous input sequence?
          while (!outputIterator.hasNext && rows.hasNext) {
            // if not, go to the next input sequence
            val s = rows.next()
            //          pattern matches at least one query hence support is equal to the sequence weight
            currentSupportGlobal = s.weight
            //            check which queries the sequence fulfills
            val bits = docIDs(s.id)
            //          If the docID is relevant for the local query then set the local weight
            if (bits.contains(1)) {
              currentSupportLocal = s.weight
              //              If the docID is relevant for both queries then increase the global weight
              if (bits.contains(2)) {
                currentSupportGlobal += s.weight
              }
            } else currentSupportLocal = 0L
            // and run sequential DesqCount to get all output sequences produced by that input
            if (usesFids) {
              outputIterator = baseMiner.mine1(s, 1L).iterator()
            } else {
              dict.gidsToFids(s, itemFids)
              outputIterator = baseMiner.mine1(itemFids, 1L).iterator()
            }
          }
          outputIterator.hasNext
        }
        override def next(): (Sequence, (Long, Long)) = {
          val pattern = outputIterator.next()
          (pattern, (currentSupportGlobal, currentSupportLocal))
        }
      }
    }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) // now sum up counts
      .filter(filter)
      //      TODO: Change from Global Support to Aggregate Function
      .map(s => s._1.withSupport(-1, s._2._2, s._2._1 - s._2._2)) // and pack the remaining sequences into a IdentifiableWeightedSequence
    // all done, return result (last parameter is true because mining.DesqCount always produces fids)
    new DefaultDesqDatasetWithAggregates(patterns, data.dict, true)
  }

  /**
    * DESQ-MultiCount
    * @param data IndentifiableDesqDataset
    * @param docIDs Corresponding CombIndex
    * @param filter Filter Function for the Results
    * @return DesqDatasetWithAggregate
    */
  //  override def mine(data: IdentifiableDesqDataset, docIDs: mutable.Map[Long, mutable.BitSet], filter: ((Sequence, (Long, Long))) => Boolean): DefaultDesqDatasetWithAggregates = {
  override def mine(data: IdentifiableDesqDataset, docIDs: Broadcast[mutable.Map[Long, mutable.BitSet]], filter: ((Sequence, Array[Long])) => Boolean): DesqDatasetWithAggregate = {
    // localize the variables we need in the RDD
    val dictBroadcast = data.broadcastDictionary()
    val docIDsBroadcast = docIDs
    val conf = ctx.conf
    val usesFids = data.usesFids
    val minSupport = conf.getLong("desq.mining.min.support")
    val counters = conf.getInt("desq.mining.count.datasets")


    val createSeqCountsMap = (seqcounts: (Sequence, Array[Long])) => {
      val map = new mutable.HashMap[Sequence, Array[Long]]()
      map.put(seqcounts._1, seqcounts._2)

      map
    }

    val incSeqCounts = (map: mutable.HashMap[Sequence, Array[Long]], seqcounts: (Sequence, Array[Long])) => {
      val seq = seqcounts._1
      if (map.contains(seq)) {
        map.update(seq, map(seq).zipAll(seqcounts._2, 0, 0).map { case (a: Long, b: Long) => a + b })
      } else map.put(seqcounts._1, seqcounts._2)
      map
    }

    val incCounts = (arr1: Array[Long], arr2: Array[Long]) => {
      arr1.zipAll(arr2, 0, 0).map { case (a: Long, b: Long) => a + b }
      arr1
    }

    val mergeSeqCounts = (map1: mutable.HashMap[Sequence, Array[Long]], map2: mutable.HashMap[Sequence, Array[Long]]) => {
      for (seq: Sequence <- map2.keys) {
        if (map1.contains(seq)) {
          map1.update(seq, map1(seq).zipAll(map2(seq), 0, 0).map { case (a: Long, b: Long) => a + b })
        } else {
          map1.put(seq, map2(seq))
        }
      }
      map1
    }


    // build RDD to perform the minig
    val patterns = data.sequences.mapPartitions(rows => {
      // for each row, get output of FST and produce (output sequence, 1) pair
      //      new Iterator[(Sequence, (Long, Long))] {
      new Iterator[(Int, (Sequence, Array[Long]))] {
        // initialize the sequential desq miner
        val dict = dictBroadcast.value
        val docIDs = docIDsBroadcast.value
        val baseContext = new de.uni_mannheim.desq.mining.DesqMinerContext(conf, dict)
        val baseMiner = new de.uni_mannheim.desq.mining.DesqCount(baseContext)
        var outputIterator: ObjectIterator[Sequence] = ObjectLists.emptyList[Sequence].iterator()
        var currentSupportGlobal = 0L
        var currentSupportLeft = 0L
        var elements = new Array[Long](counters)
        val itemFids = new IntArrayList()

        // here we check if we have an output sequence from the current row; if not, we more to the next row
        // that produces an output
        override def hasNext: Boolean = {
          // do we still have an output sequence from the previous input sequence?
          while (!outputIterator.hasNext && rows.hasNext) {
            // if not, go to the next input sequence
            val s = rows.next()
            val bits = docIDs(s.id)
            elements(0) = s.weight
            //          If the docID is relevant for the local query then set the local weight
            for (x <- 1 until counters) {
              if (bits.contains(x+1)) elements(x) = s.weight else elements(x) = 0
            }
            // and run sequential DesqCount to get all output sequences produced by that input
            if (usesFids) {
              outputIterator = baseMiner.mine1(s, 1L).iterator()
            } else {
              dict.gidsToFids(s, itemFids)
              outputIterator = baseMiner.mine1(itemFids, 1L).iterator()
            }
          }

          outputIterator.hasNext
        }

        override def next(): (Int, (Sequence, Array[Long])) = {
          val pattern = outputIterator.next()
          (pattern.hashCode(), (pattern, elements))
        }
      }
    }).aggregateByKey(new mutable.HashMap[Sequence, Array[Long]])(incSeqCounts, mergeSeqCounts)
      .mapPartitions[(Sequence, Array[Long])](rows => {
      new Iterator[(Sequence, Array[Long])] {
        var outputIterator: Iterator[(Sequence, Array[Long])] = _
        var currentPartition: (Int, mutable.HashMap[Sequence, Array[Long]]) = _

        override def hasNext: Boolean = {
          while ((outputIterator == null || !outputIterator.hasNext) && rows.hasNext) {
            currentPartition = rows.next()
            outputIterator = currentPartition._2.iterator
          }
          if (outputIterator == null) return false
          outputIterator.hasNext
        }

        override def next: (Sequence, Array[Long]) = {
          outputIterator.next()
        }
      }
    })
      .filter(filter)
      .map(s => s._1.withSupport(-1, s._2)) // and pack the remaining sequences into a IdentifiableWeightedSequence
    // all done, return result (last parameter is true because mining.DesqCount always produces fids)
    new DesqDatasetWithAggregate(patterns, data.dict, true)
  }
}


object DesqCount {
  def createConf(patternExpression: String, sigma: Long): DesqProperties = {
    val conf = de.uni_mannheim.desq.mining.DesqCount.createConf(patternExpression, sigma)
    conf.setProperty("desq.mining.miner.class", classOf[DesqCount].getCanonicalName)
    conf
  }
}