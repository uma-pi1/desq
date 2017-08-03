package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.mining.Sequence
import de.uni_mannheim.desq.util.DesqProperties
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.objects.{ObjectIterator, ObjectLists}

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

    // build RDD to perform the minig
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


  override def mine(data: IdentifiableDesqDataset, docIDs: mutable.Map[Long, mutable.BitSet], filter: ((Sequence, (Long, Long))) => Boolean): DefaultDesqDatasetWithAggregates = {
    // localize the variables we need in the RDD
    val dictBroadcast = data.broadcastDictionary()
    val docIDsBroadcast = data.sequences.context.broadcast[mutable.Map[Long, mutable.BitSet]](docIDs)
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
}


object DesqCount {
  def createConf(patternExpression: String, sigma: Long): DesqProperties = {
    val conf = de.uni_mannheim.desq.mining.DesqCount.createConf(patternExpression, sigma)
    conf.setProperty("desq.mining.miner.class", classOf[DesqCount].getCanonicalName)
    conf
  }
}