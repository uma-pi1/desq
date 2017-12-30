package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.mining._
import de.uni_mannheim.desq.util.DesqProperties
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.objects.{ObjectIterator, ObjectLists}

import scala.reflect.ClassTag

/**
  * Created by rgemulla on 14.09.2016.
  */
class DesqCount(ctx: DesqMinerContext) extends DesqMiner(ctx) {
  override def mine[T](data: DesqDataset[T])(implicit m: ClassTag[T]): DesqDataset[T] = {
    // localize the variables we need in the RDD
    val dictBroadcast = data.broadcastDictionary()
    val sequenceInterpreterBroadcast = data.broadcastSequenceInterpreter()
    val conf = ctx.conf
    val usesFids = data.usesFids
    val minSupport = conf.getLong("desq.mining.min.support")

    // build RDD to perform the minig
    val patterns = data.sequences.mapPartitions(rows => {
      // for each row, get output of FST and produce (output sequence, 1) pair
      new Iterator[(Sequence,Long)] {
        // initialize the sequential desq miner
        val dict = dictBroadcast.value
        val sequenceInterpreter = sequenceInterpreterBroadcast.value
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
            currentSupport = sequenceInterpreter.getWeight(s)

            // and run sequential DesqCount to get all output sequences produced by that input
            if (usesFids) {
              outputIterator = baseMiner.mine1(sequenceInterpreter.getSequence(s), 1L).iterator()
            } else {
              dict.gidsToFids(sequenceInterpreter.getSequence(s), itemFids)
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
    }).reduceByKey(_ + _) // now sum up count
      .filter(_._2 >= minSupport) // and drop infrequent output sequences
      .map(s => sequenceInterpreterBroadcast.value.packSequence(s._1, s._2)) // and pack the remaining sequences into a Sequence

    // all done, return result (last parameter is true because mining.DesqCount always produces fids)
    new DesqDataset[T](patterns, data, true)
  }
}

object DesqCount {
  def createConf(patternExpression: String, sigma: Long): DesqProperties = {
    val conf = de.uni_mannheim.desq.mining.DesqCount.createConf(patternExpression, sigma)
    conf.setProperty("desq.mining.miner.class", classOf[DesqCount].getCanonicalName)
    conf
  }
}