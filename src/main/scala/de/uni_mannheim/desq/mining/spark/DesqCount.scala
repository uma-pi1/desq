package de.uni_mannheim.desq.mining.spark

import java.util.Collections

import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.WeightedSequence
import de.uni_mannheim.desq.util.DesqProperties
import it.unimi.dsi.fastutil.ints.{IntArrayList, IntList}
import it.unimi.dsi.fastutil.objects.{ObjectIterator, ObjectLists}

/**
  * Created by rgemulla on 14.09.2016.
  */
class DesqCount(_ctx: DesqMinerContext) extends DesqMiner(_ctx) {
  override def mine(data: DesqDataset): DesqDataset = {
    // localize the variables we need in the RDD
    val serializedDict = data.broadcastSerializedDictionary()
    val conf = ctx.conf
    val usesFids = data.usesFids
    val minSupport = conf.getLong("desq.mining.min.support")

    // build RDD to perform the minig
    val patterns = data.sequences.mapPartitions(rows => {
      // for each row, get output of FST and produce (output sequence, 1) pair
      new Iterator[(IntList,Long)] {
        // initialize the sequential desq miner
        val dict = Dictionary.fromBytes(serializedDict.value)
        val baseContext = new de.uni_mannheim.desq.mining.DesqMinerContext
        baseContext.dict = dict
        baseContext.conf = conf
        val baseMiner = new de.uni_mannheim.desq.mining.DesqCount(baseContext)
        var outputIterator: ObjectIterator[IntList] = ObjectLists.emptyList[IntList].iterator()
        var currentSupport = 0L
        val itemFids = new IntArrayList
        val reversedOutput = baseMiner.mine1reversedOutput()

        // here we check if we have an output sequence from the current row; if not, we more to the next row
        // that produces an output
        override def hasNext: Boolean = {
          // do we still have an output sequence from the previous input sequence?
          while (!outputIterator.hasNext && rows.hasNext) {
            // if not, go to the next input sequence
            val s = rows.next
            currentSupport = s.support

            // and run sequential DesqCount to get all output sequences produced by that input
            if (usesFids) {
              outputIterator = baseMiner.mine1(s.items).iterator()
            } else {
              dict.gidsToFids(s.items, itemFids)
              outputIterator = baseMiner.mine1(itemFids).iterator()
            }
          }

          outputIterator.hasNext
        }

        override def next(): (IntList, Long) = {
          val pattern = outputIterator.next
          if (reversedOutput) {
            // will change the pattern in our outputIterator as well, but that's fine as we don't need it again
            Collections.reverse(pattern)
          }
          (pattern, currentSupport)
        }
      }
    }).reduceByKey(_ + _) // now sum up count
      .filter(_._2 >= minSupport) // and drop infrequent output sequences
      .map(s => new WeightedSequence(s._1, s._2)) // and pack the remaining sequences into a WeightedSequence

    // all done, return result (last parameter is true because mining.DesqCount always produces fids)
    new DesqDataset(patterns, data, true)
  }
}

object DesqCount {
  def createConf(patternExpression: String, sigma: Long): DesqProperties = {
    val conf = de.uni_mannheim.desq.mining.DesqCount.createConf(patternExpression, sigma)
    conf.setProperty("desq.mining.miner.class", classOf[DesqCount].getCanonicalName)
    conf
  }
}