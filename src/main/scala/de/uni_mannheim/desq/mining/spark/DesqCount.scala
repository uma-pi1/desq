package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.mining.{DesqCount, WeightedSequence}
import de.uni_mannheim.desq.util.DesqProperties
import it.unimi.dsi.fastutil.ints.{IntArrayList, IntList}
import it.unimi.dsi.fastutil.objects.ObjectLists.EmptyList
import it.unimi.dsi.fastutil.objects.{ObjectIterator, ObjectLists}
import org.apache.commons.configuration2.{Configuration, PropertiesConfiguration}

/**
  * Created by rgemulla on 14.09.2016.
  */
class DesqCount(_ctx: DesqMinerContext) extends DesqMiner(_ctx) {
  override def mine(data: DesqDataset): DesqDataset = {
    val dict = data.dict;
    val conf = ctx.conf
    val usesFids = data.usesFids
    val minSupport = conf.getLong("desq.mining.min.support")

    // build RDD to perform the minig
    val patterns = data.sequences.mapPartitions(rows => {
      // initialize the sequential desq miner
      val baseContext = new de.uni_mannheim.desq.mining.DesqMinerContext
      baseContext.dict = dict
      baseContext.conf = conf
      val baseMiner = new de.uni_mannheim.desq.mining.DesqCount(baseContext)
      var outputIterator: ObjectIterator[IntList] = ObjectLists.emptyList[IntList].iterator()
      var currentSupport = 0L
      val itemFids = new IntArrayList

      // for each row, get output of FST and produce (output sequence, 1) pair
      new Iterator[(IntList,Long)] {
        override def hasNext: Boolean = {
          while (!outputIterator.hasNext && rows.hasNext) {
            val s = rows.next
            currentSupport = s.support
            if (usesFids) {
              outputIterator = baseMiner.mine1(s.items).iterator()
            } else {
              dict.gidsToFids(s.items, itemFids)
              outputIterator = baseMiner.mine1(itemFids).iterator()
            }
          }
          return outputIterator.hasNext
        }

        override def next(): (IntList, Long) = {
          (outputIterator.next, currentSupport)
        }
      }
    }).reduceByKey(_ + _) // now count
      .filter(_._2 >= minSupport) // and drop infrequent onces
      .map(s => new WeightedSequence(s._1, s._2)) // and pack into a WeightedSequence

    // all done, return result (last parameter is true because mining.DesqCount produces fids)
    new DesqDataset(patterns, dict, true)
  }
}

object DesqCount {
  def createConf(patternExpression: String, sigma: Long): DesqProperties = {
    val conf = de.uni_mannheim.desq.mining.DesqCount.createConf(patternExpression, sigma)
    conf.setProperty("desq.mining.miner.class", classOf[DesqCount].getCanonicalName)
    conf
  }
}