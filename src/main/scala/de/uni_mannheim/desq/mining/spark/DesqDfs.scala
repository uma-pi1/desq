package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.mining.Sequence
import de.uni_mannheim.desq.mining.WeightedSequence
import de.uni_mannheim.desq.util.DesqProperties
import it.unimi.dsi.fastutil.ints._
import de.uni_mannheim.desq.io.MemoryPatternWriter

import scala.collection.JavaConverters._
import org.apache.log4j.{LogManager, Logger}

/**
  * Created by alexrenz on 05.10.2016.
  */
class DesqDfs(ctx: DesqMinerContext) extends DesqMiner(ctx) {
  override def mine(data: DesqDataset): DesqDataset = {
    // localize the variables we need in the RDD
    val dictBroadcast = data.broadcastDictionary()
    val conf = ctx.conf
    val usesFids = data.usesFids
    assert(usesFids)  // assume we are using fids (for now)

    val minSupport = conf.getLong("desq.mining.min.support")
    val sendNFAs = conf.getBoolean("desq.mining.send.nfas")
    //val numPartitions = conf.getInt("desq.mining.num.mine.partitions")


    // In a first step, we map over each input sequence and output (output item, input sequence) pairs
    //   for all possible output items in that input sequence
    // Second, we group these pairs by key [output item], so that we get an RDD like this:
    //   (output item, Iterable[input sequences])
    // Third step: see below

    val outputItemPartitions = data.sequences.mapPartitions(rows => {

      // for each row, determine the possible output elements from that input sequence and create 
      //   a (output item, input sequence) pair for all of the possible output items
      new Iterator[(Int, Sequence)] {
        val logger = LogManager.getLogger("DesqDfs")
        var t1 = System.nanoTime
        // initialize the sequential desq miner
        val dict = dictBroadcast.value
        //val item1 = dict.getItemByFid(1)
        val t1_1 = System.nanoTime()
        logger.fatal("map-buildDict: " + (t1_1-t1) / 1e9d + "s")

        val baseContext = new de.uni_mannheim.desq.mining.DesqMinerContext()
        baseContext.dict = dict
        baseContext.conf = conf
        val baseMiner = new de.uni_mannheim.desq.mining.DesqDfs(baseContext)

        val t1_2 = System.nanoTime()
        logger.fatal("map-constructMiner: " + (t1_2-t1_1) / 1e9d + "s")
        
        var outputIterator: IntIterator = new IntArraySet(0).iterator
        var currentSupport = 0L
        var itemFids = new IntArrayList
        var currentSequence : WeightedSequence = _

        val t2 = System.nanoTime()
        val mapSetupTime = (t2 - t1) / 1e9d
        logger.fatal("map-setup: " + mapSetupTime + "s")
        var serializedNFAs = new Int2ObjectOpenHashMap[Sequence]()

        // We want to output all (output item, input sequence) pairs for the current
        //   sequence. Once we are done, we move on to the next input sequence in this partition.
        override def hasNext: Boolean = {
          // do we still have pairs to output for this sequence?
          while (!outputIterator.hasNext && rows.hasNext) {
            // if not, go to the next input sequence
            currentSequence = rows.next()
            currentSupport = currentSequence.weight

            if(sendNFAs) {
              serializedNFAs = baseMiner.createNFAPartitions(currentSequence, false, 0)

              outputIterator = serializedNFAs.keySet().iterator()
            } else {
              outputIterator = baseMiner.getPivotItemsOfOneSequence(currentSequence).iterator()
            }
          }

          if(!outputIterator.hasNext) {
            val t3 = System.nanoTime()
            val mapTime = (t3 - t2) / 1e9d
            logger.fatal("map-processing: " + mapTime + "s")
          }
          outputIterator.hasNext
        }

        // we simply output (output item, input sequence)
        override def next(): (Int, Sequence) = {
          val partitionItem : Int = outputIterator.next()
          if(sendNFAs) {
            val partitionNFA = serializedNFAs.get(partitionItem)
            (partitionItem, partitionNFA.clone())
          } else {
            (partitionItem, currentSequence.clone())
          }
        }
      }
    }).groupByKey()

    // Third, we flatMap over the (output item, Iterable[input sequences]) RDD to mine each partition,
    //   with respect to the pivot item (=output item) of each partition. 
    //   At each partition, we only output sequences where the respective output item is the maximum item
    val patterns = outputItemPartitions.mapPartitions(rows => {
      //val patterns = outputItemPartitions.flatMap { (row) =>

      new Iterator[WeightedSequence] {
        // grab the necessary variables
        val dict = dictBroadcast.value
        val baseContext = new de.uni_mannheim.desq.mining.DesqMinerContext()
        baseContext.dict = dict
        baseContext.conf = conf

        // Set a memory pattern writer so we are able to retrieve the patterns later
        val result: MemoryPatternWriter = new MemoryPatternWriter()
        baseContext.patternWriter = result

        // Set up the miner
        val baseMiner = new de.uni_mannheim.desq.mining.DesqDfs(baseContext)

        var outputIterator: Iterator[WeightedSequence] = _
        var currentPartition: (Int, Iterable[Sequence]) = _

        var partitionItem: Int = _
        var sequencesIt: Iterator[Sequence] = null


        override def hasNext: Boolean = {
          while ((outputIterator == null || !outputIterator.hasNext) && rows.hasNext) {
            currentPartition = rows.next()
            partitionItem = currentPartition._1
            sequencesIt = currentPartition._2.iterator

            baseMiner.clear(false)
            result.clear()

            for (s <- sequencesIt) {
              if(sendNFAs)
                baseMiner.addNFA(s, 1, true)
              else
                baseMiner.addInputSequence(s, 1, true)
            }

            // Mine this partition, only output patterns where the output item is the maximum item
            baseMiner.minePivot(partitionItem)

            // TODO: find a better way to get a Java List accepted as a Scala TraversableOnce
            outputIterator = result.getPatterns().asScala.iterator
          }
          outputIterator.hasNext
        }

        override def next: WeightedSequence = {
          outputIterator.next()
        }
      }
    })

    // all done, return result (last parameter is true because mining.DesqCount always produces fids)
    new DesqDataset(patterns, data, true)

  }
}

object DesqDfs {
  def createConf(patternExpression: String, sigma: Long): DesqProperties = {
    val conf = de.uni_mannheim.desq.mining.DesqDfs.createConf(patternExpression, sigma)
    conf.setProperty("desq.mining.miner.class", classOf[DesqDfs].getCanonicalName)
    conf
  }
}