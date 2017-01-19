package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.mining.Sequence
import de.uni_mannheim.desq.mining.WeightedSequence
import de.uni_mannheim.desq.util.DesqProperties
import it.unimi.dsi.fastutil.ints._
import de.uni_mannheim.desq.io.MemoryPatternWriter
import de.uni_mannheim.desq.dictionary.BasicDictionary
import it.unimi.dsi.fastutil.objects.{Object2IntMap, Object2IntOpenHashMap, ObjectIterator}

import scala.collection.JavaConverters._
import org.apache.log4j.{LogManager, Logger}

/**
  * Created by alexrenz on 05.10.2016.
  */
class DesqDfs(ctx: DesqMinerContext) extends DesqMiner(ctx) {
  override def mine(data: DesqDataset): DesqDataset = {
    // localize the variables we need in the RDD
    val conf = ctx.conf
    val minSupport = conf.getLong("desq.mining.min.support")
    val dictBroadcast = data.broadcastBasicDictionary()
    val usesFids = data.usesFids
    assert(usesFids)  // assume we are using fids (for now)

    val sendNFAs = conf.getBoolean("desq.mining.send.nfas")
    //val numPartitions = conf.getInt("desq.mining.num.mine.partitions")
    val mapRepartition = conf.getInt("desq.mining.map.repartition")
    val dictHdfs = conf.getString("desq.mining.spark.dict.hdfs", "")
    val reduceShuffleSequences = conf.getBoolean("desq.mining.reduce.shuffle.sequences", false)

    var mappedSequences = data.sequences
    if(mapRepartition > 0) {
      mappedSequences = data.sequences.repartition(mapRepartition)
    }


    // In a first step, we map over each input sequence and output (output item, input sequence) pairs
    //   for all possible output items in that input sequence
    // Second, we group these pairs by key [output item], so that we get an RDD like this:
    //   (output item, Iterable[input sequences])
    // Third step: see below

    val shuffleSequences = mappedSequences.mapPartitions(rows => {

      // for each row, determine the possible output elements from that input sequence and create 
      //   a (output item, input sequence) pair for all of the possible output items
      new Iterator[(Int, WeightedSequence)] {
        val logger = LogManager.getLogger("DesqDfs")

        // retrieve dictionary and setup miner
        val dict:BasicDictionary = dictBroadcast.value
        val baseContext = new de.uni_mannheim.desq.mining.DesqMinerContext()
        baseContext.dict = dict
        baseContext.conf = conf
        val baseMiner = new de.uni_mannheim.desq.mining.DesqDfs(baseContext)

        // determine Output NFAs for given input sequences
        val outputNFAs = new Int2ObjectOpenHashMap[Object2IntOpenHashMap[Sequence]]()
        for(row <- rows) {
          baseMiner.generateOutputNFAs(row, outputNFAs, 0)
        }

        // output (pivot, nfa) pairs
        val partitionIterator = outputNFAs.int2ObjectEntrySet().iterator()
        var nextPartition:Int2ObjectMap.Entry[Object2IntOpenHashMap[Sequence]] = null
        var nfaIterator:ObjectIterator[Object2IntMap.Entry[Sequence]] =  null
        var currentPivot = -1

        override def hasNext: Boolean = {
          if(nfaIterator == null || !nfaIterator.hasNext) {
            if(partitionIterator.hasNext()) {
              nextPartition = partitionIterator.next
              currentPivot = nextPartition.getIntKey()
              nfaIterator = nextPartition.getValue().object2IntEntrySet().fastIterator()
              true
            } else {
              false
            }
          } else {
            true
          }
        }

        override def next(): (Int, WeightedSequence) = {
          val next = nfaIterator.next()
          (currentPivot, next.getKey().withSupport(next.getIntValue))
        }
      }
    })

    // Third, we flatMap over the (output item, Iterable[input sequences]) RDD to mine each partition,
    //   with respect to the pivot item (=output item) of each partition. 
    //   At each partition, we only output sequences where the respective output item is the maximum item
    val patterns = shuffleSequences.groupByKey().mapPartitions(rows => {
      //val patterns = outputItemPartitions.flatMap { (row) =>

      new Iterator[WeightedSequence] {
        // grab the necessary variables

        val dict:BasicDictionary = dictBroadcast.value

        val baseContext = new de.uni_mannheim.desq.mining.DesqMinerContext()
        baseContext.dict = dict
        baseContext.conf = conf

        // Set a memory pattern writer so we are able to retrieve the patterns later
        val result: MemoryPatternWriter = new MemoryPatternWriter()
        baseContext.patternWriter = result

        // Set up the miner
        val baseMiner = new de.uni_mannheim.desq.mining.DesqDfs(baseContext, true) // here in the second stage, we don't use the dfa, so we don't need to build it

        var outputIterator: Iterator[WeightedSequence] = _
        var currentPartition: (Int, Iterable[WeightedSequence]) = _

        var partitionItem: Int = _
        var sequencesIt: Iterator[WeightedSequence] = null


        override def hasNext: Boolean = {
          while ((outputIterator == null || !outputIterator.hasNext) && rows.hasNext) {
            currentPartition = rows.next()
            partitionItem = currentPartition._1
            sequencesIt = currentPartition._2.iterator

            result.clear()

            for (ws <- sequencesIt) {
              if(sendNFAs)
                baseMiner.addNFA(ws)
              else
                baseMiner.addInputSequence(ws, ws.weight, true)
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