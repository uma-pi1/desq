package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.mining.{OutputNFA, Sequence, WeightedSequence}
import de.uni_mannheim.desq.util.DesqProperties
import it.unimi.dsi.fastutil.ints._
import de.uni_mannheim.desq.io.MemoryPatternWriter
import de.uni_mannheim.desq.dictionary.BasicDictionary
import it.unimi.dsi.fastutil.objects._

import scala.collection.JavaConverters._
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.Partitioner

import collection.JavaConversions._

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

    val shuffleSequences = mappedSequences.mapPartitions(inputSequences => {

      // for each row, determine the possible output elements from that input sequence and create 
      //   a (output item, input sequence) pair for all of the possible output items
      new Iterator[(Int, Sequence)] {
        val logger = LogManager.getLogger("DesqDfs")

        // retrieve dictionary and setup miner
        val dict:BasicDictionary = dictBroadcast.value
        val baseContext = new de.uni_mannheim.desq.mining.DesqMinerContext()
        baseContext.dict = dict
        baseContext.conf = conf
        val baseMiner = new de.uni_mannheim.desq.mining.DesqDfs(baseContext)

        // output (pivot, nfa) pairs
        var nfaIterator:ObjectIterator[OutputNFA] = null
        var pivotIterator:IntIterator = null
        var currentInputSequence:Sequence = null

        override def hasNext: Boolean = {
          if(sendNFAs) {
            while ((nfaIterator == null || !nfaIterator.hasNext) && inputSequences.hasNext) {
              nfaIterator = baseMiner.generateOutputNFAs(inputSequences.next).getNFAs().iterator(); //int2ObjectEntrySet().fastIterator()
            }
            return nfaIterator.hasNext
          } else {
            while ((pivotIterator == null || !pivotIterator.hasNext) && inputSequences.hasNext) {
              currentInputSequence = inputSequences.next()
              pivotIterator = baseMiner.generatePivotItems(currentInputSequence).iterator();
            }
            return pivotIterator.hasNext
          }
        }

        override def next(): (Int, Sequence) = {
          if(sendNFAs) {
            val nfa = nfaIterator.next()
            (nfa.pivot, nfa.mergeAndSerialize)
          } else {
            (pivotIterator.nextInt(), currentInputSequence.clone())
          }
        }
      }
    })

//    Code for using a partitioner with reduce and then groupByKey():
//    val partitioner = new PivotHashPartitioner(shuffleSequences.getNumPartitions)

//    var processedShuffleSequences = shuffleSequences
//    if(reduceShuffleSequences) {
//      processedShuffleSequences = shuffleSequences.reduceByKey(partitioner, _ + _)
//    }

//    processedShuffleSequences.mapPartitionsWithIndex{(partitionIndex ,dataIterator) =>
//      dataIterator.map(dataInfo => (dataInfo +" is at partition " + partitionIndex))
//    }.foreach(println)


    // Combine into HashMaps
    val createMap = (nfa:Sequence) => {
      val map = new Object2LongOpenHashMap[Sequence]()
      map.put(nfa, 1)
      map
    }

    val addToMap = (map: Object2LongOpenHashMap[Sequence], nfa:Sequence) => {
      map.addTo(nfa, 1)
      map
    }

    val mergeMaps = (map1: Object2LongOpenHashMap[Sequence], map2: Object2LongOpenHashMap[Sequence]) => {
      for(entry: Object2LongMap.Entry[Sequence] <- map2.object2LongEntrySet()) {
        map1.addTo(entry.getKey, entry.getLongValue)
      }
      map1
    }

    val combinedSeqs = shuffleSequences.combineByKey(createMap, addToMap, mergeMaps)

    // Third, we flatMap over the (output item, Iterable[input sequences]) RDD to mine each partition,
    //   with respect to the pivot item (=output item) of each partition. 
    //   At each partition, we only output sequences where the respective output item is the maximum item
    val patterns = combinedSeqs.mapPartitions(rows => {
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
        var currentPartition: (Int, Object2LongOpenHashMap[Sequence]) = _
        var partitionItem: Int = _


        override def hasNext: Boolean = {
          while ((outputIterator == null || !outputIterator.hasNext) && rows.hasNext) {
            currentPartition = rows.next()
            partitionItem = currentPartition._1

            result.clear()

            if(!sendNFAs) {
                val sequencesIt = currentPartition._2.object2LongEntrySet()
                for(row : Object2LongMap.Entry[Sequence] <- sequencesIt) {
                    baseMiner.addInputSequence(row.getKey, row.getLongValue, true)
                }

                // mine the added input sequences
                baseMiner.minePivot(partitionItem);
            } else {
              // mine the passed NFAs
              baseMiner.mineNFAs(partitionItem, currentPartition._2)
            }

            // TODO: find a better way to get a Java List accepted as a Scala TraversableOnce
            outputIterator = result.getPatterns().asScala.iterator
          }
          if(outputIterator == null) return false
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


/**
  Custom partitioner for tuples of (pivot item, nfa), partitioning only by the pivot item
 */
class PivotHashPartitioner(partitions: Int) extends Partitioner {
  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case pivNFA: (Int, Sequence) => PivotHashPartitioner.nonNegativeMod(pivNFA._1.hashCode(), numPartitions)
    case piv: Int => PivotHashPartitioner.nonNegativeMod(piv.hashCode(), numPartitions)
    case _ => println("any type"); PivotHashPartitioner.nonNegativeMod(key.hashCode(), numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: PivotHashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}

object PivotHashPartitioner {
  // copy from org.apache.spark.util.Utils, as Utils is package private
  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }
}