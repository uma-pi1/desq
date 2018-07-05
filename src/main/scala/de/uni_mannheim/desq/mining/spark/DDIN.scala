package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.mining.{OutputNFA, RelevantPositions, Sequence, WeightedSequence}
import de.uni_mannheim.desq.util.{DesqProperties, PrimitiveUtils}
import it.unimi.dsi.fastutil.ints._
import de.uni_mannheim.desq.io.MemoryPatternWriter
import de.uni_mannheim.desq.dictionary.BasicDictionary
import it.unimi.dsi.fastutil.objects._

import scala.collection.JavaConverters._
import collection.JavaConversions._
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD


/**
  * High-level code for running DESQ with item-based partitioning
  * Created by alexrenz on 05.10.2016.
  */
class DDIN(ctx: DesqMinerContext) extends DesqMiner(ctx) {
    override def mine(data: DesqDataset): DesqDataset = {
        // localize the variables we need in the RDD
        val conf = ctx.conf
        val minSupport = conf.getLong("desq.mining.min.support")
        val dictBroadcast = data.broadcastBasicDictionary()
        val usesFids = data.usesFids
        assert(usesFids) // assume we are using fids (for now)

        /** Flags for algorithm variants */
        val mapRepartition = conf.getInt("desq.mining.map.repartition")  // if >0: repartition dataset before mining
        val sendNFAs = conf.getBoolean("desq.mining.send.nfas")  // if true: send NFA. otherwise: send input sequences
        val mergeSuffixes = conf.getBoolean("desq.mining.merge.suffixes", false) // if true: merge the suffixes of the NFA
        val aggregateShuffleSequences = conf.getBoolean("desq.mining.aggregate.shuffle.sequences", false)  // if true: aggregate NFA for the shuffle and the local mining
        val trimInputSequences = conf.getBoolean("desq.mining.trim.input.sequences", false)
        val trimInputSequencesAdvanced = conf.getBoolean("desq.mining.trim.input.sequences.advanced", false)
        val useGrid = ctx.conf.getBoolean("desq.mining.use.grid", false);

        // manual repartition
        var mappedSequences = data.sequences
        if (mapRepartition > 0) {
            mappedSequences = data.sequences.repartition(mapRepartition)
        }


        // Some intuition:
        // In a first step, we map over each input sequence, determine the pivot items for the input sequence, and output
        // one of the following two for each pivot item:
            // [if sendNFAs==false]: (pivot item, input sequence) pairs
            // [if sendNFAs==true]: (pivot item, nfa that encodes candidate sequences for that pivot) pairs
        // Second, we group these pairs by key (pivot item) and mine the partition for each pivot item in parallel

        // optionally (if mergeSuffixes==true), we merge common suffixes of the NFAs
        // optionally (if aggregateShuffleSequences==true), we aggregate the produced NFAs by count


        // step 1: process each input sequence in parallel
        val shuffleSequences = mappedSequences.mapPartitions(inputSequences => {

            // process each input sequence
            new Iterator[(Int, Sequence)] {
                val logger = LogManager.getLogger("DesqDfs")

                // retrieve dictionary and setup miner
                val dict: BasicDictionary = dictBroadcast.value
                val baseContext = new de.uni_mannheim.desq.mining.DesqMinerContext()
                baseContext.dict = dict
                baseContext.conf = conf
                val baseMiner = new de.uni_mannheim.desq.mining.DesqDfs(baseContext)

                var nfaIterator: ObjectIterator[Sequence] = null
                var pivotIterator: IntIterator = null
                var currentInputSequence: Sequence = null
                var totalNFAs = 0
                var stoppedNFAs = 0

                val relevantPositions = new RelevantPositions()
                override def hasNext: Boolean = {
                    // we either send NFA that encode the candidate sequences
                    if (sendNFAs) {
                        while ((nfaIterator == null || !nfaIterator.hasNext) && inputSequences.hasNext) {
                            currentInputSequence = inputSequences.next()
                            nfaIterator = baseMiner.generateNFAs(currentInputSequence).iterator();
                        }
                        if(!nfaIterator.hasNext) {
                            // cleanup
                            // ...
                            return false
                        } else {
                            return true
                        }
//                        return nfaIterator.hasNext
                    } else { // or we send input sequences
                        while ((pivotIterator == null || !pivotIterator.hasNext) && inputSequences.hasNext) {
                            currentInputSequence = inputSequences.next()
                            relevantPositions.clear()
                            pivotIterator = baseMiner.generatePivotItems(currentInputSequence, relevantPositions).iterator()
                        }
                        return pivotIterator.hasNext
                    }
                }

                override def next(): (Int, Sequence) = {
                    if (sendNFAs) {
                        val nfa = nfaIterator.next()
                        totalNFAs = totalNFAs + 1

                        // we appended the pivot item to the sequence. now we extract it and use it as key
                        val pivot = nfa.getInt(nfa.size()-1)
                        nfa.size(nfa.size()-1)

                        (pivot, nfa)
                    } else {
                        val pivot = pivotIterator.nextInt()
                        if(trimInputSequences) {
                            if(useGrid) {
                                if (trimInputSequencesAdvanced) {
                                    val minimumOutputItemAtPosition = baseMiner.minimumOutputItemAtPosition()
                                    val potentiallyIrrelevantPositions = baseMiner.potentiallyIrrelevantPositions()
                                    val minMax = baseMiner.minMaxForCurrentInputSeq(pivot)
                                    val sendSeq = currentInputSequence.cloneSubListWithRelevantPositionsForPivot(
                                        minimumOutputItemAtPosition,
                                        potentiallyIrrelevantPositions,
                                        pivot,
                                        PrimitiveUtils.getLeft(minMax),
                                        PrimitiveUtils.getRight(minMax))
                                    (pivot, sendSeq)
                                } else {
                                    val minMax = baseMiner.minMaxForCurrentInputSeq(pivot)
                                    val sendSeq = currentInputSequence.cloneSubList(PrimitiveUtils.getLeft(minMax), PrimitiveUtils.getRight(minMax)-1)
                                    (pivot, sendSeq)
                                }
                            } else {
                                val sendSeq = currentInputSequence.cloneSubList(relevantPositions.getFirstRelevant(pivot), relevantPositions.getLastRelevant(pivot))
                                (pivot, sendSeq)
                            }
                        } else {
                            (pivot, currentInputSequence.clone())
                        }
                    }
                }
            }
        })

        var patterns: RDD[WeightedSequence] = null

        /* For step 2, we have two variants, depending on the value set for aggregateBySequences:
           (1) aggregate the sequences/NFAs that we send to the partitions by count: combineByKey [if aggregateBySequences==true]
           (2) don't aggregate: groupByKey
           */

        /** Variant (1): combineByKey */
        if (aggregateShuffleSequences) {
            // create
            val createMap = (nfa: Sequence) => {
                val map = new Object2LongOpenHashMap[Sequence]()
                map.put(nfa, 1)
                map
            }

            // add
            val addToMap = (map: Object2LongOpenHashMap[Sequence], nfa: Sequence) => {
                map.addTo(nfa, 1)
                map
            }

            // combine
            val mergeMaps = (map1: Object2LongOpenHashMap[Sequence], map2: Object2LongOpenHashMap[Sequence]) => {
                for (entry: Object2LongMap.Entry[Sequence] <- map2.object2LongEntrySet()) {
                    map1.addTo(entry.getKey, entry.getLongValue)
                }
                map1
            }

            val combinedSeqs = shuffleSequences.combineByKey(createMap, addToMap, mergeMaps)


            // We now mine each partition
            // At each partition, we only output sequences where the respective output item is the maximum item
            patterns = combinedSeqs.mapPartitions(rows => {
                //val patterns = outputItemPartitions.flatMap { (row) =>

                new Iterator[WeightedSequence] {
                    // grab the necessary variables

                    val dict: BasicDictionary = dictBroadcast.value

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

                            if (!sendNFAs) { // we sent input sequences to the partitions
                                val sequencesIt = currentPartition._2.object2LongEntrySet()
                                baseMiner.preparePartition(partitionItem)
                                for (row: Object2LongMap.Entry[Sequence] <- sequencesIt) {
                                    baseMiner.addInputSequence(row.getKey, row.getLongValue, true)
                                }

                                // mine the added input sequences
                                baseMiner.minePartition()
                            } else {
                                // mine the passed NFAs
                                baseMiner.mineNFAs(partitionItem, currentPartition._2)
                            }

                            outputIterator = result.getPatterns().asScala.iterator
                        }
                        if (outputIterator == null) return false
                        outputIterator.hasNext
                    }

                    override def next: WeightedSequence = {
                        outputIterator.next()
                    }
                }
            })

        /** Variant (2): groupByKey */
        } else {

            patterns = shuffleSequences.groupByKey().mapPartitions(rows => {
                //val patterns = outputItemPartitions.flatMap { (row) =>

                new Iterator[WeightedSequence] {
                    // grab the necessary variables

                    val dict: BasicDictionary = dictBroadcast.value

                    val baseContext = new de.uni_mannheim.desq.mining.DesqMinerContext()
                    baseContext.dict = dict
                    baseContext.conf = conf

                    // Set a memory pattern writer so we are able to retrieve the patterns later
                    val result: MemoryPatternWriter = new MemoryPatternWriter()
                    baseContext.patternWriter = result

                    // Set up the miner
                    val baseMiner = new de.uni_mannheim.desq.mining.DesqDfs(baseContext, true) // here in the second stage, we don't use the dfa, so we don't need to build it

                    var outputIterator: Iterator[WeightedSequence] = _
                    var currentPartition: (Int, Iterable[Sequence]) = _
                    var partitionItem: Int = _


                    override def hasNext: Boolean = {
                        while ((outputIterator == null || !outputIterator.hasNext) && rows.hasNext) {
                            currentPartition = rows.next()
                            partitionItem = currentPartition._1

                            result.clear()

                            if (!sendNFAs) { // we sent input sequences to the partitions
                                baseMiner.preparePartition(partitionItem)
                                val sequencesIt = currentPartition._2.iterator
                                for (seq <- sequencesIt) {
                                    baseMiner.addInputSequence(seq, 1, true)
                                }

                                // mine the added input sequences
                                baseMiner.minePartition();
                            } else {
                                // mine the passed NFAs
                                baseMiner.mineNFAs(partitionItem, currentPartition._2.asJava)
                            }

                            outputIterator = result.getPatterns().asScala.iterator
                        }
                        if (outputIterator == null) return false
                        outputIterator.hasNext
                    }

                    override def next: WeightedSequence = {
                        outputIterator.next()
                    }
                }
            })
        }


        // all done, return result (last parameter is true because mining.DesqCount always produces fids)
        new DesqDataset(patterns, data, true)
    }
}

object DDIN {
    def createConf(patternExpression: String, sigma: Long): DesqProperties = {
        val conf = de.uni_mannheim.desq.mining.DesqDfs.createConf(patternExpression, sigma)
        conf.setProperty("desq.mining.miner.class", classOf[DDIN].getCanonicalName)
        conf
    }
}

