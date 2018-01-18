package de.uni_mannheim.desq.mining.spark

import java.net.URI
import java.util.zip.GZIPInputStream

import de.uni_mannheim.desq.avro.AvroDesqDatasetDescriptor
import de.uni_mannheim.desq.dictionary.{DefaultSequenceBuilder, Dictionary, DictionaryBuilder}
import de.uni_mannheim.desq.io.DelSequenceReader
import de.uni_mannheim.desq.mining.WeightedSequence
import it.unimi.dsi.fastutil.ints.{Int2LongOpenHashMap, IntAVLTreeSet}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by rgemulla on 12.09.2016.
  */
class DesqDataset(override val sequences: RDD[WeightedSequence], override val sequenceInterpreter: WeightedSequenceInterpreter) extends GenericDesqDataset[WeightedSequence](sequences, sequenceInterpreter) {
  private var sequenceInterpreterBroadcast: Broadcast[WeightedSequenceInterpreter] = _

  // -- building ------------------------------------------------------------------------------------------------------

  def this(sequences: RDD[WeightedSequence], source: DesqDataset) {
    this(sequences, source.sequenceInterpreter)
    sequenceInterpreterBroadcast = source.sequenceInterpreterBroadcast
  }

  /** Returns a copy of this dataset with a new dictionary, containing updated counts and fid identifiers. The
    * original input sequences are "translated" to the new dictionary if needed. */
  def copyWithRecomputedCountsAndFids(): DesqDataset = {
    // compute counts
    val usesFids = sequenceInterpreter.usesFids
    val sequenceInterpreterBroadcast = broadcastSequenceInterpreter()
    val totalItemFreqs = sequences.mapPartitions(rows => {
      new Iterator[(Int, (Long,Long))] {
        val sequenceInterpreter = sequenceInterpreterBroadcast.value
        val itemCfreqs = new Int2LongOpenHashMap()
        var currentItemCfreqsIterator = itemCfreqs.int2LongEntrySet().fastIterator()
        var currentWeight = 0L
        val ancItems = new IntAVLTreeSet()

        override def hasNext: Boolean = {
          while (!currentItemCfreqsIterator.hasNext && rows.hasNext) {
            val sequence = rows.next()
            currentWeight = sequenceInterpreter.getWeight(sequence)
            if(usesFids) {
              sequenceInterpreter.getDictionary.computeItemCfreqs(sequenceInterpreter.getFids(sequence), itemCfreqs, ancItems, usesFids, 1)
            } else {
              sequenceInterpreter.getDictionary.computeItemCfreqs(sequenceInterpreter.getGids(sequence), itemCfreqs, ancItems, usesFids, 1)
            }
            currentItemCfreqsIterator = itemCfreqs.int2LongEntrySet().fastIterator()
          }
          currentItemCfreqsIterator.hasNext
        }

        override def next(): (Int, (Long, Long)) = {
          val entry = currentItemCfreqsIterator.next()
          (entry.getIntKey, (currentWeight, entry.getLongValue*currentWeight))
        }
      }
    }).reduceByKey((c1,c2) => (c1._1+c2._1, c1._2+c2._2)).collect

    // and put them in the dictionary
    val newDict = sequenceInterpreter.getDictionary.deepCopy()
    newDict.clearFreqs() // reset all frequencies to 0 (important for items that do not occur in totalItemFreqs)
    for (itemFreqs <- totalItemFreqs) {
      val fid = if (usesFids) itemFreqs._1 else newDict.fidOf(itemFreqs._1)
      newDict.setDfreqOf(fid, itemFreqs._2._1)
      newDict.setCfreqOf(fid, itemFreqs._2._2)
    }
    newDict.recomputeFids()

    if (!usesFids) {
      // if we are not using fids, we are done
      val newSequenceInterpreter = new WeightedSequenceInterpreter(false)
      newSequenceInterpreter.setDictionary(newDict)

      new DesqDataset(sequences, newSequenceInterpreter)
    } else {
      // otherwise we need to relabel the fids
      val newDictBroadcast = sequences.context.broadcast(newDict)
      val newSequences = sequences.mapPartitions(rows => {
        new Iterator[WeightedSequence] {
          val newDict = newDictBroadcast.value
          val sequenceInterpreter = sequenceInterpreterBroadcast.value

          override def hasNext: Boolean = rows.hasNext

          override def next(): WeightedSequence = {
            val oldSeq = rows.next()
            val newSeq = sequenceInterpreter.getCopy(oldSeq)
            sequenceInterpreter.getDictionary.fidsToGids(sequenceInterpreter.getFids(newSeq))
            newDict.gidsToFids(sequenceInterpreter.getFids(newSeq))
            newSeq
          }
        }
      })
      val newSequenceInterpreter = new WeightedSequenceInterpreter(true)
      newSequenceInterpreter.setDictionary(newDict)

      new DesqDataset(sequences, newSequenceInterpreter)
    }
  }

  // -- conversion ----------------------------------------------------------------------------------------------------

  /** Returns dataset with sequences encoded as Fids.
    *  If sequences are encoded as gids, they are converted to fids. Otherwise, nothing is done.
    */
  //noinspection AccessorLikeMethodIsEmptyParen
  def toFids(): DesqDataset = {
    val usesFids = sequenceInterpreter.usesFids
    val sequenceInterpreterBroadcast = broadcastSequenceInterpreter()

    if (usesFids) {
      this
    } else {
      val newSequences = sequences.mapPartitions(rows => {
        new Iterator[WeightedSequence] {
          val sequenceInterpreter = sequenceInterpreterBroadcast.value

          override def hasNext: Boolean = rows.hasNext

          override def next(): WeightedSequence = {
            val oldSeq = rows.next()
            val newSeq = sequenceInterpreter.getCopy(oldSeq)
            sequenceInterpreter.getDictionary.gidsToFids(sequenceInterpreter.getGids(newSeq))
            newSeq
          }
        }
      })

      val newSequenceInterpreter = new WeightedSequenceInterpreter(true)
      newSequenceInterpreter.setDictionary(sequenceInterpreter.getDictionary)

      new DesqDataset(newSequences, newSequenceInterpreter)
    }
  }

  /** Returns dataset with sequences encoded as Gids.
    *  If sequences are encoded as fids, they are converted to gids. Otherwise, nothing is done.
    */
  //noinspection AccessorLikeMethodIsEmptyParen
  def toGids(): DesqDataset = {
    val usesFids = sequenceInterpreter.usesFids
    val sequenceInterpreterBroadcast = broadcastSequenceInterpreter()

    if (!usesFids) {
      this
    } else {
      val newSequences = sequences.mapPartitions(rows => {
        new Iterator[WeightedSequence] {
          val sequenceInterpreter = sequenceInterpreterBroadcast.value

          override def hasNext: Boolean = rows.hasNext

          override def next(): WeightedSequence = {
            val oldSeq = rows.next()
            val newSeq = sequenceInterpreter.getCopy(oldSeq)
            sequenceInterpreter.getDictionary.fidsToGids(sequenceInterpreter.getFids(newSeq))
            newSeq
          }
        }
      })

      val newSequenceInterpreter = new WeightedSequenceInterpreter(false)
      newSequenceInterpreter.setDictionary(sequenceInterpreter.getDictionary)

      new DesqDataset(newSequences, newSequenceInterpreter)
    }
  }
}

object DesqDataset {
  // -- I/O -----------------------------------------------------------------------------------------------------------

  def load(inputPath: String)(implicit sc: SparkContext): DesqDataset = {
    val fileSystem = FileSystem.get(new URI(inputPath), sc.hadoopConfiguration)

    // read descriptor
    var descriptor = new AvroDesqDatasetDescriptor()
    val descriptorPath = s"$inputPath/descriptor.json"
    val descriptorIn = fileSystem.open(new Path(descriptorPath))
    val reader = new SpecificDatumReader[AvroDesqDatasetDescriptor](classOf[AvroDesqDatasetDescriptor])
    val decoder = DecoderFactory.get.jsonDecoder(descriptor.getSchema, descriptorIn)
    descriptor = reader.read(descriptor, decoder)
    descriptorIn.close()

    // read dictionary
    val dictPath = s"$inputPath/dict.avro.gz"
    val dictIn = fileSystem.open(new Path(dictPath))
    val dict = new Dictionary()
    dict.readAvro(new GZIPInputStream(dictIn))
    dictIn.close()

    // read sequences
    val sequencePath = s"$inputPath/sequences"
    val sequences = sc.sequenceFile(sequencePath, classOf[NullWritable], classOf[WeightedSequence]).map(kv => kv._2)

    val sequenceInterpreter = new WeightedSequenceInterpreter(descriptor.getUsesFids)
    sequenceInterpreter.setDictionary(dict)

    // return the dataset
    new DesqDataset(sequences, sequenceInterpreter)
  }

  /** Loads data from the specified del file */
  def loadFromDelFile(delFile: RDD[String], dict: Dictionary, usesFids: Boolean): DesqDataset = {
    val sequences = delFile.map(line => {
      val s = new WeightedSequence(Array.empty[Int], 1L)
      DelSequenceReader.parseLine(line, s)
      s
    })

    val sequenceInterpreter = new WeightedSequenceInterpreter(usesFids)
    sequenceInterpreter.setDictionary(dict)

    new DesqDataset(sequences, sequenceInterpreter)
  }

  /** Loads data from the specified del file */
  def loadFromDelFile(delFile: String, dict: Dictionary, usesFids: Boolean = false)(implicit sc: SparkContext): DesqDataset = {
    loadFromDelFile(sc.textFile(delFile), dict, usesFids)
  }

  // -- building ------------------------------------------------------------------------------------------------------

  /** Builds a DesqDataset from an RDD of string arrays. Every array corresponds to one sequence, every element to
    * one item. The generated hierarchy is flat. */
  def buildFromStrings(rawData: RDD[Array[String]]): DesqDataset = {
    val parse = (strings: Array[String], seqBuilder: DictionaryBuilder) => {
      seqBuilder.newSequence(1)
      for (string <- strings) {
        seqBuilder.appendItem(string)
      }
    }
    build[Array[String]](rawData, parse)
  }

  /** Builds a DesqDataset from arbitrary input data. The dataset is linked to the original data and parses it again
    * when used. For improved performance, save the dataset once created.
    *
    * @param rawData the input data as an RDD
    * @param parse method that takes an input element, parses it, and registers the resulting items (and their parents)
    *              with the provided DictionaryBuilder. Used to construct the dictionary and to translate the data.
    * @tparam T type of input data elements
    * @return the created DesqDataset
    */
  def build[T](rawData: RDD[T], parse: (T, DictionaryBuilder) => _): DesqDataset = {
    // construct the dictionary
    val dict = GenericDesqDataset.buildDictionary[T](rawData, parse)

    // now convert the sequences (lazily)
    val dictBroadcast = rawData.context.broadcast(dict)
    val sequences = rawData.mapPartitions(rows => new Iterator[WeightedSequence] {
      val dict = dictBroadcast.value
      val seqBuilder = new DefaultSequenceBuilder(dict)

      override def hasNext: Boolean = rows.hasNext

      override def next(): WeightedSequence = {
        parse.apply(rows.next(), seqBuilder)
        val s = new WeightedSequence(seqBuilder.getCurrentGids, seqBuilder.getCurrentWeight)
        dict.gidsToFids(s)
        s
      }
    })

    val sequenceInterpreter = new WeightedSequenceInterpreter(true)
    sequenceInterpreter.setDictionary(dict)

    // return the dataset
    val result = new DesqDataset(sequences, sequenceInterpreter)
    result
  }
}