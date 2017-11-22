package de.uni_mannheim.desq.mining.spark

import java.net.URI
import java.util.Calendar
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import de.uni_mannheim.desq.avro.AvroDesqDatasetDescriptor
import de.uni_mannheim.desq.dictionary._
import de.uni_mannheim.desq.io.DelSequenceReader
import de.uni_mannheim.desq.mining.WeightedSequence
import de.uni_mannheim.desq.util.DesqProperties
import it.unimi.dsi.fastutil.ints._
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by rgemulla on 12.09.2016.
  * Added itemsetSeparatorGid by sulbrich on 27.09.2017: 0 -> No Itemset, Frequency of Gid = 0 -> Just Itemsets, else -> Sequence of Itemsets
  */
class DesqDataset(val sequences: RDD[WeightedSequence], val dict: Dictionary, val usesFids: Boolean = false, val itemsetSeparatorGid: Int = 0) {
  private var dictBroadcast: Broadcast[Dictionary] = _

  // -- building ------------------------------------------------------------------------------------------------------

  def this(sequences: RDD[WeightedSequence], source: DesqDataset, usesFids: Boolean) {
    this(sequences, source.dict, usesFids, source.itemsetSeparatorGid)
    dictBroadcast = source.dictBroadcast
  }

  /** Creates a copy of this DesqDataset with a deep copy of its dictionary. Useful when changes should be
    * performed to a dictionary that has been broadcasted before (and hence cannot/should not be changed). */
  def copy(): DesqDataset = {
    new DesqDataset(sequences, dict.deepCopy(), usesFids, itemsetSeparatorGid)
  }

  /** Returns a copy of this dataset with a new dictionary, containing updated counts and fid identifiers. The
    * original input sequences are "translated" to the new dictionary if needed. */
  def copyWithRecomputedCountsAndFids(): DesqDataset = {
    // compute counts
    val usesFids = this.usesFids
    val dictBroadcast = broadcastDictionary()
    val totalItemFreqs = sequences.mapPartitions(rows => {
      new Iterator[(Int, (Long,Long))] {
        val dict = dictBroadcast.value
        val itemCfreqs = new Int2LongOpenHashMap()
        var currentItemCfreqsIterator = itemCfreqs.int2LongEntrySet().fastIterator()
        var currentWeight = 0L
        val ancItems = new IntAVLTreeSet()

        override def hasNext: Boolean = {
          while (!currentItemCfreqsIterator.hasNext && rows.hasNext) {
            val sequence = rows.next()
            currentWeight = sequence.weight
            dict.computeItemCfreqs(sequence, itemCfreqs, ancItems, usesFids, 1)
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
    val newDict = dict.deepCopy()
    newDict.clearFreqs() // reset all frequencies to 0 (important for items that do not occur in totalItemFreqs)
    for (itemFreqs <- totalItemFreqs) {
      val fid = if (usesFids) itemFreqs._1 else newDict.fidOf(itemFreqs._1)
      newDict.setDfreqOf(fid, itemFreqs._2._1)
      newDict.setCfreqOf(fid, itemFreqs._2._2)
    }
    newDict.recomputeFids()

    // if we are not using fids, we are done
    if (!usesFids) {
      return new DesqDataset(sequences, newDict, false)
    }

    // otherwise we need to relabel the fids
    val newDictBroadcast = sequences.context.broadcast(newDict)
    val newSequences = sequences.mapPartitions(rows => {
      new Iterator[WeightedSequence] {
        val dict = dictBroadcast.value
        val newDict = newDictBroadcast.value

        override def hasNext: Boolean = rows.hasNext

        override def next(): WeightedSequence = {
          val oldSeq = rows.next()
          val newSeq = oldSeq.clone()
          dict.fidsToGids(newSeq)
          newDict.gidsToFids(newSeq)
          newSeq
        }
      }
    })
    val newData = new DesqDataset(newSequences, newDict, true, itemsetSeparatorGid)
    newData.dictBroadcast = newDictBroadcast
    newData
  }

  // -- conversion ----------------------------------------------------------------------------------------------------

  /** Returns dataset with sequences encoded as Fids.
    *  If sequences are encoded as gids, they are converted to fids. Otherwise, nothing is done.
    */
  //noinspection AccessorLikeMethodIsEmptyParen
  def toFids(): DesqDataset = {
    val usesFids = this.usesFids
    val dictBroadcast = broadcastDictionary()

    if (usesFids) {
      this
    } else {
      val newSequences = sequences.mapPartitions(rows => {
        new Iterator[WeightedSequence] {
          val dict = dictBroadcast.value

          override def hasNext: Boolean = rows.hasNext

          override def next(): WeightedSequence = {
            val oldSeq = rows.next()
            val newSeq = oldSeq.clone()
            dict.gidsToFids(newSeq)
            newSeq
          }
        }
      })

      new DesqDataset(newSequences, this, true)
    }
  }

  /** Returns dataset with sequences encoded as Gids.
    *  If sequences are encoded as fids, they are converted to gids. Otherwise, nothing is done.
    */
  //noinspection AccessorLikeMethodIsEmptyParen
  def toGids(): DesqDataset = {
    val usesFids = this.usesFids
    val dictBroadcast = broadcastDictionary()

    if (!usesFids) {
      this
    } else {
      val newSequences = sequences.mapPartitions(rows => {
        new Iterator[WeightedSequence] {
          val dict = dictBroadcast.value

          override def hasNext: Boolean = rows.hasNext

          override def next(): WeightedSequence = {
            val oldSeq = rows.next()
            val newSeq = oldSeq.clone()
            dict.fidsToGids(newSeq)
            newSeq
          }
        }
      })

      new DesqDataset(newSequences, this, false)
    }
  }

  /** Returns an RDD that contains for each sequence an array of its string identifiers and its weight. */
  //noinspection AccessorLikeMethodIsEmptyParen
  def toSidsWeightPairs(): RDD[(Array[String],Long)] = {
    val dictBroadcast = broadcastDictionary()
    val usesFids = this.usesFids // to localize

    sequences.mapPartitions(rows => {
      new Iterator[(Array[String],Long)] {
        val dict = dictBroadcast.value

        override def hasNext: Boolean = rows.hasNext

        override def next(): (Array[String], Long) = {
          val s = rows.next()
          val itemSids = new Array[String](s.size())
          for (i <- Range(0,s.size())) {
            if (usesFids) {
              itemSids(i) = dict.sidOfFid(s.getInt(i))
            } else {
              itemSids(i) = dict.sidOfGid(s.getInt(i))
            }
          }
          (itemSids, s.weight)
        }
      }
    })
  }

  // -- I/O -----------------------------------------------------------------------------------------------------------

  def save(outputPath: String): DesqDataset = {
    val fileSystem = FileSystem.get(new URI(outputPath), sequences.context.hadoopConfiguration)

    // write sequences
    val sequencePath = s"$outputPath/sequences"
    sequences.map(s => (NullWritable.get(),s)).saveAsSequenceFile(sequencePath)

    // write dictionary
    val dictPath = s"$outputPath/dict.avro.gz"
    val dictOut = FileSystem.create(fileSystem, new Path(dictPath), FsPermission.getFileDefault)
    dict.writeAvro(new GZIPOutputStream(dictOut))
    dictOut.close()

    // write descriptor
    val descriptor = new AvroDesqDatasetDescriptor()
    descriptor.setCreationTime(Calendar.getInstance().getTime.toString)
    descriptor.setUsesFids(usesFids)
    val descriptorPath = s"$outputPath/descriptor.json"
    val descriptorOut = FileSystem.create(fileSystem, new Path(descriptorPath), FsPermission.getFileDefault)
    val writer = new SpecificDatumWriter[AvroDesqDatasetDescriptor](classOf[AvroDesqDatasetDescriptor])
    val encoder = EncoderFactory.get.jsonEncoder(descriptor.getSchema, descriptorOut)
    writer.write(descriptor, encoder)
    encoder.flush()
    descriptorOut.close()

    // return a new dataset for the just saved data
    new DesqDataset(
      sequences.context.sequenceFile(sequencePath, classOf[NullWritable], classOf[WeightedSequence]).map(kv => kv._2),
      dict, usesFids)
  }


  // -- mining --------------------------------------------------------------------------------------------------------

  def mine(minerConf: DesqProperties): DesqDataset = {
    val ctx = new DesqMinerContext(minerConf)
    mine(ctx)
  }

  def mine(ctx: DesqMinerContext): DesqDataset = {
    val miner = DesqMiner.create(ctx)
    mine(miner)
  }

  def mine(miner: DesqMiner): DesqDataset = {
    miner.mine(this)
  }


  // -- helpers -------------------------------------------------------------------------------------------------------

  /** Returns a broadcast variable that can be used to access the dictionary of this dataset. The broadcast
    * variable stores the dictionary in serialized form for memory efficiency. Use
    * <code>Dictionary.fromBytes(result.value)</code> to get the dictionary at workers.
    */
  def broadcastDictionary(): Broadcast[Dictionary] = {
    if (dictBroadcast == null) {
      val dict = this.dict
      dictBroadcast = sequences.context.broadcast(dict)
    }
    dictBroadcast
  }


  /** Pretty prints up to <code>maxSequences</code> sequences contained in this dataset using their sid's. */
  def print(maxSequences: Int = -1): Unit = {
    val strings = toSidsWeightPairs().map(s => {
      val sidString = s._1.deep.mkString("[", " ", "]")
      if (s._2 == 1)
        sidString
      else
        sidString + "@" + s._2
    })
    if (maxSequences < 0)
      strings.collect().foreach(println)
    else
      strings.take(maxSequences).foreach(println)
  }

  /** Returning just Sids without weight -> easier handling for generic itemset creation
    * copy of toSidsWeightPairs()
    * created by sulbrich 22.09.2017) */
  def toSids: RDD[Array[String]] = {
    val dictBroadcast = broadcastDictionary()
    val usesFids = this.usesFids // to localize

    sequences.mapPartitions(rows => {
      new Iterator[Array[String]] {
        val dict:Dictionary = dictBroadcast.value

        override def hasNext: Boolean = rows.hasNext

        override def next(): Array[String] = {
          val s = rows.next()
          val itemSids = new Array[String](s.size())
          for (i <- Range(0,s.size())) {
            if (usesFids) {
              itemSids(i) = dict.sidOfFid(s.getInt(i))
            } else {
              itemSids(i) = dict.sidOfGid(s.getInt(i))
            }
          }
          itemSids
        }
      }
    })

  }

  def getCfreqOfSeparator: Long = {
    dict.cfreqOf(dict.fidOf(itemsetSeparatorGid))
  }

  def containsItemsets(): Boolean = {
    itemsetSeparatorGid > 0 && getCfreqOfSeparator == 0
  }

  def containsSequencesOfItemsets(): Boolean = {
    itemsetSeparatorGid > 0 && getCfreqOfSeparator > 0
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

    // return the dataset
    new DesqDataset(sequences, dict, descriptor.getUsesFids)
  }

  /** Loads data from the specified del file */
  def loadFromDelFile(delFile: RDD[String], dict: Dictionary, usesFids: Boolean): DesqDataset = {
    val sequences = delFile.map(line => {
      val s = new WeightedSequence(Array.empty[Int], 1L)
      DelSequenceReader.parseLine(line, s)
      s
    })

    new DesqDataset(sequences, dict, usesFids)
  }

  /** Loads data from the specified del file */
  def loadFromDelFile(delFile: String, dict: Dictionary, usesFids: Boolean = false)(implicit sc: SparkContext): DesqDataset = {
    loadFromDelFile(sc.textFile(delFile), dict, usesFids)
  }

  // -- building ------------------------------------------------------------------------------------------------------

  /** Builds a DesqDataset from an RDD of string arrays. Every array corresponds to one sequence, every element to
    * one item. The generated hierarchy is flat, if no base dictionary is provided (containing hierarchy information)
    * If a ItemSet Separator is provided, the DesqDataset will contain ItemSets*/
  def buildFromStrings(rawData: RDD[Array[String]]): DesqDataset= {
    buildFromStrings(rawData, None, None)
  }
  def buildFromStrings(rawData: RDD[Array[String]], itemSetSeparator: Option[String], extDict: Option[Dictionary]): DesqDataset = {
    val parse = (strings: Array[String], seqBuilder: DictionaryBuilder) => {
      seqBuilder.newSequence(1)
      for (string <- strings) {
        seqBuilder.appendItem(string)
      }
    }
    build[Array[String]](rawData, parse, itemSetSeparator, extDict)
  }

  /**
    * Build an item set DesqDataset based on an existing DesqDataset
    * Optionally provide an external basis dict, default is to reuse the dict of the provided DesqDataset
    */
  def buildItemsets(sourceDataset: DesqDataset, itemSetSeparator: Option[String], extDict: Option[Dictionary]): DesqDataset = {
    buildFromStrings(sourceDataset.toSids,
      if (itemSetSeparator.isDefined) itemSetSeparator else Option.apply("|"),
      if (extDict.isDefined) extDict else Option.apply(sourceDataset.dict.deepCopy())
    )
  }

  def buildItemsets(sourceDataset: DesqDataset): DesqDataset = {
    buildItemsets(sourceDataset,None, None)
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
  /*def build[T](rawData: RDD[T], parse: (T, DictionaryBuilder) => _): DesqDataset = {
     val dict = rawData.mapPartitions(rows => {
      val dictBuilder = new DefaultDictionaryBuilder()
      while (rows.hasNext) {
        parse.apply(rows.next(), dictBuilder)
      }
      dictBuilder.newSequence(0) // flush last sequence
      Iterator.single(dictBuilder.getDictionary)
    }).treeReduce((d1, d2) => { d1.mergeWith(d2); d1 }, 3)
    dict.recomputeFids()

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

    // return the dataset
    val result = new DesqDataset(sequences, dict, true)
    result.dictBroadcast = dictBroadcast
    result
  }*/

  def build[T](rawData: RDD[T], parse: (T, DictionaryBuilder) => _): DesqDataset ={
    build(rawData, parse, None, None)
  }

  def build[T](rawData: RDD[T], parse: (T, DictionaryBuilder) => _, itemsetSeparator: Option[String], extDict: Option[Dictionary]): DesqDataset = {
    val isItemset = itemsetSeparator.isDefined

    // --- Dictionary
    // - Handle basic dictionary: optional separator / external dict for hierarchies
    val baseDictBuilder =
      if(extDict.isDefined) new DefaultDictionaryBuilder(extDict.get.deepCopy())
      else new DefaultDictionaryBuilder()
    //add separator if defined
    if(isItemset) baseDictBuilder.appendItem(itemsetSeparator.get) //only added if not existing

    var dictBroadcast = rawData.context.broadcast(baseDictBuilder.getDictionary)

    // - Create actual dictionary
    val dict = rawData.mapPartitions(rows => {
      val dictBuilder = new DefaultDictionaryBuilder(dictBroadcast.value)
      while (rows.hasNext) {
        parse.apply(rows.next(), dictBuilder)
      }
      dictBuilder.newSequence(0) // flush last sequence
      Iterator.single(dictBuilder.getDictionary)
    }).treeReduce((d1, d2) => { d1.mergeWith(d2); d1 }, 3)
    dict.recomputeFids()

    // --- now convert the sequences (lazily)
   dictBroadcast = rawData.context.broadcast(dict)
    val sequences = rawData.mapPartitions(rows => new Iterator[WeightedSequence] {
      val dict = dictBroadcast.value
      val seqBuilder =
        if(isItemset) new DefaultSequenceBuilder(dict,itemsetSeparator.get)
        else new DefaultSequenceBuilder(dict)

      override def hasNext: Boolean = rows.hasNext

      override def next(): WeightedSequence = {
        parse.apply(rows.next(), seqBuilder)
        val s = new WeightedSequence(seqBuilder.getCurrentGids, seqBuilder.getCurrentWeight)
        dict.gidsToFids(s)
        s
      }
    })

    // return the dataset
    val result =
      if(isItemset){
        new DesqDataset(sequences, dict, true, dict.gidOf(itemsetSeparator.get))
      }else{
        new DesqDataset(sequences, dict, true)
      }
    result.dictBroadcast = dictBroadcast
    result

  }
}