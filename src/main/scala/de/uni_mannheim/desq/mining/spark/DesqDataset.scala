package de.uni_mannheim.desq.mining.spark

import java.net.URI
import java.util.zip.GZIPInputStream

import de.uni_mannheim.desq.avro.AvroDesqDatasetDescriptor
import de.uni_mannheim.desq.dictionary.{DefaultSequenceBuilder, Dictionary, DictionaryBuilder}
import de.uni_mannheim.desq.io.DelSequenceReader
import de.uni_mannheim.desq.mining.WeightedSequence
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
class DesqDataset(override val sequences: RDD[WeightedSequence],
                  override val descriptor: WeightedSequenceDescriptor)
                  extends GenericDesqDataset[WeightedSequence](sequences, descriptor) {
  private var descriptorBroadcast: Broadcast[WeightedSequenceDescriptor] = _

  // -- building ------------------------------------------------------------------------------------------------------

  def this(sequences: RDD[WeightedSequence], source: DesqDataset) {
    this(sequences, source.descriptor)
    descriptorBroadcast = source.descriptorBroadcast
  }

  /** Returns a copy of this dataset with a new dictionary, containing updated counts and fid identifiers. The
    * original input sequences are "translated" to the new dictionary if needed. */
  override def recomputeDictionary(): DesqDataset = {
    val newDescriptor = descriptor.copy().asInstanceOf[WeightedSequenceDescriptor]
    recomputeDictionary(newDescriptor.getDictionary)

    if (!descriptor.usesFids) {
      // if we are not using fids, we are done
      new DesqDataset(sequences, newDescriptor)
    } else {
      // otherwise we need to relabel the fids
      val newDictBroadcast = sequences.context.broadcast(newDescriptor.getDictionary)
      val newSequences = sequences.mapPartitions(rows => {
        new Iterator[WeightedSequence] {
          val newDict = newDictBroadcast.value
          val descriptor = descriptorBroadcast.value

          override def hasNext: Boolean = rows.hasNext

          override def next(): WeightedSequence = {
            val oldSeq = rows.next()
            val newSeq = descriptor.getCopy(oldSeq)
            descriptor.getDictionary.fidsToGids(descriptor.getFids(newSeq))
            newDict.gidsToFids(descriptor.getFids(newSeq))
            newSeq
          }
        }
      })

      new DesqDataset(sequences, newDescriptor)
    }
  }

  // -- conversion ----------------------------------------------------------------------------------------------------

  /** Returns dataset with sequences encoded as Fids.
    *  If sequences are encoded as gids, they are converted to fids. Otherwise, nothing is done.
    */
  override def toDesqDatasetWithFids(): DesqDataset = {
    if (descriptor.usesFids) {
      this
    } else {
      super.toDesqDatasetWithFids()
    }
  }

  /** Returns dataset with sequences encoded as Gids.
    *  If sequences are encoded as fids, they are converted to gids. Otherwise, nothing is done.
    */
  override def toDesqDatasetWithGids(): DesqDataset = {
    if (!descriptor.usesFids) {
      this
    } else {
      super.toDesqDatasetWithGids()
    }
  }
}

object DesqDataset {
  // -- I/O -----------------------------------------------------------------------------------------------------------

  def load(inputPath: String)(implicit sc: SparkContext): DesqDataset = {
    val fileSystem = FileSystem.get(new URI(inputPath), sc.hadoopConfiguration)

    // read descriptor
    var avroDescriptor = new AvroDesqDatasetDescriptor()
    val avroDescriptorPath = s"$inputPath/descriptor.json"
    val avroDescriptorIn = fileSystem.open(new Path(avroDescriptorPath))
    val reader = new SpecificDatumReader[AvroDesqDatasetDescriptor](classOf[AvroDesqDatasetDescriptor])
    val decoder = DecoderFactory.get.jsonDecoder(avroDescriptor.getSchema, avroDescriptorIn)
    avroDescriptor = reader.read(avroDescriptor, decoder)
    avroDescriptorIn.close()

    // read dictionary
    val dictPath = s"$inputPath/dict.avro.gz"
    val dictIn = fileSystem.open(new Path(dictPath))
    val dict = new Dictionary()
    dict.readAvro(new GZIPInputStream(dictIn))
    dictIn.close()

    // read sequences
    val sequencePath = s"$inputPath/sequences"
    val sequences = sc.sequenceFile(sequencePath, classOf[NullWritable], classOf[WeightedSequence]).map(kv => kv._2)

    val descriptor = new WeightedSequenceDescriptor(avroDescriptor.getUsesFids)
    descriptor.setDictionary(dict)

    // return the dataset
    new DesqDataset(sequences, descriptor)
  }

  /** Loads data from the specified del file */
  def loadFromDelFile(delFile: RDD[String], dict: Dictionary, usesFids: Boolean): DesqDataset = {
    val sequences = delFile.map(line => {
      val s = new WeightedSequence(Array.empty[Int], 1L)
      DelSequenceReader.parseLine(line, s)
      s
    })

    val descriptor = new WeightedSequenceDescriptor(usesFids)
    descriptor.setDictionary(dict)

    new DesqDataset(sequences, descriptor)
  }

  /** Loads data from the specified del file */
  def loadFromDelFile(delFile: String, dict: Dictionary, usesFids: Boolean = false)(implicit sc: SparkContext): DesqDataset = {
    loadFromDelFile(sc.textFile(delFile), dict, usesFids)
  }

}