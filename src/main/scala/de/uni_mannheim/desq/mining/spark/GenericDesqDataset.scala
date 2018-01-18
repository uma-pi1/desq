package de.uni_mannheim.desq.mining.spark

import java.net.URI
import java.util.Calendar
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import de.uni_mannheim.desq.avro.AvroDesqDatasetDescriptor
import de.uni_mannheim.desq.dictionary.{DefaultDictionaryBuilder, DefaultSequenceBuilder, Dictionary, DictionaryBuilder}
import de.uni_mannheim.desq.io.DelSequenceReader
import de.uni_mannheim.desq.mining.{Sequence, WeightedSequence}
import de.uni_mannheim.desq.util.DesqProperties
import it.unimi.dsi.fastutil.ints._
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class GenericDesqDataset[T](val sequences: RDD[T], val sequenceInterpreter: DesqSequenceInterpreter[T]) {
  private var sequenceInterpreterBroadcast: Broadcast[DesqSequenceInterpreter[T]] = _

  // -- building ------------------------------------------------------------------------------------------------------

  def this(sequences: RDD[T], source: GenericDesqDataset[T]) {
    this(sequences, source.sequenceInterpreter)
    sequenceInterpreterBroadcast = source.sequenceInterpreterBroadcast
  }

  /** Creates a copy of this DesqDataset with a deep copy of its dictionary. Useful when changes should be
    * performed to a dictionary that has been broadcasted before (and hence cannot/should not be changed). */
  def copy(): GenericDesqDataset[T] = {
    new GenericDesqDataset(sequences, sequenceInterpreter.copy())
  }

  // -- conversion ----------------------------------------------------------------------------------------------------

  /** Returns an RDD that contains for each sequence an array of its string identifiers and its weight. */
  //noinspection AccessorLikeMethodIsEmptyParen
  def toSidsWeightPairs(): RDD[(Array[String],Long)] = {
    val sequenceInterpreterBroadcast = broadcastSequenceInterpreter()

    sequences.mapPartitions(rows => {
      new Iterator[(Array[String],Long)] {
        val sequenceInterpreter = sequenceInterpreterBroadcast.value

        override def hasNext: Boolean = rows.hasNext

        override def next(): (Array[String], Long) = {
          val s = rows.next()
          (sequenceInterpreter.getSids(s), sequenceInterpreter.getWeight(s))
        }
      }
    })
  }

  // -- I/O -----------------------------------------------------------------------------------------------------------

  def save(outputPath: String)(implicit m: ClassTag[T]): GenericDesqDataset[T] = {
    val fileSystem = FileSystem.get(new URI(outputPath), sequences.context.hadoopConfiguration)

    // write sequences
    val sequencePath = s"$outputPath/sequences"
    sequences.map(s => (NullWritable.get(),sequenceInterpreter.getWritable(s))).saveAsSequenceFile(sequencePath)

    // write dictionary
    val dictPath = s"$outputPath/dict.avro.gz"
    val dictOut = FileSystem.create(fileSystem, new Path(dictPath), FsPermission.getFileDefault)
    sequenceInterpreter.getDictionary.writeAvro(new GZIPOutputStream(dictOut))
    dictOut.close()

    // write descriptor
    val descriptor = new AvroDesqDatasetDescriptor()
    descriptor.setCreationTime(Calendar.getInstance().getTime.toString)
    val descriptorPath = s"$outputPath/descriptor.json"
    val descriptorOut = FileSystem.create(fileSystem, new Path(descriptorPath), FsPermission.getFileDefault)
    val writer = new SpecificDatumWriter[AvroDesqDatasetDescriptor](classOf[AvroDesqDatasetDescriptor])
    val encoder = EncoderFactory.get.jsonEncoder(descriptor.getSchema, descriptorOut)
    writer.write(descriptor, encoder)
    encoder.flush()
    descriptorOut.close()

    // return a new dataset for the just saved data
    new GenericDesqDataset[T](
      sequences.context.sequenceFile(sequencePath, classOf[NullWritable], classOf[Writable]).map(kv => kv._2).asInstanceOf[RDD[T]],
      sequenceInterpreter)
  }


  // -- mining --------------------------------------------------------------------------------------------------------

  def mine(minerConf: DesqProperties)(implicit m: ClassTag[T]): GenericDesqDataset[T] = {
    val ctx = new DesqMinerContext(minerConf)
    mine(ctx)
  }

  def mine(ctx: DesqMinerContext)(implicit m: ClassTag[T]): GenericDesqDataset[T] = {
    val miner = DesqMiner.create(ctx)
    mine(miner)
  }

  def mine(miner: DesqMiner)(implicit m: ClassTag[T]): GenericDesqDataset[T] = {
    miner.mine(this)
  }


  // -- helpers -------------------------------------------------------------------------------------------------------

  /** Returns a broadcast variable that can be used to access the sequence interpreter of this dataset. The broadcast
    * variable stores the dictionary contained in the sequence interpreter in serialized form for memory efficiency.
    * Use <code>Dictionary.fromBytes(result.value.getDictionary)</code> to get the dictionary at workers.
    */
  def broadcastSequenceInterpreter(): Broadcast[DesqSequenceInterpreter[T]] = {
    if (sequenceInterpreterBroadcast == null) {
      val sequenceInterpreter = this.sequenceInterpreter
      sequenceInterpreterBroadcast = sequences.context.broadcast(sequenceInterpreter)
    }
    sequenceInterpreterBroadcast
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
}

object GenericDesqDataset {
  // -- building ------------------------------------------------------------------------------------------------------

  def buildDictionaryFromStrings(rawData: RDD[Array[String]]) : Dictionary = {
    val parseForDictionary = (strings: Array[String], seqBuilder: DictionaryBuilder) => {
      seqBuilder.newSequence(1)
      for (string <- strings) {
        seqBuilder.appendItem(string)
      }
    }

    buildDictionary[Array[String]](rawData, parseForDictionary)
  }

  def buildDictionary[T](rawData: RDD[T], parse: (T, DictionaryBuilder) => _) : Dictionary = {
    val dict = rawData.mapPartitions(rows => {
      val dictBuilder = new DefaultDictionaryBuilder()
      while (rows.hasNext) {
        parse.apply(rows.next(), dictBuilder)
      }
      dictBuilder.newSequence(0) // flush last sequence
      Iterator.single(dictBuilder.getDictionary)
    }).treeReduce((d1, d2) => { d1.mergeWith(d2); d1 }, 3)
    dict.recomputeFids()

    dict
  }
}
