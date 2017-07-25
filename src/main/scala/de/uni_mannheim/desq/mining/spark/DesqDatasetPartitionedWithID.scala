package de.uni_mannheim.desq.mining.spark

import java.net.URI
import java.util.Calendar
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import de.uni_mannheim.desq.avro.AvroDesqDatasetDescriptor
import de.uni_mannheim.desq.dictionary._
import de.uni_mannheim.desq.mining.{IdentifiableWeightedSequence, WeightedSequence}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.{ClassTag, _}

/**
  * Created by ivo on 25.07.17.
  */
class DesqDatasetPartitionedWithID[V <: IdentifiableWeightedSequence](val sequences: RDD[(Long, V)], val dict: Dictionary, val usesFids: Boolean) {

  def saveWithId(outputPath: String)(implicit ct:ClassTag[V]): DesqDatasetPartitionedWithID[V] = {
    val fileSystem = FileSystem.get(new URI(outputPath), sequences.context.hadoopConfiguration)

    // write sequences
    val sequencePath = s"$outputPath/sequences"
    sequences.mapPartitions(iter => iter.map(s=>(s._1, s._2))).saveAsSequenceFile(sequencePath)

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
    new DesqDatasetPartitionedWithID[V](
      sequences.context.sequenceFile(sequencePath, classOf[LongWritable], ct.runtimeClass).map(kv => (kv._1.get(), kv._2.asInstanceOf[V])),
      dict, usesFids)
  }

  /**
    * Transforms this DesqDataset which uses a RDD[K,V] in a regular DesqDataset[V]
    *
    * @return DesqDataset[V]
    */
  def toDesqDataset[V<:WeightedSequence](implicit ct:ClassTag[V]): DesqDataset[V] = {
    val seqs = sequences.mapPartitions[V](iter=> iter.map(f=>f._2.asInstanceOf[V]))
    new DesqDataset[V](seqs , dict, usesFids)
  }

}

object DesqDatasetPartitionedWithID {


  /**
    *
    * @param inputPath Path were the Dataset is stored
    * @param sc Implicit SparkContext
    * @param ct Implicit ClassTag of the SequenceType
    * @tparam V
    * @return
    */
  def loadWithId[V <: IdentifiableWeightedSequence](inputPath: String)(implicit sc: SparkContext, ct:ClassTag[V]): DesqDatasetPartitionedWithID[V] = {
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
    val sequences = sc.sequenceFile(sequencePath, classOf[LongWritable], ct.runtimeClass).map(kv => (kv._1.get(), kv._2.asInstanceOf[V]))

    // return the dataset
    new DesqDatasetPartitionedWithID[V](sequences, dict, descriptor.getUsesFids)
  }
}
