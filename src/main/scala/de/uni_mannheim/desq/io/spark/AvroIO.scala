package de.uni_mannheim.desq.io.spark

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.avro.Schema
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat, AvroKeyOutputFormat}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by ivo on 16.05.17.
  *
  * Utility class for reading and writing avro files
  *
  * Additionally the Avro Schema should be registered with the DesqKryoRegistrator
  * http://subprotocol.com/system/apache-spark-ec2-avro.html
  */
object AvroIO {
  /**
    * Read Avro files from disk into a RDD
    * @param path source path
    * @param schema avro schema
    * @param recursive boolean flag
    * @param sc SparkContext
    * @tparam T ClassTag of AvroFile
    * @return RDD containing AvroObjects
    */
  def read[T: ClassTag](path: String, schema: Schema, recursive:Boolean = true)(implicit sc:SparkContext): RDD[T] = {
    val conf = sc.hadoopConfiguration
    conf.set ("avro.schema.input.key", schema.toString)
    if(recursive) conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    val rddTuples = sc.newAPIHadoopFile(path, classOf[AvroKeyInputFormat[T]], classOf[AvroKey[T]], classOf[NullWritable], conf)
    val rddT = rddTuples.mapPartitions(
      f = (iter: Iterator[(AvroKey[T], NullWritable)]) => iter.map(_._1.datum())

      , preservesPartitioning = true
    )
    rddT
  }

  /**
    * Writes AvroObjects to file
    * @param path destination path
    * @param schema avro schema
    * @param data RDD containing AvroObjects
    * @param sc SparkContext
    * @tparam T ClassTag of the AvroObjects
    */
  def write[T: ClassTag](path: String, schema:Schema, data: RDD[T])(implicit sc:SparkContext) = {


    import org.apache.avro.file.DataFileWriter
    import org.apache.avro.specific.SpecificDatumWriter

    if (!Files.exists(Paths.get(path))) {
      Files.createDirectories(Paths.get(path))
    }

    val datumWriter = new SpecificDatumWriter[T]
    val dataFileWriter = new DataFileWriter(datumWriter)
    val records = data.collect()
    var number = 1
    for (record: T <- records) {
      dataFileWriter.create(schema, new File(s"$path/$number.avro"))
      dataFileWriter.append(record)
      dataFileWriter.close()
      number += 1
    }
  }
}
