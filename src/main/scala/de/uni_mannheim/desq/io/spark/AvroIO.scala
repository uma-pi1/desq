package de.uni_mannheim.desq.io.spark

import org.apache.avro.Schema
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by ivo on 16.05.17.
  *
  * Additionally the Avro Schema should be registered with the DesqKryoRegistrator
  * http://subprotocol.com/system/apache-spark-ec2-avro.html
  */
object AvroIO {
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
}
