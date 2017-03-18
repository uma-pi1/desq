package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.Desq._
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by alexrenz on 19.03.2017.
  */
object CountCluewebItems extends App {
  val conf = new SparkConf().setAppName(getClass.getName)//.setMaster("local")
  initDesq(conf)
  implicit val sc = new SparkContext(conf)

  val ds = 25
  val sourceFolder = "hdfs://engine01.informatik.uni-mannheim.de:9000/data/clueweb-mpii/cw-sen-seq-"+ds+"/raw/"

  // read the sequence file
  val sf = sc.sequenceFile[org.apache.hadoop.io.LongWritable,de.mpii.fsm.util.IntArrayWritable](sourceFolder, 64)

  // count distinct items
  val wordCount = sf.flatMap(_._2.getContents()).map((_,1)).reduceByKey(_ + _)

  // number of distinct items
  val count = wordCount.count()

  // write count
  sc.parallelize(Array(count)).saveAsTextFile("hdfs://engine01.informatik.uni-mannheim.de:9000/user/alex/cw"+ds+"count.txt")

  println("DONE")
}
