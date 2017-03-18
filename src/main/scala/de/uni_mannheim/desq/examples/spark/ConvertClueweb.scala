package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.Desq._
import de.uni_mannheim.desq.dictionary.{Dictionary, DictionaryBuilder}
import de.uni_mannheim.desq.mining.spark.DesqDataset
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created by alexrenz on 19.03.2017.
  */
object ConvertClueweb extends App {
  val conf = new SparkConf().setAppName(getClass.getName)//.setMaster("local")
  initDesq(conf)
  implicit val sc = new SparkContext(conf)


  val sourceFolder = "hdfs://engine01.informatik.uni-mannheim.de:9000/data/clueweb-mpii/cw-sen-seq-50/raw/"
  val destinationFolder = "hdfs://engine01.informatik.uni-mannheim.de:9000/data/clueweb-mpii/cw-sen-seq-50-DesqDataset/"

  // read the sequence file
  val sf = sc.sequenceFile[org.apache.hadoop.io.LongWritable,de.mpii.fsm.util.IntArrayWritable](sourceFolder, 64)

  // extract the backing int[]'s
  val rawData = sf.map(_._2.getContents()) // gives RDD[Array[Int]]

  // build a DesqDataset
  val parse = (ints: Array[Int], seqBuilder: DictionaryBuilder) => {
    seqBuilder.newSequence(1)
    for (int <- ints) {
      seqBuilder.appendItem(int.toString) // not the smartest thing ever, but works
    }
  }
  val data = DesqDataset.build[Array[Int]](rawData,parse)

  // save it
  val savedData = data.save(destinationFolder)
  savedData.print(5)
  println("--")

  // load it
  val loadedData = DesqDataset.load(destinationFolder)
  loadedData.print(5)
  println("--")

  println("DONE")
}
