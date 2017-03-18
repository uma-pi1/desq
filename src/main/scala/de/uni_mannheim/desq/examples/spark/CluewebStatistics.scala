package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.Desq._
import de.uni_mannheim.desq.dictionary.DictionaryBuilder
import de.uni_mannheim.desq.mining.spark.DesqDataset
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by alexrenz on 19.03.2017.
  */
object CluewebStatistics extends App {
  val conf = new SparkConf().setAppName(getClass.getName)//.setMaster("local")
  initDesq(conf)
  implicit val sc = new SparkContext(conf)


  val datasetFolder = "hdfs://engine01.informatik.uni-mannheim.de:9000/data/clueweb-mpii/cw-sen-seq-50-DesqDataset/"

  // load data
  val data = DesqDataset.load(datasetFolder)
  data.print(5)
  println("--")

  val sequenceLengths = data.sequences.map(_.size()).cache()

  // Calculate average length
  val meanLength = sequenceLengths.mean()
  println("Average length: " + meanLength)

  // Maximum length
  val maxLength = sequenceLengths.max()
  println("Maximum length: " + maxLength)

  // total sequences
  val totalSeqs = sequenceLengths.count()
  println("Total sequences: " + totalSeqs)

  // Total items
  val totalItems = sequenceLengths.sum()
  println("Total items: " + totalItems);

  // Total Distinct items
  val distinctItems = data.dict.size()
  println("Distinct items: " + distinctItems)

  val stats = Array(meanLength, maxLength, totalSeqs, totalItems, distinctItems)

  sc.parallelize(stats).saveAsTextFile("hdfs://engine01.informatik.uni-mannheim.de:9000/user/alex/cw50stats.txt")


  println("DONE")
}
