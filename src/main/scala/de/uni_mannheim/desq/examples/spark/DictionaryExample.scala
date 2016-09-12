package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.spark.DesqDataset
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by rgemulla on 12.09.2016.
  */
object DictionaryExample extends App {
  val conf = new SparkConf().setAppName(getClass.getName)
  val sc = new SparkContext(conf)

  val dictFile = getClass.getResource("/icdm16-example/dict.json")
  val dataFile = getClass.getResource("/icdm16-example/data.del")

  val dict: Dictionary = Dictionary.loadFrom(dictFile)
  println("Initial dictionary")
  dict.writeJson(System.out)
  println

  println("\nData:")
  val delFile = sc.parallelize(Source.fromURL(dataFile).getLines.toSeq)
  val data = DesqDataset.fromDelFile(delFile, dict, false)
  data.sequences.collect.foreach(println)
}
