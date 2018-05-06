package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.Desq._
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.spark.{DesqDataset, GenericDesqDataset}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by rgemulla on 12.09.2016.
  */
object DictionaryExample extends App {
  val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
  initDesq(conf)
  implicit val sc = new SparkContext(conf)

  val dictFile = getClass.getResource("/icdm16-example/dict.json")
  val dataFile = getClass.getResource("/icdm16-example/data.del")

  val dict: Dictionary = Dictionary.loadFrom(dictFile)
  println("Initial dictionary")
  dict.writeJson(System.out)
  println()

  println("\nData:")
  val delFile = sc.parallelize(Source.fromURL(dataFile).getLines.toSeq)
  val data = DesqDataset.loadFromDelFile(delFile, dict, usesFids = false)
  data.sequences.collect().foreach(println)
  println()
  data.print()

  println("\nDictionary with frequencies")
  val newData = data.recomputeDictionary()
  newData.dictionary.writeJson(System.out)
  println()
}
