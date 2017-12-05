package de.uni_mannheim.desq.examples.readme

import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.spark.{DesqCount, DesqDataset, DesqMiner, DesqMinerContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object DesqExample {

  def main(args: Array[String]) {
    implicit val sc = new SparkContext(new SparkConf().setAppName(getClass.getName).setMaster("local"))

    // load dictionary

    val dictionary = Dictionary.loadFrom(this.getClass.getResource("/readme/dictionary.json"))

    // parallelize input sequences

    val sequences = sc.parallelize(Source.fromURL(this.getClass.getResource("/readme/sequences.del")).getLines.toSeq)

    // create DesqDataset

    val data = DesqDataset.loadFromDelFile(sequences, dictionary, usesFids = false).copyWithRecomputedCountsAndFids()

    // create DesqProperties

    val properties = DesqCount.createConf("(..)", 2)
//    val properties = DesqCount.createConf("PERSON (VERB PREP?) CITY", 2)

    // create DesqMiner

    val miner = DesqMiner.create(new DesqMinerContext(properties))

    // run DesqMiner

    val patterns = miner.mine(data)

    // print output sequences

    patterns.print()
  }

}
