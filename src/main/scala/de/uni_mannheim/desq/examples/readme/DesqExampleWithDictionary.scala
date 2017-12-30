package de.uni_mannheim.desq.examples.readme

import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.spark.{DesqCount, DesqDataset, DesqMiner, DesqMinerContext}
import org.apache.spark.{SparkConf, SparkContext}

object DesqExampleWithDictionary {

  def main(args: Array[String]) {
    implicit val sc = new SparkContext(new SparkConf().setAppName(getClass.getName).setMaster("local"))

    val dictionary = Dictionary.loadFrom("data/readme/dictionary.json")

    val data = DesqDataset.loadFromDelFile("data/readme/sequences.del", dictionary).copyWithRecomputedCountsAndFids()

    val properties = DesqCount.createConf("PERSON (VERB PREP?) CITY", 2)
    val miner = DesqMiner.create(new DesqMinerContext(properties))

    val patterns = miner.mine(data)
    patterns.print()
  }

}
