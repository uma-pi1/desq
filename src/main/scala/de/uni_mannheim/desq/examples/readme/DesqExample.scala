package de.uni_mannheim.desq.examples.readme

import de.uni_mannheim.desq.mining.spark._
import org.apache.spark.{SparkConf, SparkContext}

object DesqExample {

  def main(args: Array[String]) {
    implicit val sc = new SparkContext(new SparkConf().setAppName(getClass.getName).setMaster("local"))

    val sequences = sc.textFile("data/readme/sequences.txt")
    val data = DesqDataset.buildFromStrings(sequences.map(s => s.split("\\s+")))

    val properties = DesqCount.createConf("(..)", 2)
    val miner = DesqMiner.create(new DesqMinerContext(properties))

    val patterns = miner.mine(data)
    patterns.print()
  }

}
