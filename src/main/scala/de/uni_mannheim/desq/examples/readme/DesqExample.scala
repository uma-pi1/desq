package de.uni_mannheim.desq.examples.readme

import de.uni_mannheim.desq.Desq
import de.uni_mannheim.desq.mining.spark._
import org.apache.spark.{SparkConf, SparkContext}

object DesqExample {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    Desq.initDesq(sparkConf)

    implicit val sc = new SparkContext(sparkConf)

    // read the data
    val sequences = sc.textFile("data/readme/sequences.txt")

    // convert data into DESQ's internal format (DesqDataset)
    val data = DesqDataset.buildFromStrings(sequences.map(s => s.split("\\s+")))

    // create a Miner
    val patternExpression = "(..)"
    val minimumSupport = 2
    val properties = DesqCount.createConf(patternExpression, minimumSupport)
    val miner = DesqMiner.create(new DesqMinerContext(properties))

    // do the mining; this creates another DesqDataset containing the result
    val patterns = miner.mine(data)

    // print the result
    patterns.print()
  }

}
