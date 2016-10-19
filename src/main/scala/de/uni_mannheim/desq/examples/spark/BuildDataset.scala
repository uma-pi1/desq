package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.Desq
import de.uni_mannheim.desq.mining.spark.{DesqCount, DesqDataset, DesqMiner, DesqMinerContext}
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

import old.de.uni_mannheim.desq.dictionary.DictionaryIO
import java.io._

/**
  * Build datasets
  * Created by alexrenz on 08.10.2016.
  */
object BuildDataset {
  
  def main(args: Array[String]) {
    if(args.length < 1) {
      println("Usage: BuildDataset dataset")
      System.exit(0)
    }
    
    // Init Desq, build SparkContext
    val sparkConf = new SparkConf().setAppName(getClass.getName)
    Desq.initDesq(sparkConf)
    implicit val sc = new SparkContext(sparkConf)
      
    val dataset = args(0)
    val base = "/home/alex/"
    val baseOrig = base + "Data/" + dataset + "/" + dataset

    val dict = DictionaryIO.loadFromDel(new FileInputStream(baseOrig + "-dict.del"), true);

    dict.write("/" + base + "/Data/prep2/dict/" + dataset + ".avro.gz")
    val data = DesqDataset.loadFromDelFile("file://" + baseOrig + "-data.del", dict, true)

    data.save("file://" + base + "Data/prep2/" + dataset)

/*

run like this:

source ~/sl.sh && ~/spark/bin/spark-submit \
--master "local" \
--class de.uni_mannheim.desq.examples.spark.BuildDataset \
target/desq-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
nyt-1991


 */

  }
}

