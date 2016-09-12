package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.io.{DelPatternReader, DelSequenceReader}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by rgemulla on 12.09.2016.
  */
class DesqDataset(_sequences: RDD[Sequence], _dict: Dictionary, _usesFids: Boolean = false) {
  val sequences: RDD[Sequence] = _sequences
  val dict = _dict
  val usesFids = _usesFids
}

object DesqDataset {
  def fromDelFile(delFile: RDD[String], dict: Dictionary, usesFids: Boolean): DesqDataset = {
    val sequences = delFile.map(s => {
      val sequence = new Sequence
      DelSequenceReader.parseLine(s, sequence.items)
      sequence
    })
    new DesqDataset(sequences, dict, usesFids)
  }

  def fromDelFile(implicit sc: SparkContext, delFile: String, dict: Dictionary, usesFids: Boolean = false): DesqDataset = {
    fromDelFile(sc.textFile(delFile), dict, usesFids)
  }
}