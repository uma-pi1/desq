package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.avro.Sentence
import de.uni_mannheim.desq.converters.nyt.NytUtils
import de.uni_mannheim.desq.dictionary._
import de.uni_mannheim.desq.io.DelSequenceReader
import de.uni_mannheim.desq.mining.{IdentifiableWeightedSequence, WeightedSequence}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by rgemulla on 12.09.2016.
  */
class IdentifiableDesqDataset(override val sequences: RDD[IdentifiableWeightedSequence], override val dict: Dictionary, override val usesFids: Boolean = false) extends DesqDataset[IdentifiableWeightedSequence](sequences, dict, usesFids) {
  private var dictBroadcast: Broadcast[Dictionary] = _

  // -- building ------------------------------------------------------------------------------------------------------

  def this(sequences: RDD[IdentifiableWeightedSequence], source: IdentifiableDesqDataset, usesFids: Boolean) {
    this(sequences, source.dict, usesFids)
    dictBroadcast = source.dictBroadcast
  }
}

object IdentifiableDesqDataset extends DesqDatasetCore[IdentifiableWeightedSequence] {

  /**
    * Builds a DesqDataset from an RDD of rows. Every row corresponds to one article, which may contain many sentences.
    * Every sentence contains tokens. The generated hierarchy is deep.
    */

  def buildFromSentencesWithID(rawData: RDD[(Long, Sentence)]): IdentifiableDesqDataset = {
    val parse = (id: Long, sentence: Sentence, dictionaryBuilder: DictionaryBuilder) => NytUtils.processSentence(id, sentence, dictionaryBuilder)
    val result = buildWithId[Sentence, IdentifiableWeightedSequence](rawData, parse)
    result.asInstanceOf[IdentifiableDesqDataset]
  }

  def buildFromSentences(rawData: RDD[Sentence]): IdentifiableDesqDataset = {
    //    val parse = NytUtil.parse
    val parse = (sentence: Sentence, dictionaryBuilder: DictionaryBuilder) => NytUtils.processSentence(-1L, sentence, dictionaryBuilder)
    build[Sentence, WeightedSequence](rawData, parse).asInstanceOf[IdentifiableDesqDataset]
  }

  def load(inputPath: String)(implicit sc: SparkContext): DesqDataset[IdentifiableWeightedSequence] = {
    super.load[IdentifiableWeightedSequence](inputPath)
  }
//
//  def loadFromDelFilea(delFile: RDD[String], dict: Dictionary, usesFids: Boolean = false): IdentifiableDesqDataset = {
//    val dataset = loadFromDelFile[WeightedSequence](delFile, dict, usesFids)
//    dataset.asInstanceOf[IdentifiableDesqDataset]
//  }

  /** Loads data from the specified del file */
  def loadFromDelFile[T<:WeightedSequence:ClassTag](delFile: RDD[String], dict: Dictionary, usesFids: Boolean): DesqDataset[T] = {
    val sequences = delFile.map[IdentifiableWeightedSequence](line => {
      val s = new IdentifiableWeightedSequence(-1L, Array.empty[Int], 1L)
      DelSequenceReader.parseLine(line, s)
      s
    })

    new IdentifiableDesqDataset(sequences, dict, usesFids).asInstanceOf[DesqDataset[T]]
  }

  /** Loads data from the specified del file */
  def loadFromDelFile[T<: WeightedSequence : ClassTag](delFile: String, dict: Dictionary, usesFids: Boolean)(implicit sc: SparkContext): DesqDataset[T] = {
    loadFromDelFile[T](sc.textFile(delFile), dict, usesFids)
  }

}


