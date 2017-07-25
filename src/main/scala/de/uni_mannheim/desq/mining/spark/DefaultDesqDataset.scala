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
class DefaultDesqDataset(sequences: RDD[WeightedSequence], dict: Dictionary, usesFids: Boolean = false) extends DesqDataset[WeightedSequence](sequences, dict, usesFids) {
  private var dictBroadcast: Broadcast[Dictionary] = _

  // -- building ------------------------------------------------------------------------------------------------------

  def this(sequences: RDD[WeightedSequence], source: DefaultDesqDataset, usesFids: Boolean) {
    this(sequences, source.dict, usesFids)
    dictBroadcast = source.dictBroadcast
  }

  /** Returns a copy of this dataset with a new dictionary, containing updated counts and fid identifiers. The
    * original input sequences are "translated" to the new dictionary if needed. */
  override def copyWithRecomputedCountsAndFids(): DefaultDesqDataset = {
    val result = super.copyWithRecomputedCountsAndFids()
    result.asInstanceOf[DefaultDesqDataset]
  }

  /**
    * Builds a DesqDataset from an RDD of rows. Every row corresponds to one article, which may contain many sentences.
    * Every sentence contains tokens. The generated hierarchy is deep.
    */
}

object DefaultDesqDataset extends DesqDatasetCore[WeightedSequence] {

  def buildFromSentences(rawData: RDD[Sentence]): DefaultDesqDataset = {
    //    val parse = NytUtil.parse
    val parse = (sentence: Sentence, dictionaryBuilder: DictionaryBuilder) => NytUtils.processSentence(-1L, sentence, dictionaryBuilder)
    build[Sentence, WeightedSequence](rawData, parse)

  }

  def load(inputPath: String)(implicit sc: SparkContext): DefaultDesqDataset = {
    super.load[WeightedSequence](inputPath)
  }

  /** Loads data from the specified del file */
  override def loadFromDelFile[T <: WeightedSequence : ClassTag](delFile: RDD[String], dict: Dictionary, usesFids: Boolean): DesqDataset[T] = {
    val sequences = delFile.map[WeightedSequence](line => {
      val s = new WeightedSequence(Array.empty[Int], 1L)
      DelSequenceReader.parseLine(line, s)
      s
    })
    new DefaultDesqDataset(sequences, dict, usesFids).asInstanceOf[DesqDataset[T]]
  }

  /** Loads data from the specified del file */
  override def loadFromDelFile[T <: WeightedSequence : ClassTag](delFile: String, dict: Dictionary, usesFids: Boolean)(implicit sc: SparkContext): DesqDataset[T] = {
    loadFromDelFile[T](sc.textFile(delFile), dict, usesFids)
  }

}

