package de.uni_mannheim.desq.mining.spark

import de.uni_mannheim.desq.avro.Sentence
import de.uni_mannheim.desq.converters.nyt.NytUtils
import de.uni_mannheim.desq.dictionary.{Dictionary, DictionaryBuilder}
import de.uni_mannheim.desq.io.DelSequenceReader
import de.uni_mannheim.desq.mining.{AggregatedWeightedSequence, WeightedSequence}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by ivo on 04.07.17.
  */
class DefaultDesqDatasetWithAggregates(sequences: RDD[AggregatedWeightedSequence], dict: Dictionary, usesFids: Boolean) extends DesqDataset[AggregatedWeightedSequence](sequences, dict, usesFids){
  private var dictBroadcast: Broadcast[Dictionary] = _

  // -- building ------------------------------------------------------------------------------------------------------

  def this(sequences: RDD[AggregatedWeightedSequence], source: DefaultDesqDatasetWithAggregates, usesFids: Boolean) {
    this(sequences, source.dict, usesFids)
    dictBroadcast = source.dictBroadcast
  }

  /** Returns a copy of this dataset with a new dictionary, containing updated counts and fid identifiers. The
    * original input sequences are "translated" to the new dictionary if needed. */
  override def copyWithRecomputedCountsAndFids(): DefaultDesqDatasetWithAggregates = {
    val result = super.copyWithRecomputedCountsAndFids()
    result.asInstanceOf[DefaultDesqDatasetWithAggregates]
  }

  /**
    * Builds a DesqDataset from an RDD of rows. Every row corresponds to one article, which may contain many sentences.
    * Every sentence contains tokens. The generated hierarchy is deep.
    */
}

object DefaultDesqDatasetWithAggregates extends DesqDatasetCore[AggregatedWeightedSequence] {

  def buildFromSentences(rawData: RDD[Sentence]): DefaultDesqDatasetWithAggregates = {
    //    val parse = NytUtil.parse
    val parse = (sentence: Sentence, dictionaryBuilder: DictionaryBuilder) => NytUtils.processSentence(-1L, sentence, dictionaryBuilder)
    build[Sentence, AggregatedWeightedSequence](rawData, parse).asInstanceOf[DefaultDesqDatasetWithAggregates]
  }

  //  /** Loads data from the specified del file */
  //  override def loadFromDelFile[T <: AggregatedWeightedSequence](delFile: String, dict: Dictionary, usesFids: Boolean = false)(implicit sc: SparkContext): DefaultDesqDatasetWithAggregates = {
  //    this.loadFromDelFile(sc.textFile(delFile), dict, usesFids)
  //  }
  //
  //  /** Loads data from the specified del file */
  //  override def loadFromDelFile[F <: AggregatedWeightedSequence : ClassTag](delFile: RDD[String], dict: Dictionary, usesFids: Boolean): DefaultDesqDatasetWithAggregates = {
  //    val result = super.loadFromDelFile[F](delFile, dict, usesFids)
  //    result.asInstanceOf[DefaultDesqDatasetWithAggregates]
  //  }

  def loadFromDelFilea(delFile: RDD[String], dict: Dictionary, usesFids: Boolean = false): DefaultDesqDatasetWithAggregates = {
    val dataset = loadFromDelFile[AggregatedWeightedSequence](delFile, dict, usesFids)
    dataset.asInstanceOf[DefaultDesqDatasetWithAggregates]
  }



  /** Loads data from the specified del file */
  override def loadFromDelFile[T <: WeightedSequence : ClassTag](delFile: RDD[String], dict: Dictionary, usesFids: Boolean): DesqDataset[T] = {
    val sequences = delFile.map[AggregatedWeightedSequence](line => {
      val s = new AggregatedWeightedSequence(Array.empty[Int], 1L, 1L)
      DelSequenceReader.parseLine(line, s)
      s
    })
    new DefaultDesqDatasetWithAggregates(sequences, dict, usesFids).asInstanceOf[DesqDataset[T]]
  }

  /** Loads data from the specified del file */
  override def loadFromDelFile[T <: WeightedSequence : ClassTag](delFile: String, dict: Dictionary, usesFids: Boolean)(implicit sc: SparkContext): DesqDataset[T] = {
    loadFromDelFile[T](sc.textFile(delFile), dict, usesFids)
  }

}
