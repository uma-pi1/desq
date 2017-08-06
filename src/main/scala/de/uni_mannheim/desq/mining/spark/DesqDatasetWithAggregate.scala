package de.uni_mannheim.desq.mining.spark

  import de.uni_mannheim.desq.avro.Sentence
  import de.uni_mannheim.desq.converters.nyt.NytUtils
  import de.uni_mannheim.desq.dictionary.{Dictionary, DictionaryBuilder}
  import de.uni_mannheim.desq.io.DelSequenceReader
  import de.uni_mannheim.desq.mining.{AggregatedSequence, WeightedSequence}
  import org.apache.spark.SparkContext
  import org.apache.spark.broadcast.Broadcast
  import org.apache.spark.rdd.RDD

  import scala.reflect.ClassTag
/**
  * Created by ivo on 05.08.17.
  */
class DesqDatasetWithAggregate(sequences: RDD[AggregatedSequence], dict: Dictionary, usesFids: Boolean) extends DesqDataset[AggregatedSequence](sequences, dict, usesFids){
    private var dictBroadcast: Broadcast[Dictionary] = _

    // -- building ------------------------------------------------------------------------------------------------------

    def this(sequences: RDD[AggregatedSequence], source: DesqDatasetWithAggregate, usesFids: Boolean) {
      this(sequences, source.dict, usesFids)
      dictBroadcast = source.dictBroadcast
    }

    /** Returns a copy of this dataset with a new dictionary, containing updated counts and fid identifiers. The
      * original input sequences are "translated" to the new dictionary if needed. */
    override def copyWithRecomputedCountsAndFids(): DesqDatasetWithAggregate = {
      val result = super.copyWithRecomputedCountsAndFids()
      result.asInstanceOf[DesqDatasetWithAggregate]
    }

    /**
      * Builds a DesqDataset from an RDD of rows. Every row corresponds to one article, which may contain many sentences.
      * Every sentence contains tokens. The generated hierarchy is deep.
      */
  }

  object DesqDatasetWithAggregate extends DesqDatasetCore[AggregatedSequence] {

    def buildFromSentences(rawData: RDD[Sentence]): DesqDatasetWithAggregate = {
      //    val parse = NytUtil.parse
      val parse = (sentence: Sentence, dictionaryBuilder: DictionaryBuilder) => NytUtils.processSentence(-1L, sentence, dictionaryBuilder)
      build[Sentence, AggregatedSequence](rawData, parse).asInstanceOf[DesqDatasetWithAggregate]
    }

    //  /** Loads data from the specified del file */
    //  override def loadFromDelFile[T <: AggregatedSequence](delFile: String, dict: Dictionary, usesFids: Boolean = false)(implicit sc: SparkContext): DesqDatasetWithAggregate = {
    //    this.loadFromDelFile(sc.textFile(delFile), dict, usesFids)
    //  }
    //
    //  /** Loads data from the specified del file */
    //  override def loadFromDelFile[F <: AggregatedSequence : ClassTag](delFile: RDD[String], dict: Dictionary, usesFids: Boolean): DesqDatasetWithAggregate = {
    //    val result = super.loadFromDelFile[F](delFile, dict, usesFids)
    //    result.asInstanceOf[DesqDatasetWithAggregate]
    //  }

    def loadFromDelFilea(delFile: RDD[String], dict: Dictionary, usesFids: Boolean = false): DesqDatasetWithAggregate = {
      val dataset = loadFromDelFile[AggregatedSequence](delFile, dict, usesFids)
      dataset.asInstanceOf[DesqDatasetWithAggregate]
    }



    /** Loads data from the specified del file */
    override def loadFromDelFile[T <: WeightedSequence : ClassTag](delFile: RDD[String], dict: Dictionary, usesFids: Boolean): DesqDataset[T] = {
      val sequences = delFile.map[AggregatedSequence](line => {
        val s = new AggregatedSequence(Array.empty[Int], new Array[Long](1))
        DelSequenceReader.parseLine(line, s)
        s
      })
      new DesqDatasetWithAggregate(sequences, dict, usesFids).asInstanceOf[DesqDataset[T]]
    }

    /** Loads data from the specified del file */
    override def loadFromDelFile[T <: WeightedSequence : ClassTag](delFile: String, dict: Dictionary, usesFids: Boolean)(implicit sc: SparkContext): DesqDataset[T] = {
      loadFromDelFile[T](sc.textFile(delFile), dict, usesFids)
    }

  }

