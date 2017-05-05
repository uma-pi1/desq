package de.uni_mannheim.desq.converters.nyt

import java.io.File
import java.util

import com.google.common.io.Files
import de.uni_mannheim.desq.Desq.initDesq
import de.uni_mannheim.desq.converters.nyt.avroschema.{Article, Sentence, Span}
import de.uni_mannheim.desq.examples.spark.DesqBuilderExample.getClass
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.{AvroInputFormat, AvroWrapper}
import org.apache.hadoop.io.NullWritable
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.JavaConverters._

/**
  * Created by ivo on 03.05.17.
  */
object NytUtil {

  def loadArticlesFromFile(rootDir: String)(implicit sc: SparkContext): RDD[Row] = {
    val subDirs = ConvertNyt.getLeafSubdirs(new File(rootDir))
    if (subDirs.size() == 0) {
      subDirs.add(new File(rootDir))
    }
    val paths = for (folder: File <- subDirs.asScala;
                     file: File <- folder.listFiles();
                     if Files.getFileExtension(file.getPath()).endsWith("avro")
    ) yield {
      file.getPath()
    }
    val job = AvroUtils.getJobInputKeyAvroSchema(Article.SCHEMA$)
    val schema = Article.SCHEMA$
    val spark = SparkSession.builder().master("local").getOrCreate()
    val articles = paths.toList.map(path => spark.read.format("com.databricks.spark.avro").option("avroSchema", schema.toString).load(path)).reduce((df1, df2) => df1.union(df2))
    articles.rdd
  }


  def convertToArticle(row: Row): Article = {
    val article = new Article()
    article.setAbstract$(row.getString(0))
    article.setSentences(convertToSentence(row.getSeq[Any](2)))
    article
  }

  def convertToSentence(sentencesRaw: Seq[Any]): util.List[Sentence] = {
    val sentences = for (sentenceRaw <- sentencesRaw) yield {
      val sentence = new Sentence
      sentence.setTokens(convertToTokens(sentenceRaw.asInstanceOf[GenericRowWithSchema].getSeq(0)))
      sentence.setSId(sentenceRaw.asInstanceOf[GenericRowWithSchema].getInt(1))
      sentence.setSg(sentenceRaw.asInstanceOf[GenericRowWithSchema].getString(2))
      sentence.setDp(sentenceRaw.asInstanceOf[GenericRowWithSchema].getString(3))
      sentence.setSpan(convertToSpan(sentenceRaw.asInstanceOf[GenericRowWithSchema].get(4)))
      sentence
    }
    sentences.asJava
  }

  def convertToTokens(tokensRaw: Seq[Any]): util.List[avroschema.Token] = {
    val tokens = for (tokenRaw <- tokensRaw) yield {
      val token = new avroschema.Token()
      token.setPos(tokenRaw.asInstanceOf[GenericRowWithSchema].getString(0))
      token.setNer(tokenRaw.asInstanceOf[GenericRowWithSchema].getString(1))
      token.setSpan(convertToSpan(tokenRaw.asInstanceOf[GenericRowWithSchema].get(2)))
      token.setLemma(tokenRaw.asInstanceOf[GenericRowWithSchema].getString(3))
      token.setWord(tokenRaw.asInstanceOf[GenericRowWithSchema].getString(4))
      token.setIndex(tokenRaw.asInstanceOf[GenericRowWithSchema].getInt(5))
      token
    }
    tokens.asJava
  }

  def convertToSpan(rawSpan: Any): Span = {
    val span = new Span()
    span.setStartIndex(rawSpan.asInstanceOf[GenericRowWithSchema].getInt(0))
    span.setEndIndex(rawSpan.asInstanceOf[GenericRowWithSchema].getInt(1))
    span
  }

  def loadArticles(path:String)(implicit sc:SparkContext) : Unit ={
    val avroRDD = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](path)
//    avroRDD.map(l => new String(l._1.datum.g{ et("username").toString()) } ).first
    //avroRDD.collect().apply(0)._1.datum().get(2)
//    avroRDD.flatMap(a=>a._1.datum().get(2).asInstanceOf[Array[Sentence]].toIterator)
    avroRDD.flatMap(a=>a._1.datum().get(2).asInstanceOf[TraversableOnce[GenericRecord]])
    avroRDD
  }

  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    initDesq(conf)
    implicit val sc = new SparkContext(conf)
    val dir = "data-local/NYTimesProcessed/results/2008/01/"
    loadArticlesFromFile(dir)
  }
}
