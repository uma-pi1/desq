package de.uni_mannheim.desq.converters.nyt

import java.io.File

import com.google.common.io.Files
import de.uni_mannheim.desq.Desq.initDesq
import de.uni_mannheim.desq.comparing.{DesqCompareMulti, DesqCountMulti}
import de.uni_mannheim.desq.converters.nyt.avroschema.Article
import de.uni_mannheim.desq.dictionary.DefaultDictionaryBuilder
import it.unimi.dsi.fastutil.ints.IntArrayList
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by ivo on 22.03.17.
  */
class NYTConverter(val rootDir: String) {

  val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
  implicit val sc = new SparkContext(conf)
  val builder = new DefaultDictionaryBuilder()

  val itemFids = new IntArrayList()

  val subDirs = ConvertNyt.getLeafSubdirs(new File(rootDir))


  def readNytSpark(rootDir: String): (RDD[(String, mutable.HashSet[String])], Dataset[Row]) = {
    //    val path = "./data-local/NYTimesProcessed/results/2005"
    val subDirs = ConvertNyt.getLeafSubdirs(new File(rootDir))
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
    //    val articles = spark.read.format("com.databricks.spark.avro").option("avroSchema", schema.toString).load(path).cache()

    val sentences = articles.select(articles.col("filename"), explode(articles.col("sentences")))

    val tokens = sentences.select(explode(sentences.col("col.tokens.word")), articles.col("filename"))
//    val index = tokens.map(row => (row.getString(0),row.getString(1)))
    val index = tokens.rdd.map(row => (row.getString(0), row.getString(1)))

    val initialSet = mutable.HashSet.empty[String]
    val addTokenDocPair = (set: mutable.HashSet[String], pair: String) => set += pair
    val mergeSets = (set1: mutable.HashSet[String], set2: mutable.HashSet[String]) => set1 ++= set2
//    val finalIndex = index.groupBy(col("col.tokens.word") as "word").agg(collect_set("filename") as "docs")
    val finalIndex = index.aggregateByKey(initialSet)(addTokenDocPair, mergeSets)

    (finalIndex, articles)

  }

  def filterArticlesByKeyword(uniqueByKey: RDD[(String, mutable.HashSet[String])] , articles: Dataset[Row], key: String): String = {
//    val filter = uniqueByKey.filter(col("word").isin(key))
    val filter = uniqueByKey.filter(_._1.matches(key))
    //.filter(!_._1.matches("opposing"))
    val articlesFiltered = articles.filter(articles.col("filename").isin(filter.collect().apply(0)._2.toList: _*))
//    val articlesFiltered = articles.filter(articles.col("filename").isin(filter.collect().apply(0).getSeq[Any](1): _*))
    //    val filterRight = uniqueByKey.filter(_._1.matches("Clinton")).filter(!_._1.matches("Trump"))
    //    val articlesFilteredRight = articles.filter(articles.col("filename").isin(filterRight.collect().apply(0)._2.toList: _*))

    //    val roomRight = new Newsroom("./data-local/processed/results/", "right")
    //    roomRight.initialize()
    //    for (article <- articlesFilteredRight.collect()) {
    //      val avroArticle = convertToArticle(article)
    //      roomRight.processArticle(avroArticle)
    //    }
    //    roomRight.shutdown()
    val room = new Newsroom("./data-local/processed/results/", key)
    room.initialize()
    for (article <- articlesFiltered.collect()) {
      val avroArticle = NytUtil.convertToArticle(article)
      room.processArticle(avroArticle)
    }
    room.shutdown()

    "./data-local/processed/results/" + key
  }

  def runAlgorithms(left: String, right: String, patternExpression: String, sigma: Int) {
    val multi = DesqCountMulti.run(left, right, patternExpression, sigma)
    val compare = new DesqCompareMulti(multi)
    val leftSeq = compare.compare(compare.leftDataset, compare.rightDataset)
    val rightSeq = compare.compare(compare.rightDataset, compare.leftDataset)
    println("*******************************************LEFT SEQUENCES*******************************************")
    leftSeq.print()
    println("*******************************************RIGHT SEQUENCES******************************************")
    rightSeq.print()
    //    compare.compare(compare.leftCurrent, compare.itLeft, compare.rightCurrent, compare.itRight)
  }



}

object NYTConverter extends App {
  val converter = new NYTConverter("data-local/NYTimesProcessed/results/2006/01")
  val rootDir = "./data-local/NYTimesProcessed/results/2006/01"
  val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
  initDesq(conf)
  implicit val sc = new SparkContext(conf)
  converter.readNytSpark(rootDir)

}

