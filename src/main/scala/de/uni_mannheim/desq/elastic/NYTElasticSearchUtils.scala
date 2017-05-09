package de.uni_mannheim.desq.elastic

import de.uni_mannheim.desq.Desq.initDesq
import de.uni_mannheim.desq.converters.nyt.NytUtil
import de.uni_mannheim.desq.converters.nyt.avroschema.Article
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * Created by ivo on 08.05.17.
  */


class NYTElasticSearchUtils {

  def writeArticlesToEs(articles: RDD[Article]) = {
    articles.map(article => {
      case class NYTEsArticle(abstract$: CharSequence, content: CharSequence, publication: CharSequence, onlineSections: CharSequence) /*, sentences: java.util.List[Sentence])*/
      val publicationDate = article.getPublicationMonth + "." + article.getPublicationDayOfMonth + "." + article.getPublicationYear
      val esArticle = NYTEsArticle(article.getAbstract$, article.getContent, publicationDate, article.getOnlineSections) /*, article.getSentences)*/
      esArticle
    }).saveToEs("nyt/article")
    print("done")
  }

}

object NYTElasticSearchUtils extends App {
  val sparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local").remove("spark.serializer")
  sparkConf.set("es.index.auto.create", "true")
  sparkConf.set("es.resource", "nyt/article")
  initDesq(sparkConf)
  implicit val sc = new SparkContext(sparkConf)

  val nyt_avro = "data-local/NYTimesProcessed/results/2007/01/"

//  First Try
  /*val conf = Job.getInstance(sc.hadoopConfiguration)
  FileInputFormat.setInputPaths(conf, nyt_avro)
  val records = sc.newAPIHadoopRDD(conf.getConfiguration, classOf[AvroKeyInputFormat[AvroArticle]],
  classOf[AvroKey[AvroArticle]], classOf[NullWritable])*/

//  Second Try
  /*  import com.databricks.spark.avro._
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder().master("local").getOrCreate()
    val sqlContext = spark.sqlContext
    import spark.implicits._
    val df = spark.read.avro(nyt_avro)*/


// Third Try
/*      val articles_raw = sc.newAPIHadoopFile(nyt_avro, classOf[AvroKeyInputFormat[Article]], classOf[AvroKey[Article]], classOf[NullWritable], job.getConfiguration)
    val articles = articles_raw.map{case (a, _) => a.datum()}*/

//  Fourth Try
  /*val articles_raw  = sc.hadoopFile[AvroWrapper[Article], NullWritable, AvroInputFormat[Article]](nyt_avro)
  val articles = articles_raw.map(tuple => {
    val article = tuple._1.datum().asInstanceOf[Article]
    article
  })*/

/*
// Fifth Try
  import org.apache.avro.Schema
  import org.apache.spark.sql.SparkSession

  val schema = new Schema.Parser().parse(new File("src/main/avro/AvroArticle.avsc"))
  val spark = SparkSession.builder().master("local").getOrCreate()
  val records = spark
    .read
    .format("com.databricks.spark.avro")
    .option("avroSchema", schema.toString)
    .load(nyt_avro)*/


  val articles = NytUtil.loadArticlesFromFile(nyt_avro).map(r => NytUtil.convertToArticle(r))
  val nytEs = new NYTElasticSearchUtils
  nytEs.writeArticlesToEs(articles)
}
