package de.uni_mannheim.desq.elastic

import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldType.{IntegerType, StringType}
import com.sksamuel.elastic4s.xpack.security.XPackElasticClient
import com.sksamuel.elastic4s.{ElasticsearchClientUri, TcpClient}
import de.uni_mannheim.desq.Desq.initDesq
import de.uni_mannheim.desq.avro.AvroArticle
import de.uni_mannheim.desq.converters.nyt.NytUtil
import de.uni_mannheim.desq.mining.IdentifiableWeightedSequence
import de.uni_mannheim.desq.mining.spark.{DesqDatasetPartitionedWithID, IdentifiableDesqDataset}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.spark._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration.Duration

/**
  * Created by ivo on 08.05.17.
  *
  * Utility class to create a DesqDataSet with corresponding ElasticSearch Index and search the index
  *
  *
  */
//TODO: Method that allows mixture of metadata and keyword queries
//TODO: Mehtod that calls the corresponding query depending on metadata or keywords beeing set

class NYTElasticSearchUtils extends Serializable {

  /**
    * Loads the Articles from 'path_in' and then gives unique ids to each one.
    * Loads the Articles with their ID to ElasticSearch
    * Creates a DataSet with IdentifiableWeightedSequences, where each one has the Article ID
    * Dataset is stored on disk
    *
    * @param path_in  Directory where the source avro articles lay
    * @param path_out Directory where the DataSet should be written
    * @param sc       SparkContext
    */
  def createIndexAndDataset(path_in: String, path_out: String, index: String)(implicit sc: SparkContext) = {
    val articles = NytUtil.loadArticlesFromFile(path_in)
    val articlesWithId = articles.zipWithUniqueId()
    val indexTime = Stopwatch.createStarted
    this.createNYTIndex(index)
    indexTime.stop()
    println(s"Creating the index took ${indexTime.elapsed(TimeUnit.MILLISECONDS)}}")
    val elasticTime = Stopwatch.createStarted
    this.writeArticlesToEs(articlesWithId, index + "/article")
    elasticTime.stop
    println(s"Writing to Elastic took ${elasticTime.elapsed(TimeUnit.MILLISECONDS)}}")
    val datasetTime = Stopwatch.createStarted
    val sentences = articlesWithId.map(f => (f._2, f._1)).flatMapValues(f => f.getSentences)
    val dataset = IdentifiableDesqDataset.buildFromSentencesWithID(sentences)
//    val dataset2 = dataset.save(path_out)
    val partitionedDataset = DesqDatasetPartitionedWithID.partitionById[IdentifiableWeightedSequence](dataset, new HashPartitioner(48))
    partitionedDataset.save(path_out)
    datasetTime.stop()
    println(s"Creating the dataset took ${datasetTime.elapsed(TimeUnit.MILLISECONDS)}")

  }

  /**
    * Create a new index for the NewYorkTimes Corpus
    *
    * @param indexS Name for the Index
    */
  def createNYTIndex(indexS:String) = {


    ESConnection.client.execute {
//      create index indexS indexSetting("index.store.type", "mmapfs") indexSetting("index.store.preload", Array("nvd", "dvd", "tim")) mappings (
        createIndex(indexS) mappings (
        mapping("article").as(
          textField("abstract$"),
          textField("content"),
          textField("onlineSections"),
          intField("publicationYear"),
          //          dateField("publicationDate")
          dateField("publicationDate"),
          dynamicDateField()

        ) dateDetection true dynamicDateFormats ("yyyy/MM/dd")
        )

    }
  }


  /**
    * Writes the RDD of Articles with their corresponding IDs to Elasticsearch
    *
    * @param articles RDD containing Articles with IDs
    * @param index    Name of the Index that we want to write to
    */
  def writeArticlesToEs(articles: RDD[(AvroArticle, Long)], index: String) = {
    case class NYTEsArticle(abstract$: String, content: String, publicationDate: String, publicationYear: Int, onlineSections: String) /*, sentences: java.util.List[Sentence])*/
    articles.map(a => {
      val article = a._1
      val day = if (article.getPublicationYear.isEmpty) "01" else if (article.getPublicationDayOfMonth.toInt < 9) "0" + article.getPublicationDayOfMonth else article.getPublicationDayOfMonth
      val month = if (article.getPublicationYear.isEmpty) "01" else if (article.getPublicationMonth.toInt < 9) "0" + article.getPublicationMonth else article.getPublicationMonth
      val year = if (article.getPublicationYear.isEmpty) 9999 else article.getPublicationYear.toInt
      val publicationDate = year.toString + "/" + month + "/" + day
//      val publicationDate = year.toString + month + day
      val esArticle = NYTEsArticle(article.getAbstract$, article.getContent, publicationDate, year, article.getOnlineSections) /*, article.getSentences)*/
      (String.valueOf(a._2), esArticle)
    }).saveToEsWithMeta(index)
    print("done loading articles to ES")
  }

  /**
    * Search ElasticSearch with a query and a limit
    *
    * @param query_s Query String to search for
    * @param limit_i Limit of Results
    * @return IDs of the matching articles as Strings
    */
  def searchES(query_s: String, index: String, limit_i: Int = 10000): Seq[Long] = {
    val resp = ESConnection.client.execute {
      search(index) query query_s fetchSource false storedFields "_id" limit limit_i
    }.await(Duration(30, "seconds"))
    val ids = resp.ids.map(id => id.toLong)
    ids
  }

  /**
    * Search ES Index by a date range query
    *
    * @param min     Lower Date bound in format yyyyMMdd
    * @param max     Upper Date bound in format yyyyMMdd
    * @param index   Name of the index
    * @param limit_i Limit of the results that we return
    * @return
    */

  def searchESByDateRange(min: String, max: String, index: String, limit_i: Int = 10000): Seq[Long] = {
    val resp = ESConnection.client.execute {
      search(index) storedFields "publicationDate" query {
        rangeQuery("publicationDate") gte min lte max
      }
    }.await(Duration(30, "seconds"))
    val ids = resp.ids.map(id => id.toLong)
    ids
  }

  /**
    * Search ES Index for a certain year
    *
    * @param year Year to query for
    * @param index Name of the index
    * @param limit_i Limit of the results that we return
    * @return
    */
  def searchESByYear(year: Int, index: String, limit_i: Int = 10000): Seq[Long] = {
    val resp = ESConnection.client.execute{
      search(index) termQuery("publicationYear", year) limit limit_i
    }.await(Duration(30, "seconds"))
    val ids = resp.ids.map(id => id.toLong)
    ids
  }


  /**
    * Combines the results of two or more queries to elasticsearch.
    * The information for which query the result is relevant is encoded in the bitset
    *
    * @param index   Name of the Index to query
    * @param limit_i Size Limit of the resultset
    * @param queries two or more query strings
    * @return Index Structure that maps ids to a bitset
    */
  def searchESCombines(index: String, limit_i: Int, queries: String*): mutable.Map[Long, mutable.BitSet] = {
    val map = mutable.Map[Long, mutable.BitSet]()
    for (i <- 0 to queries.size - 1 ) yield {
      val ids = searchES(queries.get(i), index, limit_i)
      for (id <- ids) {
        val newBitSet = map.getOrElse(id, mutable.BitSet(i)) += i
        map += (id -> newBitSet)
      }
    }
    map
  }

}

object NYTElasticSearchUtils extends App {
  val sparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local")
  sparkConf.set("es.index.auto.create", "true")
  sparkConf.set("es.resource", "nyt/article")
  initDesq(sparkConf)
  implicit val sc = new SparkContext(sparkConf)


//  val path_in = "data-local/NYTimesProcessed/results/"
//  val path_out = "data-local/processed/sparkconvert/es_all_v2/"
//
  val nytEs = new NYTElasticSearchUtils
  nytEs.createNYTIndex("test1")
//  nytEs.createIndexAndDataset(path_in, path_out, "nyt_v2")
//
//  val dataset = IdentifiableDesqDataset.load(path_out)
//  val ids_1 = nytEs.searchES("usa", "nyt/article")
//
//  val ids_2 = nytEs.searchES("germany", "nyt/article")
//
//  println(s"count of ids: ${ids_1.size}")
//  println(s"count before filter: ${dataset.sequences.count()}")
//  val usa = new IdentifiableDesqDataset(dataset.sequences.filter(f => ids_1.contains(String.valueOf(f.id))), dataset.dict, true)
//  val germany = new IdentifiableDesqDataset(dataset.sequences.filter(f => ids_2.contains(String.valueOf(f.id))), dataset.dict, true)
//
//  val germany_updated = germany.copyWithRecomputedCountsAndFids()
//  val usa_updated = usa.copyWithRecomputedCountsAndFids()
//  println(s"count after filter: ${usa.sequences.count()}")
//  println(s"count after filter: ${germany.sequences.count()}")
}

/**
  * Object that holds the ElasticSeach Connection
  */
object ESConnection extends Serializable {
  val settings = Settings.builder().put("cluster.name", "elasticsearch").build()
//  lazy val client = TcpClient.transport(settings, ElasticsearchClientUri("localhost", 9300))
  lazy val client = XPackElasticClient.apply(settings, ElasticsearchClientUri("localhost", 9300))

}
