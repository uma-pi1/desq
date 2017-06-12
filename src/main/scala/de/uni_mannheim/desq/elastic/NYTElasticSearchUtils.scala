package de.uni_mannheim.desq.elastic

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.{ElasticsearchClientUri, TcpClient}
import de.uni_mannheim.desq.Desq.initDesq
import de.uni_mannheim.desq.avro.AvroArticle
import de.uni_mannheim.desq.converters.nyt.NytUtil
import de.uni_mannheim.desq.mining.spark.DesqDataset
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.spark._

import scala.collection.JavaConversions._
import scala.collection.immutable.BitSet
import scala.collection.mutable

/**
  * Created by ivo on 08.05.17.
  *
  * Utility class to create a DesqDataSet with corresponding ElasticSearch Index and search the index
  */


class NYTElasticSearchUtils extends Serializable {

  /**
    * Loads the Articles from 'path_in' and then gives unique ids to each one.
    * Loads the Articles with their ID to ElasticSearch
    * Creates a DataSet with IdentifiableWeightedSequences, where each one has the Article ID
    *
    * @param path_in  Directory where the source avro articles lay
    * @param path_out Directory where the DataSet should be written
    * @param sc       SparkContext
    * @return
    */
  def createIndexAndDataset(path_in: String, path_out: String, index: String)(implicit sc: SparkContext) = {
    val articles = NytUtil.loadArticlesFromFile(path_in)
    val articlesWithId = articles.zipWithUniqueId()
    this.writeArticlesToEs(articlesWithId, index: String)
    val sentences = articlesWithId.map(f => (f._2, f._1)).flatMapValues(f => f.getSentences)
    val dataset = DesqDataset.buildFromSentencesWithID(sentences)
    dataset.save(path_out)

  }

  /**
    * Writes the RDD of Articles with their corresponding IDs to Elasticsearch
    *
    * @param articles RDD containing Articles with IDs
    */
  def writeArticlesToEs(articles: RDD[(AvroArticle, Long)], index: String) = {
    case class NYTEsArticle(abstract$: String, content: String, publication: String, onlineSections: String) /*, sentences: java.util.List[Sentence])*/
    articles.map(a => {
      val article = a._1
      val publicationDate = article.getPublicationMonth + "." + article.getPublicationDayOfMonth + "." + article.getPublicationYear
      val esArticle = NYTEsArticle(article.getAbstract$, article.getContent, publicationDate, article.getOnlineSections) /*, article.getSentences)*/
      (String.valueOf(a._2), esArticle)
    }).saveToEsWithMeta(index)
    print("done")
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
      search(index) storedFields "_id" query query_s fetchSource false limit limit_i
    }.await
    val ids = resp.ids.map(id => id.toLong)
    ids
  }

  def searchESCombines(index: String, limit_i: Int, queries: String*): mutable.Map[Long, mutable.BitSet] = {
    val map = mutable.Map[Long, mutable.BitSet]()
    for (i <- 0 to queries.size-1) yield {
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

  val path_in = "data-local/NYTimesProcessed/results/2007/01/"
  val path_out = "data-local/processed/sparkconvert/es/"

  val nytEs = new NYTElasticSearchUtils
  nytEs.createIndexAndDataset(path_in, path_out, "nyt200701/article")

  val dataset = DesqDataset.load(path_out)
  val ids_1 = nytEs.searchES("usa", "nyt/article")

  val ids_2 = nytEs.searchES("germany", "nyt/article")

  println(s"count of ids: ${ids_1.size}")
  println(s"count before filter: ${dataset.sequences.count()}")
  val usa = new DesqDataset(dataset.sequences.filter(f => ids_1.contains(String.valueOf(f.id))), dataset.dict, true)
  val germany = new DesqDataset(dataset.sequences.filter(f => ids_2.contains(String.valueOf(f.id))), dataset.dict, true)

  val germany_updated = germany.copyWithRecomputedCountsAndFids()
  val usa_updated = usa.copyWithRecomputedCountsAndFids()
  println(s"count after filter: ${usa.sequences.count()}")
  println(s"count after filter: ${germany.sequences.count()}")
}

/**
  * Object that holds the ElasticSeach Connection
  */
object ESConnection extends Serializable {
  val settings = Settings.builder().put("cluster.name", "elasticsearch").build()
  lazy val client = TcpClient.transport(settings, ElasticsearchClientUri("localhost", 9300))
}
