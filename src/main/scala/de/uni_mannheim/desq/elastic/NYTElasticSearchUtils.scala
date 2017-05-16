package de.uni_mannheim.desq.elastic

import com.sksamuel.elastic4s.{ElasticsearchClientUri, TcpClient}
import de.uni_mannheim.desq.Desq.initDesq
import de.uni_mannheim.desq.avro.AvroArticle
import de.uni_mannheim.desq.converters.nyt.NytUtil
import de.uni_mannheim.desq.converters.nyt.avroschema.Article
import de.uni_mannheim.desq.io.spark.AvroIO
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.spark._

/**
  * Created by ivo on 08.05.17.
  */


class NYTElasticSearchUtils {
  val settings = Settings.builder().put("cluster.name", "elasticsearch").build()
  val client = TcpClient.transport(settings, ElasticsearchClientUri("localhost", 9300))
  //  TcpClient(settings, ElasticsearchClientUri("localhoast", 9300))
  import com.sksamuel.elastic4s.ElasticDsl._

  def writeArticlesToEs(articles: RDD[Article]) = {
    articles.map(article => {
      case class NYTEsArticle(abstract$: CharSequence, content: CharSequence, publication: CharSequence, onlineSections: CharSequence) /*, sentences: java.util.List[Sentence])*/
      val publicationDate = article.getPublicationMonth + "." + article.getPublicationDayOfMonth + "." + article.getPublicationYear
      val esArticle = NYTEsArticle(article.getAbstract$, article.getContent, publicationDate, article.getOnlineSections) /*, article.getSentences)*/
      esArticle
    }).saveToEs("nyt/article")
    print("done")
  }

  def searchES(query: String, limit: Int = 10): Seq[String] = {

    val resp = client.execute {
      search("nyt" / "article") query query limit limit
    }.await
    resp.ids
  }
}

object NYTElasticSearchUtils extends App {
  val sparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local").remove("spark.serializer")
  sparkConf.set("es.index.auto.create", "true")
  sparkConf.set("es.resource", "nyt/article")
  sparkConf.registerAvroSchemas(Article.SCHEMA$)
  initDesq(sparkConf)
  implicit val sc = new SparkContext(sparkConf)

  val path = "data-local/NYTimesProcessed/results/2007/01"

  val articles = AvroIO.read[AvroArticle](path, AvroArticle.SCHEMA$)

  val nytEs = new NYTElasticSearchUtils
  //  nytEs.writeArticlesToEs(articles)
  val ids = nytEs.searchES("new york", 0)
  ids
}
