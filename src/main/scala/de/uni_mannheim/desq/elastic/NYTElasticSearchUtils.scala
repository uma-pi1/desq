package de.uni_mannheim.desq.elastic

import java.net.URI
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.xpack.security.XPackElasticClient
import de.uni_mannheim.desq.Desq.initDesq
import de.uni_mannheim.desq.avro.{AvroArticle, Sentence}
import de.uni_mannheim.desq.converters.nyt.NytUtil
import de.uni_mannheim.desq.io.spark.AvroIO
import de.uni_mannheim.desq.mining.IdentifiableWeightedSequence
import de.uni_mannheim.desq.mining.spark.{DesqDatasetPartitionedWithID, IdentifiableDesqDataset}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.NullWritable
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

class NYTElasticSearchUtils extends Serializable {

  /**
    * Loads the Articles from 'path_in' and then gives unique ids to each one.
    * Loads the Articles with their ID to ElasticSearch
    * Creates a DataSet with IdentifiableWeightedSequences, where each one has the Article ID
    * Dataset is stored on disk
    *
    * @param path_in  Directory where the source avro articles lay
    * @param path_out Directory where the DataSet should be written
    * @param index    Name of the Index
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
    ESConnection.client.execute{
      updateSettings(index) set("max_result_window","1800000")
    }
    partitionedDataset.save(path_out)
    datasetTime.stop()
    println(s"Creating the dataset took ${datasetTime.elapsed(TimeUnit.MILLISECONDS)}")

  }


  /** Creates the Index and Dataset used for the Experimental Evaluation of the Thesis
    *
    * @param path_in  Directory where the source avro articles are read from
    * @param path_out Directory where the DataSet is written to
    * @param index    Name of the Index
    * @param sc       Spark Context
    */
  def createIndexAndDatasetEval(path_in: String, path_out: String, index: String)(implicit sc: SparkContext) = {
    val articles = NytUtil.loadArticlesFromFile(path_in)
    val articlesWithId = articles.zipWithUniqueId().cache
    val indexTime = Stopwatch.createStarted
    this.createNYTIndex(index)
    indexTime.stop()
    println(s"Creating the index took ${indexTime.elapsed(TimeUnit.MILLISECONDS)}}")
    val elasticTime = Stopwatch.createStarted
    articlesWithId.unpersist()
    val sampleArticles = articlesWithId.sample(false, 0.005, 1337)

    val fileSystem = FileSystem.get(new URI(path_out), sampleArticles.context.hadoopConfiguration)

    // write sample docs to file
    val docsPath = s"$path_out/rawdocs"
    AvroIO.write(docsPath, sampleArticles.take(1)(0)._1.getSchema, sampleArticles.map(x=>x._1))


    this.writeArticlesToEs(sampleArticles, index + "/article")

    val datasetTime = Stopwatch.createStarted
    val sampleSequences = sampleArticles.map(f => (f._2, f._1)).flatMapValues(f => f.getSentences)
    val dataset = IdentifiableDesqDataset.buildFromSentencesWithID(sampleSequences)
    val sampleSize = sampleSequences.count()
    println(s"We have ${sampleSize} in our index")
    val parts = math.ceil(sampleSize / 150000).toInt
    val partitionedDataset = DesqDatasetPartitionedWithID.partitionById[IdentifiableWeightedSequence](dataset, new HashPartitioner(parts))
    partitionedDataset.save(path_out)
    datasetTime.stop()
    ESConnection.client.execute{
      updateSettings(index) set("max_result_window","1800000")
    }
    println(s"Creating the dataset took ${datasetTime.elapsed(TimeUnit.MILLISECONDS)}")
    elasticTime.stop
    println(s"Writing to Elastic took ${elasticTime.elapsed(TimeUnit.MILLISECONDS)}}")
  }


  /**
    * Create a new index for the NewYorkTimes Corpus
    *
    * @param indexS Name for the Index
    */
  def createNYTIndex(indexS: String) = {


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
//    ESConnection.client.execute {
//      updateSettings(indexS) set("max_result_window","1800000")
//    }

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
    val resp = if(query_s.equals("*")) {
      ESConnection.client.execute {
        search(index) query {
          constantScoreQuery(
            wildcardQuery("content", query_s)
          )
        } fetchSource false storedFields "_id" limit limit_i
      }.await(Duration(30L, TimeUnit.SECONDS))
    } else {
      ESConnection.client.execute {
        search(index) query {
          constantScoreQuery(
            matchQuery("content", query_s)
          )
        } fetchSource false storedFields "_id" limit limit_i
      }.await(Duration(30L, TimeUnit.SECONDS))
    }
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
        constantScoreQuery(
          rangeQuery("publicationDate") gte min lte max
        )
      } fetchSource false storedFields "_id" limit limit_i
    }.await(Duration(270L, TimeUnit.SECONDS))
    val ids = resp.ids.map(id => id.toLong)
    ids
  }

  /**
    * Search the Collection with a date range and a term
    * @param query_s Keyword query
    * @param min lower bound of the date range
    * @param max upper bound of the date range
    * @param index name of the index
    * @param limit_i limit for the amount of returned results
    * @return Collection of IDs
    */
  def searchESDateRangeAndTerm(query_s: String, min: String, max: String, index: String, limit_i: Int = 10000): Seq[Long] = {
    val resp = if (query_s.equals("*")) {
      ESConnection.client.execute {
        search(index) query
          boolQuery.
            must(
            rangeQuery("publicationDate") gte min lte max,
          wildcardQuery("content", query_s)
            ) fetchSource false storedFields "_id" limit limit_i
      }.await(Duration(90, TimeUnit.SECONDS))
    } else {
      ESConnection.client.execute {
        search(index) query
          boolQuery.
            must(
              rangeQuery("publicationDate") gte min lte max,
              matchQuery("content", query_s)
            ) fetchSource false storedFields "_id" limit limit_i
      }.await(Duration(90, TimeUnit.SECONDS))
    }
    val ids = resp.ids.map(id => id.toLong)
    ids
  }

  /**
    * Search the Collection with a date range and a section name
    * @param query_s Section name
    * @param min lower bound of the date range
    * @param max upper bound of the date range
    * @param index name of the index
    * @param limit_i limit for the amount of returned results
    * @return Collection of IDs
    */
  def searchESDateRangeAndSection(query_s: String, min: String, max: String, index: String, limit_i: Int = 10000): Seq[Long] = {
    val resp = ESConnection.client.execute {
      search(index) query
        boolQuery.
          must(
            rangeQuery("publicationDate") gte min lte max,
            matchQuery("onlineSections", query_s)
          ) fetchSource false storedFields "_id" limit limit_i
    }.await(Duration(90, TimeUnit.SECONDS))
    val ids = resp.ids.map(id => id.toLong)
    ids
  }

  /**
    * Initialization of a combined index for multiple queries
    * @param index name of the index
    * @param limit_i limit of the returned results
    * @param queryFrom lower bound of the date range
    * @param queryTo upper bound of the date range
    * @param section section name
    * @param queries variadic parameter for keyword queries
    * @return combined index over all query results
    */
  def searchESWithDateRangeBackground(index: String, limit_i: Int, queryFrom: String, queryTo: String, section: Boolean, queries: String*) = {
    val map = mutable.Map[Long, mutable.BitSet]()
    val ids = searchESByDateRange(queryFrom, queryTo, index, 1800000)
    for (id <- ids) {
      val newBitSet = map.getOrElse(id, mutable.BitSet(1)) += 1
      map += (id -> newBitSet)
    }
    for (i <- 0 until queries.size) yield {
      val ids = if (section) {
        searchESDateRangeAndSection(queries.get(i), queryFrom, queryTo, index, limit_i)
      }
      else {
        searchESDateRangeAndTerm(queries.get(i), queryFrom, queryTo, index, limit_i)
      }
      for (id <- ids) {
        val newBitSet = map.getOrElse(id, mutable.BitSet(i + 2)) += i + 2
        newBitSet += 1
        map += (id -> newBitSet)
      }
    }
    map
  }


  /**
    * Search ES Index for a certain year
    *
    * @param year    Year to query for
    * @param index   Name of the index
    * @param limit_i Limit of the results that we return
    * @return
    */
  def searchESByYear(year: Int, index: String, limit_i: Int = 10000): Seq[Long] = {
    val resp = ESConnection.client.execute {
      search(index) termQuery("publicationYear", year) limit limit_i
    }.await(Duration(30, TimeUnit.SECONDS))
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
    for (i <- 0 to queries.size - 1) yield {
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
//  val nytEs = new NYTElasticSearchUtils
//  //  nytEs.createNYTIndex("test1")
//
//  nytEs.searchESWithDateRangeBackground("nyt2006", 10000, "2006/04/01", "2006/05/01", false, "Easter", "Easter")

  ESConnection.client.execute{
    updateSettings("thesis") set("max_result_window","1800000")
  }

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
