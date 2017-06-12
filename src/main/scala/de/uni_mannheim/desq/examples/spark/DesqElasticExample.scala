package de.uni_mannheim.desq.examples.spark

import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import de.uni_mannheim.desq.elastic.NYTElasticSearchUtils

/**
  * Created by ivo on 12.06.17.
  */
object DesqElasticExample {
  def querySimple(query_left: String, query_right: String, index: String)(implicit es: NYTElasticSearchUtils) {
    print("Querying Elastic... ")
    val queryTime = Stopwatch.createStarted
    val ids_query_l = es.searchES(query_left, index)
    val ids_query_r = es.searchES(query_right, index)
    println(s"overlap ${ids_query_l.intersect(ids_query_r).size}")
    queryTime.stop()
    println(queryTime.elapsed(TimeUnit.MILLISECONDS) + "ms")
  }

  def queryHashmap(index:String, limit:Int, query_l:String, query_r:String)(implicit es: NYTElasticSearchUtils): Unit ={
    val map = es.searchESCombines(index, limit, query_l, query_r)
    println("hello")
  }

def main(args: Array[String]) {
  implicit val es = new NYTElasticSearchUtils
  val index = "nyt2006/article"
  val limit = 10000
  val query_l = "\"Donald Trump\""
  val query_r = "\"Hillary Clinton\""
  queryHashmap(index, limit, query_l, query_r)
}
}
