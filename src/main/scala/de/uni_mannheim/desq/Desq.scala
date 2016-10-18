package de.uni_mannheim.desq

import org.apache.spark.SparkConf

/** Entry & utility methods for Desq. */
object Desq {
  def initDesq(conf: SparkConf) {
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val currentRegistrator = conf.get("spark.kryo.registrator", null)
    if (currentRegistrator == null) {
      conf.set("spark.kryo.registrator", "de.uni_mannheim.desq.io.spark.DesqKryoRegistrator")
    } else {
      conf.set("spark.kryo.registrator", s"$currentRegistrator,de.uni_mannheim.desq.io.spark.DesqKryoRegistrator")
    }
  }
}
