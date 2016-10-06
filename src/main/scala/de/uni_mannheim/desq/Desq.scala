package de.uni_mannheim.desq

import org.apache.spark.SparkConf

/**
  * Created by rgemulla on 05.10.2016.
  */
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
