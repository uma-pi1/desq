package de.uni_mannheim.desq.mining.spark

import java.util
import java.util.{Collections, Comparator}

import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.util.DesqProperties
import org.apache.spark.SparkContext
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConversions._
import scala.io.Source

/**
 * Created by rgemulla on 18.07.2016.
 */
@RunWith(classOf[Parameterized])
class Icdm16TraditionalMiningTest(_sigma: Long, _gamma: Int, _lambda: Int, _generalize: Boolean,
                                  _minerName: String, _conf: DesqProperties)
  extends TraditionalMiningTest(_sigma, _gamma, _lambda, _generalize, _minerName, _conf) {

    override def goldFileBaseName = "icdm16/icdm16-traditional-patterns-ids"

    override def testDirectoryName = getClass.getSimpleName

    /** The data */
    override def getDataset()(implicit sc: SparkContext): DesqDataset = Icdm16TraditionalMiningTest.getDataset()
}

object Icdm16TraditionalMiningTest {
    var dataset: DesqDataset = _

    @Parameterized.Parameters(name = "Icdm16TraditionalMiningTest-{4}-{0}-{1}-{2}-{3}-")
    def data(): util.Collection[Array[Object]] = {
      val parameters = new util.ArrayList[Array[Object]]()
      val baseData = de.uni_mannheim.desq.mining.Icdm16TraditionalMiningTest.baseData()
      for (par <- collectionAsScalaIterable(baseData)) {
        for (miner <- MinerConfigurations.all(par(0).asInstanceOf[java.lang.Long],
          par(1).asInstanceOf[java.lang.Integer], par(2).asInstanceOf[java.lang.Integer],
          par(3).asInstanceOf[java.lang.Boolean])) {
            parameters.add(Array[Object](par(0), par(1), par(2), par(3), miner._1, miner._2))
        }
      }

      Collections.sort(parameters, new Comparator[Array[Object]] {
        override def compare(o1: Array[Object], o2: Array[Object]): Int = {
          o1(4).asInstanceOf[String].compareTo(o2(4).asInstanceOf[String])
        }
      })
      parameters
    }

    /** The data */
    def getDataset()(implicit sc: SparkContext): DesqDataset = {
        if (dataset == null) {
            val dictFile = this.getClass.getResource("/icdm16-example/dict.json")
            val dataFile = this.getClass.getResource("/icdm16-example/data.del")

            // load the dictionary & update hierarchy
            val dict = Dictionary.loadFrom(dictFile)
            val delFile = sc.parallelize(Source.fromURL(dataFile).getLines.toSeq)
            dataset = DesqDataset.fromDelFile(delFile, dict, usesFids = false).copyWithRecomputedCountsAndFids()
            dataset.sequences.cache()
        }
        dataset
    }
}