package de.uni_mannheim.desq.mining.spark

import java.lang.Long
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

    override def getGoldFileBaseName() = "icdm16/icdm16-traditional-patterns-ids"

    override def getTestDirectoryName() = getClass().getSimpleName()

    /** The data */
    override def getDataset()(implicit sc: SparkContext): DesqDataset = Icdm16TraditionalMiningTest.getDataset()
}

object Icdm16TraditionalMiningTest {
    var dataset: DesqDataset = _

    @Parameterized.Parameters(name = "Icdm16TraditionalMiningTest-{4}-{0}-{1}-{2}-{3}-")
    def data(): util.Collection[Array[Object]] = {
        val parameters = new util.ArrayList[Array[Object]]()
        for (sigma <- Array(1L,3L,5L,7L))
           for (gamma <- Array(0,1,2))
                for (lambda <- Array(1,3,5,7))
                    for (generalize <- Array(false, true))
                        for (miner <- MinerConfigurations.all(sigma, gamma, lambda, generalize)) {
                            parameters.add(Array[Object](new Long(sigma), new Integer(gamma), new Integer(lambda), new java.lang.Boolean(generalize), miner._1, miner._2))
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
            dataset = DesqDataset.fromDelFile(delFile, dict, false).copyWithRecomputedCountsAndFids
        }
        dataset
    }
}