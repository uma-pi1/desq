package de.uni_mannheim.desq.mining.spark

import java.util
import java.util.{Collections, Comparator}

import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.io.DelSequenceReader
import de.uni_mannheim.desq.mining.WeightedSequence
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
class Icdm16TraditionalMiningTest(sigma: Long, gamma: Int, lambda: Int, generalize: Boolean,
                                  minerName: String, conf: DesqProperties)
  extends TraditionalMiningTest(sigma, gamma, lambda, generalize, minerName, conf) {

    override def goldFileBaseName = "icdm16/icdm16-traditional-patterns-ids"

    override def testDirectoryName = getClass.getSimpleName

    /** The data */
    override def getDesqDataset()(implicit sc: SparkContext): DesqDataset = Icdm16TraditionalMiningTest.getDesqDataset()
    override def getGenericDesqDataset()(implicit sc: SparkContext): GenericDesqDataset[(Array[String], Long)] = Icdm16TraditionalMiningTest.getGenericDesqDataset()
}

object Icdm16TraditionalMiningTest {
    var desqDataset: DesqDataset = _
    var genericDesqDataset: GenericDesqDataset[(Array[String], Long)] = _

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
    def getDesqDataset()(implicit sc: SparkContext): DesqDataset = {
      if (desqDataset == null) {
        val dictFile = this.getClass.getResource("/icdm16-example/dict.json")
        val dataFile = this.getClass.getResource("/icdm16-example/data.del")

        // load the dictionary & update hierarchy
        val dict = Dictionary.loadFrom(dictFile)
        val delFile = sc.parallelize(Source.fromURL(dataFile).getLines.toSeq)
        desqDataset = DesqDataset.loadFromDelFile(delFile, dict, usesFids = false).recomputeDictionary()
        desqDataset.sequences.cache()
      }
      desqDataset
    }

    /** The data */
    def getGenericDesqDataset()(implicit sc: SparkContext): GenericDesqDataset[(Array[String], Long)] = {
      if (genericDesqDataset == null) {
        val desqDataset = getDesqDataset()
        val sequences = desqDataset.sequences.collect().map(ws => {
          (desqDataset.descriptor.getSids(ws), desqDataset.descriptor.getWeight(ws))
        })

        val descriptor = new StringArrayAndLongDescriptor()
        descriptor.setDictionary(desqDataset.descriptor.getDictionary)

        genericDesqDataset = new GenericDesqDataset[(Array[String], Long)](sc.parallelize(sequences), descriptor)
        genericDesqDataset.sequences.cache()
      }
      genericDesqDataset
    }

}