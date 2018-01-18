package de.uni_mannheim.desq.mining.spark

import java.util
import java.util.{Collections, Comparator}

import de.uni_mannheim.desq.mining.WeightedSequence
import de.uni_mannheim.desq.util.DesqProperties
import org.apache.spark.SparkContext
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConversions._

/**
  * Created by rgemulla on 20.09.2016.
  */

@RunWith(classOf[Parameterized])
class Icdm16DesqMiningTest(sigma: Long, patternExpression: String, minerName: String, conf: DesqProperties)
  extends DesqMiningTest(sigma, patternExpression, minerName, conf) {
  /** The data */
  override def getDataset()(implicit sc: SparkContext): GenericDesqDataset[WeightedSequence] = Icdm16TraditionalMiningTest.getDataset()

  override def goldFileBaseName: String = "icdm16/icdm16-desq-patterns-ids"

  override def testDirectoryName: String = getClass.getSimpleName
}

object Icdm16DesqMiningTest {
  @Parameterized.Parameters(name = "Icdm16DesqMiningTest-{2}-{0}-{1}")
  def data(): util.Collection[Array[Object]] = {
    val parameters = new util.ArrayList[Array[Object]]()
    val baseData = de.uni_mannheim.desq.mining.Icdm16DesqMiningTest.baseData()
    for (par <- collectionAsScalaIterable(baseData))
      for (miner <- MinerConfigurations.all(par(0).asInstanceOf[java.lang.Long], par(1).asInstanceOf[String]))
        parameters.add(Array[Object](par(0), par(1), miner._1, miner._2))

    Collections.sort(parameters, new Comparator[Array[Object]] {
      override def compare(o1: Array[Object], o2: Array[Object]): Int = o1(2).asInstanceOf[String].compareTo(o2(2).asInstanceOf[String])
    })
    parameters
  }

}