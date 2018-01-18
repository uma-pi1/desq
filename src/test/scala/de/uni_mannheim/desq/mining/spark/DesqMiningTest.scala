package de.uni_mannheim.desq.mining.spark

import java.io.{File, FileOutputStream}

import de.uni_mannheim.desq.io.DelPatternWriter
import de.uni_mannheim.desq.mining.WeightedSequence
import de.uni_mannheim.desq.util.{DesqProperties, TestUtils}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.assertj.core.api.Assertions._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.scalatest.junit.AssertionsForJUnit

/**
  * Created by rgemulla on 20.09.2016.
  */
@RunWith(classOf[Parameterized])
abstract class DesqMiningTest(sigma: Long, patternExpression: String,
                              minerName: String, conf: DesqProperties) extends AssertionsForJUnit {
  /** The data */
  def getDataset()(implicit sc: SparkContext): GenericDesqDataset[WeightedSequence]

  def goldFileBaseName: String

  def testDirectoryName: String


  @Test
  def test() {
    val fileName = goldFileBaseName+ "-" + sigma + "-" + sanitize(patternExpression) + ".del"
    val actualFile = TestUtils.newTemporaryFile(
      TestUtils.getPackageResourcesPath(getClass) + "/" + testDirectoryName + "/" + minerName + "/" + fileName)
    mine(actualFile)
    try {
      val expectedFile = TestUtils.getPackageResource(classOf[de.uni_mannheim.desq.mining.DesqMiningTest], fileName) // use path of sequential results
      assertThat(actualFile).hasSameContentAs(expectedFile)
    } catch {
      case e: NullPointerException =>
        DesqMiningTest.logger.error("Can't access expected data file for " + actualFile)
        throw e
    }
  }

  def mine(outputDelFile: File) {
    implicit val sc = de.uni_mannheim.desq.util.spark.TestUtils.sc
    val data: GenericDesqDataset[WeightedSequence] = getDataset()

    // Perform pattern mining into del file
    val resultRDD = data.mine(conf)
    val result = resultRDD.sequences.collect()

    // write the data
    val patternWriter = new DelPatternWriter(new FileOutputStream(outputDelFile),
      DelPatternWriter.TYPE.GID)
    patternWriter.setDictionary(data.sequenceInterpreter.getDictionary)
    result.foreach(patternWriter.write)
    patternWriter.close()

    // sort del file
    TestUtils.sortDelPatternFile(outputDelFile)
  }

  def sanitize(s: String) = de.uni_mannheim.desq.mining.DesqMiningTest.sanitize(s)
}

object DesqMiningTest {
  private val logger = Logger.getLogger(classOf[DesqMiningTest])
}
