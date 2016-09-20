package de.uni_mannheim.desq.mining.spark

import java.io.{File, FileOutputStream}

import de.uni_mannheim.desq.io.DelPatternWriter
import de.uni_mannheim.desq.util.{DesqProperties, TestUtils}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.scalatest.junit.AssertionsForJUnit
import org.assertj.core.api.Assertions.assertThat


/** A test for traditional frequent sequence mining. Datasets and parameters are set by implementing classes.
 *
 * Created by rgemulla on 18.07.2016.
 */
@RunWith(classOf[Parameterized])
abstract class TraditionalMiningTest(_sigma: Long, _gamma: Int, _lambda: Int, _generalize: Boolean,
                                     _minerName: String, _conf: DesqProperties) extends AssertionsForJUnit {
    val sigma = _sigma
    val gamma = _gamma
    val lambda = _lambda
    val generalize = _generalize
    val minerName = _minerName
    val conf = _conf

    /** The data */
    def getDataset()(implicit sc: SparkContext): DesqDataset

    def goldFileBaseName: String

    def testDirectoryName: String

    @Test
    def test() {
        val fileName = goldFileBaseName + "-" + sigma + "-" + gamma + "-" + lambda + "-" + generalize + ".del"
        val actualFile = TestUtils.newTemporaryFile(
                TestUtils.getPackageResourcesPath(getClass) + "/" + testDirectoryName + "/" + minerName + "/" + fileName)
        mine(actualFile)
        try {
            val expectedFile = TestUtils.getPackageResource(classOf[de.uni_mannheim.desq.mining.TraditionalMiningTest], fileName) // use path of sequential results
            assertThat(actualFile).hasSameContentAs(expectedFile)
        } catch {
            case e: NullPointerException =>
                TraditionalMiningTest.logger.error("Can't access expected data file for " + actualFile)
                throw e
        }
    }

    def mine(outputDelFile: File) {
        implicit val sc = de.uni_mannheim.desq.util.spark.TestUtils.sc
        var data: DesqDataset = getDataset()

        // Perform pattern mining into del file
        val resultRDD = data.mine(conf)
        val result = resultRDD.sequences.collect()

        // write the data
        val patternWriter = new DelPatternWriter(new FileOutputStream(outputDelFile),
            DelPatternWriter.TYPE.GID)
        patternWriter.setDictionary(data.dict)
        result.foreach(patternWriter.write)
        patternWriter.close()

        // sort del file
        TestUtils.sortDelPatternFile(outputDelFile)
    }
}

object TraditionalMiningTest {
    private val logger = Logger.getLogger(classOf[TraditionalMiningTest])
}
