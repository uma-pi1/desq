package de.uni_mannheim.desq.mining.spark

import java.util.{ArrayList, List}

import de.uni_mannheim.desq.util.DesqProperties

/** Helper methods to creates configurations for (all) miners. Used for testing. */
object MinerConfigurations {
    def desqCount(sigma: Long, patternExpression: String, useFlist: Boolean, iterative: Boolean,
                  pruneIrrelevantInputs: Boolean, useTwoPass: Boolean): (String, DesqProperties) = {
        val conf = DesqCount.createConf(patternExpression, sigma)
        conf.setProperty("desq.mining.use.flist", useFlist)
        conf.setProperty("desq.mining.iterative", iterative)
        conf.setProperty("desq.mining.prune.irrelevant.inputs", pruneIrrelevantInputs)
        conf.setProperty("desq.mining.use.two.pass", useTwoPass)
        val minerName = "DesqCount-" + toLetter(useFlist) + toLetter(iterative) + toLetter(pruneIrrelevantInputs) +
                toLetter(useTwoPass)
        (minerName, conf)
    }

    def desqCount(sigma: Long, gamma: Int, lambda: Int, generalize: Boolean, useFlist: Boolean, iterative: Boolean,
                  pruneIrrelevantInputs: Boolean, useTwoPass: Boolean): (String, DesqProperties) = {
        val  patternExpression = DesqMiner.patternExpressionFor(gamma, lambda, generalize)
        desqCount(sigma, patternExpression, useFlist, iterative, pruneIrrelevantInputs, useTwoPass)
    }

    def all(sigma: Long, gamma: Int, lambda: Int, generalize: Boolean): List[(String, DesqProperties)] = {
        val allMiners = new ArrayList[(String, DesqProperties)]()
        val patternExpression = DesqMiner.patternExpressionFor(gamma, lambda, generalize)
        allMiners.addAll( all(sigma, patternExpression) )
        allMiners
    }

    def all(sigma: Long, patternExpression: String): List[(String, DesqProperties)] = {
        val allMiners = new ArrayList[(String, DesqProperties)]()
        allMiners.add(desqCount(sigma, patternExpression, false, false, false, false))
        allMiners.add(desqCount(sigma, patternExpression, false, false, true, false))
        allMiners.add(desqCount(sigma, patternExpression, false, false, true, true))
        allMiners.add(desqCount(sigma, patternExpression, false, true, false, false))
        allMiners.add(desqCount(sigma, patternExpression, false, true, true, false))
        allMiners.add(desqCount(sigma, patternExpression, false, true, true, true))
        allMiners.add(desqCount(sigma, patternExpression, true, false, false, false))
        allMiners.add(desqCount(sigma, patternExpression, true, false, true, false))
        allMiners.add(desqCount(sigma, patternExpression, true, false, true, true))
        allMiners.add(desqCount(sigma, patternExpression, true, true, false, false))
        allMiners.add(desqCount(sigma, patternExpression, true, true, true, false))
        allMiners.add(desqCount(sigma, patternExpression, true, true, true, true))
        allMiners
    }

    def toLetter(b: Boolean): String = {
        if (b) "t" else "f"
    }
}

