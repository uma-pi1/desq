package de.uni_mannheim.desq.experiments.spark

import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import de.uni_mannheim.desq.Desq
import de.uni_mannheim.desq.mining.spark._
import de.uni_mannheim.desq.patex.PatExUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Class to conventiently run DesqSeq, DesqCand, and other algorithms on various pattern expressions
  * Usage:
  * input=hdfs://path-to-input-DesqDataset-on-hdfs-or-local/  output=hdfs://path-to-folder-for-found-frequent-sequences/  case=A1  algorithm=DesqSeq
  *
  * More information can be found in the README.md
  *
  * Created by alexrenz on 08.10.2016.
  */
object DesqRunner {
    var sparkConf: SparkConf = _
    implicit var sc: SparkContext = _

    var sigma: Long = _
    var patternExp: String = _
    var verbose: Boolean = _

    // Switches
    var sendNfas: Boolean = _
    var mergeSuffixes: Boolean = _
    var useDesqCount: Boolean = _
    var aggregateShuffleSequences: Boolean = _
    var trimInputSequences: Boolean = _
    var useHybrid: Boolean = _
    var useGrid: Boolean = _
    var sendToAllFrequentItems: Boolean = _
    var useFlist: Boolean = _
    var stopAtLastPivotPos: Boolean = _

    val runConf = scala.collection.mutable.Map[String, String]()


    def main(args: Array[String]) {
        // run on one local core if we run without command line arguments
        if (args.length == 0) {
            runConf.put("master", "local[1]")
        }

        // parse commandline arguments
        if (args.length > 0) {
            for (arg <- args) {
                val splits = arg.split("=")
                runConf.put(splits(0), {
                    if (splits.length > 1) splits(1) else ""
                })
            }
        }
        println(runConf)

        // set up SparkContext
        var appName = getClass.getName
        if (runConf.contains("case"))
            appName = runConf.get("case").get + "@" + runConf.get("algorithm").get
        if (runConf.contains("master"))
            sparkConf = new SparkConf().setAppName(appName).setMaster(runConf.get("master").get)
        else
            sparkConf = new SparkConf().setAppName(appName)
        Desq.initDesq(sparkConf)
        sc = new SparkContext(sparkConf)

        // start application
        if (args.length > 0) { // use command line settings
            runDesq()
        } else { // use default settings for local running: run the thesis example in all algorithms
            runConf.put("count.patterns", "true")
            runConf.put("input", "data/vldb-example/DesqDataset/")
            runGrid(Array("VLDB"), Array("Naive", "SemiNaive", "DesqSeq", "DesqCand"))
        }
    }

    /** Runs each combination cases x algorithms and reports the results */
    def runGrid(cases:Array[String], algorithms:Array[String]) {
        var output = ""
        for (theCase <- cases) {
            for (algorithm <- algorithms) {
                runConf.put("case", theCase)
                runConf.put("algorithm", algorithm)
                val res = runDesq()
                output += "Pattern expression '" + theCase + "' mined with " + algorithm + ": found " + res._1 + " frequent sequences with a total frequency of " + res._2 + "\n"
            }
            output += "\n"
        }

        println("------------------------------------------------------------------")
        println("------------------------------------------------------------------")
        println(output)
    }


    /** Runs distributed DESQ algorithms for parameters specified in runConf */
    def runDesq(): (Long, Long) = {

        setPatternExpression(runConf.get("case").get)
        setAlgorithmVariant(runConf.get("algorithm").get)

        println("------------------------------------------------------------------")
        println("Mining " + runConf.get("case").get + " with " + runConf.get("algorithm").get)
        println("------------------------------------------------------------------")

        println(sparkConf.toDebugString)

        println("Loading dataset from " + runConf.get("input").get)
        val data = DesqDataset.load(runConf.get("input").get)

        val sw = Stopwatch.createStarted()
        // Build miner conf
        patternExp = PatExUtils.toFidPatEx(data.descriptor.getDictionary, patternExp)
        // translate pattern expression to fids
        var minerConf = DesqDistributed.createConf(patternExp, sigma)
        if (useDesqCount) {
            minerConf = DesqCount.createConf(patternExp, sigma)
        }
        minerConf.setProperty("desq.mining.use.flist", useFlist)
        minerConf.setProperty("desq.mining.prune.irrelevant.inputs", "false")
        minerConf.setProperty("desq.mining.use.two.pass", "true")
        minerConf.setProperty("desq.mining.send.nfas", sendNfas)
        minerConf.setProperty("desq.mining.merge.suffixes", mergeSuffixes)
        minerConf.setProperty("desq.mining.aggregate.shuffle.sequences", aggregateShuffleSequences)
        minerConf.setProperty("desq.mining.trim.input.sequences", trimInputSequences)
        minerConf.setProperty("desq.mining.use.hybrid", useHybrid)
        minerConf.setProperty("desq.mining.use.grid", useGrid)
        minerConf.setProperty("desq.mining.send.to.all.frequent.items", sendToAllFrequentItems)

        // Construct miner
        val ctx = new DesqMinerContext(minerConf)
        println("Miner properties: ")
        ctx.conf.prettyPrint()
        val miner = DesqMiner.create(ctx)

        // Mine
        val result = miner.mine(data)

        if (runConf.contains("count.patterns")) {
            // if count.patterns is set to true, we only count patterns, with no output
            val (count, freq) = result.sequences.map(ws => (1, ws.weight)).fold((0, 0L))((a, b) => (a._1 + b._1, a._2 + b._2))
            println("-------------------")
            println("count, freq")
            println("-------------------")
            println("(" + count + ", " + freq + ")")
            println("Took " + sw.stop().elapsed(TimeUnit.MILLISECONDS))
            (count, freq)
        } else {
            // otherwise, we store the patterns
            val outputFolder = runConf.get("output").get
            result.sequences.saveAsTextFile(outputFolder);
            (0, 0)
        }
    }

    // -- pattern expressions ----------------------------------------------------------------------------------------

    def setPatternExpression(useCase: String) {
        verbose = false
        useCase match {
            case "VLDB" => {
                patternExp = "(A)[(.^)|.*]*(b)"
                sigma = 2
                verbose = true
            }
            case "Thesis" => {
                patternExp = "A([c|d][A^|B^]+e)"
                sigma = 2
                verbose = true
            }
            // -- New York Times Corpus ---------------------------------------------
            case "N0" => {
                patternExp = "flourisher@NN flourisher@NN"
                sigma = 10
            }
            case "N1" => {
                patternExp = "ENTITY (VB+ NN+? IN?) ENTITY"
                sigma = 10
            }
            case r"N1\((\d+)$o\)" => {
                patternExp = "ENTITY (VB+ NN+? IN?) ENTITY"
                sigma = o.toInt
            }
            case "N2" => {
                patternExp = "(ENTITY^ VB+ NN+? IN? ENTITY^)"
                sigma = 100
            }
            case r"N2\((\d+)$o\)" => {
                patternExp = "(ENTITY^ VB+ NN+? IN? ENTITY^)"
                sigma = o.toInt
            }
            case "N3" => {
                patternExp = "(ENTITY^ be@VB=^) DT? (RB? JJ? NN)"
                sigma = 10
            }
            case r"N3\((\d+)$o\)" => {
                patternExp = "(ENTITY^ be@VB=^) DT? (RB? JJ? NN)"
                sigma = o.toInt
            }
            case "N4" => {
                patternExp = "(.^){3} NN"
                sigma = 1000
            }
            case r"N4\((\d+)$o\)" => {
                patternExp = "(.^){3} NN"
                sigma = o.toInt
            }
            case "N5" => {
                patternExp = "([.^ . .]|[. .^ .]|[. . .^])"
                sigma = 1000
            }
            case r"N5\((\d+)$o\)" => {
                patternExp = "([.^ . .]|[. .^ .]|[. . .^])"
                sigma = o.toInt
            }
            // -- Amazon Reviews ---------------------------------------------
            case "A0" => {
                patternExp = "B000BM3MMK B000BM3MMK"
                sigma = 500
            }
            case "A1" => {
                patternExp = "(Electronics@^)[.{0,2}(Electronics@^)]{1,4}"
                sigma = 500
            }
            case "A2" => {
                patternExp = "(Books@)[.{0,2}(Books@)]{1,4}"
                sigma = 100
            }
            case "A3" => {
                patternExp = "\"Digital Cameras@Camera & Photo@Electronics@\"[.{0,3}(.^)]{1,4}"
                sigma = 100
            }
            case "A4" => {
                patternExp = "(\"Musical Instruments@\"^)[.{0,2}(\"Musical Instruments@\"^)]{1,4}"
                sigma = 100
            }
            // -- Traditional frequent sequence mining constraints ---------------------------------------------
            // traditional constraints: no hierarchy, max length, max gap (e.g. MG-FSM): M(minSupport,maxGap,maxLength)
            case r"M\((\d+)$o,(\d+)$g,(\d+)$l\)" => {
                patternExp = "(.)[.{0," + g.toInt + "}(.)]{1," + (l.toInt - 1) + "}"
                sigma = o.toInt
            }
            // traditional constraints: with hierarchy, max length, max gap (e.g. LASH): L(minSupport,maxGap,maxLength)
            case r"L\((\d+)$o,(\d+)$g,(\d+)$l\)" => {
                patternExp = "(.^)[.{0," + g.toInt + "}(.^)]{1," + (l.toInt - 1) + "}"
                sigma = o.toInt
            }
            // traditional constraints: no hierarchy, max length  (e.g. PrefixSpan): S(minSupport,maxLength)
            case r"S\((\d+)$o,(\d+)$m\)" => {
                patternExp = "(.)[.*(.)]{," + (m.toInt - 1)+ "}"
                sigma = o.toInt
            }
            // -- Toy dataset examples ---------------------------------------------
            case "I1@1" => {
                patternExp = "[c|d]([A^|B=^]+)e"
                sigma = 1
                verbose = true
            }
            case "I1@2" => {
                patternExp = "[c|d]([A^|B=^]+)e"
                sigma = 2
                verbose = true
            }
            case "I2" => {
                patternExp = "([.^ . .])"
                sigma = 1
                verbose = true
            }
            case "IA2" => {
                patternExp = "(A)[.{0,2}(A)]{1,4}"
                sigma = 1
                verbose = true
            }
            case "IA4" => {
                patternExp = "(A^)[.{0,2}(A^)]{1,4}"
                sigma = 1
                verbose = true
            }
            case "IX1" => {
                patternExp = "[c|d](a2).*([A^|B=^]).*(e)"
                sigma = 1
                verbose = true
            }
            case "IX2" => {
                patternExp = "[c|d](a2).*([A^|B=^]).*(B^e)"
                sigma = 1
                verbose = true
            }
            case "IX3" => {
                patternExp = "(a1* b12 e)"
                sigma = 1
                verbose = true
            }
            case "IX4" => {
                patternExp = "([c|a1] .* [.* A]+ .* [d|e])"
                sigma = 1
                verbose = true
            }
            case _ => {
                System.out.println("Do not know the pattern expression " + useCase)
                System.exit(1)
            }
        }
    }

    /** Baseline algorithms and algorithm variants */
    def setAlgorithmVariant(algorithm: String) {
        //set some defaults
        sendNfas = false
        mergeSuffixes = false
        useDesqCount = false
        aggregateShuffleSequences = false
        trimInputSequences = false
        useHybrid = false
        useGrid = false
        sendToAllFrequentItems = false
        useFlist = true
        stopAtLastPivotPos = true
        algorithm match {
            case "Naive" =>
                useDesqCount = true
                useFlist = false
            case "SemiNaive" =>
                useDesqCount = true
            case "DesqSeq.noGrid.noTrim" =>
                aggregateShuffleSequences = false
            case "DesqSeq.noGrid.noTrim.noStop" =>
                aggregateShuffleSequences = false
                stopAtLastPivotPos = false
            case "DesqSeq.noGrid" =>
                aggregateShuffleSequences = false
                trimInputSequences = true
            case "DesqSeq.noGrid.noStop" =>
                aggregateShuffleSequences = false
                trimInputSequences = true
                stopAtLastPivotPos = false
            case "DesqSeq.noTrim.noStop" =>
                aggregateShuffleSequences = false
                useGrid = true
                stopAtLastPivotPos = false
            case "DesqSeq.noTrim" =>
                aggregateShuffleSequences = false
                useGrid = true
            case "DesqSeq.noStop" =>
                aggregateShuffleSequences = false
                useGrid = true
                trimInputSequences = true
                stopAtLastPivotPos = false
            case "DesqSeq" =>
                aggregateShuffleSequences = false
                useGrid = true
                trimInputSequences = true
            case "DesqSeq.sendAllFreq" =>
                sendToAllFrequentItems = true
                aggregateShuffleSequences = false
            case "DesqCand" =>
                sendNfas = true
                mergeSuffixes = true
                aggregateShuffleSequences = true
            case "DesqCand.constructWithGrid" =>
                sendNfas = true
                mergeSuffixes = true
                aggregateShuffleSequences = true
                useGrid = true
            case "DesqCand.tries.noAgg" =>
                sendNfas = true
                mergeSuffixes = false
                aggregateShuffleSequences = false
            case "DesqCand.tries" =>
                sendNfas = true
                mergeSuffixes = false
                aggregateShuffleSequences = true
            case "DesqCand.noAgg" =>
                sendNfas = true
                mergeSuffixes = true
                aggregateShuffleSequences = false
            case "DesqHybrid" =>
                sendNfas = true
                mergeSuffixes = true
                aggregateShuffleSequences = true
                useHybrid = true
            case _ =>
                System.out.println("Unknown algorithm variant")
                System.exit(0)
        }
    }

    implicit class Regex(sc: StringContext) {
        def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
    }

}
