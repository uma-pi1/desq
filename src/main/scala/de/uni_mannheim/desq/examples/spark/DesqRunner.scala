package de.uni_mannheim.desq.examples.spark

import java.util.concurrent.TimeUnit
import com.google.common.base.Stopwatch
import de.uni_mannheim.desq.Desq
import de.uni_mannheim.desq.dictionary.Dictionary
import de.uni_mannheim.desq.mining.spark.{DDIN, DesqCount}
import de.uni_mannheim.desq.mining.spark.{DesqDataset, DesqMiner, DesqMinerContext}
import org.apache.spark.{SparkConf, SparkContext}
import de.uni_mannheim.desq.patex.PatExUtils
import org.apache.spark.mllib.fpm.PrefixSpan


/**
  * Class to conventiently run D-SEQ, D-CAND, and other algorithms on various pattern expressions
  * Usage:
  * input=hdfs://path-to-input-DesqDataset-on-hdfs-or-local/  output=hdfs://path-to-folder-for-found-frequent-sequences/  expression=A1  algorithm=DDIN
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
    var sendNFAs: Boolean = _
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

        // by dfeault, do not do any manual repartitioning before the mining
        runConf.put("map.repartition", "0")

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
        if (runConf.contains("expression"))
            appName = runConf.get("expression").get + "@" + runConf.get("algorithm").get
        if (runConf.contains("master"))
            sparkConf = new SparkConf().setAppName(appName).setMaster(runConf.get("master").get)
        else
            sparkConf = new SparkConf().setAppName(appName)
        Desq.initDesq(sparkConf)
        sc = new SparkContext(sparkConf)

        // how to encode a dataset
    /*  val dict = Dictionary.loadFrom("data/icde-example/dictionary.json")
        val textSequences = sc.textFile("data/icde-example/sequences.del")
        textSequences.map(_.split(" ").map(dict.gidOf).mkString(" ")).saveAsTextFile("data/icde-example/sequences.gids.del")
        val dds = DesqDataset.loadFromDelFile("data/icde-example/sequences.gids.del", dict)
        dds.toFids().save("data/icde-example/DesqDataset/") */


        // start application
        if (args.length > 0) { // use command line settings
            if(runConf.get("algorithm").get == "PrefixSpan")
                runMllib()
            else
                runDesq()
        } else { // use default settings for local running: run the thesis example in all algorithms
            runConf.put("count.patterns", "true")
            runConf.put("input", "data/icde-example/DesqDataset/")
            runGrid(Array("ICDE"), Array("Naive", "SemiNaive", "D-SEQ", "D-CAND"))
        }
    }

    /** Runs each combination expressions x algorithms and reports the results */
    def runGrid(expressions:Array[String], algorithms:Array[String]) {
        var output = ""
        for (exp <- expressions) {
            for (algorithm <- algorithms) {
                runConf.put("expression", exp)
                runConf.put("algorithm", algorithm)
                val res = runDesq()
                output += "Pattern expression '" + exp + "' mined with " + algorithm + ": found " + res._1 + " frequent sequences with a total frequency of " + res._2 + "\n"
            }
            output += "\n"
        }

        System.out.println("------------------------------------------------------------------")
        System.out.println("------------------------------------------------------------------")
        System.out.println(output)
    }


    /** Runs distributed DESQ algorithms for parameters specified in runConf */
    def runDesq(): (Long, Long) = {

        setPatternExpression(runConf.get("expression").get)
        setAlgorithmVariant(runConf.get("algorithm").get)


        System.out.println("------------------------------------------------------------------")
        System.out.println("Mining " + runConf.get("expression").get + " with " + runConf.get("algorithm").get)
        System.out.println("------------------------------------------------------------------")

        println(sparkConf.toDebugString)

        println("Loading dataset from " + runConf.get("input").get)
        val data = DesqDataset.load(runConf.get("input").get)

        val sw = new Stopwatch().start()
        // Build miner conf
        patternExp = PatExUtils.toFidPatEx(data.dict, patternExp)
        // translate pattern expression to fids
        var minerConf = DDIN.createConf(patternExp, sigma)
        if (useDesqCount) {
            minerConf = DesqCount.createConf(patternExp, sigma)
        }
        minerConf.setProperty("desq.mining.use.flist", useFlist)
        minerConf.setProperty("desq.mining.prune.irrelevant.inputs", "false")
        minerConf.setProperty("desq.mining.use.two.pass", "true")
        minerConf.setProperty("desq.mining.send.nfas", sendNFAs)
        minerConf.setProperty("desq.mining.merge.suffixes", mergeSuffixes)
        minerConf.setProperty("desq.mining.aggregate.shuffle.sequences", aggregateShuffleSequences)
        minerConf.setProperty("desq.mining.map.repartition", runConf.get("map.repartition").get)
        minerConf.setProperty("desq.mining.trim.input.sequences", trimInputSequences)
        minerConf.setProperty("desq.mining.use.hybrid", useHybrid)
        minerConf.setProperty("desq.mining.use.grid", useGrid)
        minerConf.setProperty("desq.mining.send.to.all.frequent.items", sendToAllFrequentItems)
        minerConf.setProperty("desq.mining.stop.at.last.pivot.pos", stopAtLastPivotPos)

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

            // print the found frequent sequences
            // result.sequences.collect().foreach(println)

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

    /** Runs MLLIB's PrefixSpan implementation */
    def runMllib(): (Long, Long) = {

        var sigma = 0
        var maxLength = 0

        // parse the given case
        runConf.get("expression").get match {
            case r"S\((\d+)$o,(\d+)$m\)" => {
                maxLength = m.toInt
                sigma = o.toInt // absolute value. for MLLIB's PrefixSpan, we need to convert this to a fraction
            }
        }

        System.out.println("------------------------------------------------------------------")
        System.out.println("Mining " + runConf.get("expression").get + " with " + runConf.get("algorithm").get)
        System.out.println("------------------------------------------------------------------")

        println(sparkConf.toDebugString)

        var inputPath = runConf.get("input").get
        if(inputPath.last == '/')
            inputPath = inputPath.substring(0,inputPath.length-1)

        println("Loading dataset from " + runConf.get("input").get)
        val desqData = DesqDataset.load(inputPath)
        val data = desqData.sequences.map(_.toArray.map(item => Array(item)))
        val numSeqs = data.count()

        // MLLIB PrefixSpan takes sinSupport as fraction of the number of input sequences
        var minSupport = 1.0 * sigma / numSeqs
        // MLLIB ceil's the product minSupport*numSeqs, so let's prevent sigmas that are ceiled upwards due to rounding
        while(Math.ceil(minSupport * numSeqs) > sigma) {
            minSupport = 0.9999999999999999 * minSupport
        }
        println("sigma=" + sigma + ", numSeqs=" + numSeqs + " -> minSup=" + minSupport + ". maxLength=" + maxLength)

        // cache the sequences as soon as we read them the first time
        data.cache()

        // mine
        println("-------------------")
        println("mining...")
        println("-------------------")
        val sw = new Stopwatch().start()
        val prefixSpan = new PrefixSpan().setMinSupport(minSupport).setMaxPatternLength(maxLength)
        val model = prefixSpan.run(data)

        // store the frequent sequences
        model.freqSequences.map(fs => "[" + fs.sequence.map(_(0)).mkString(" ") + "]@" + fs.freq).saveAsTextFile(runConf.get("output").get)

        // count, freq output
        if(runConf.contains("count.patterns")) {
            model.freqSequences.first().freq
            val count = model.freqSequences.cache().count()
            val freq = model.freqSequences.map(_.freq).reduce(_ + _)
            println("-------------------")
            println("count, freq")
            println("-------------------")
            println("(" + count + ", " + freq + ")")
            println("Took " + sw.stop().elapsed(TimeUnit.MILLISECONDS))

            model.freqSequences.unpersist()
            return (count, freq)
        }

        data.unpersist()

        (0,0)
    }


    // -- pattern expressions ----------------------------------------------------------------------------------------

    def setPatternExpression(expression: String) {
        verbose = false
        expression match {
            case "ICDE" => {
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
            case r"T2\((\d+)$o,(\d+)$g,(\d+)$l\)" => {
                patternExp = "(.)[.{0," + g.toInt + "}(.)]{1," + (l.toInt - 1) + "}"
                sigma = o.toInt
            }
            // traditional constraints: with hierarchy, max length, max gap (e.g. LASH): L(minSupport,maxGap,maxLength)
            case r"T3\((\d+)$o,(\d+)$g,(\d+)$l\)" => {
                patternExp = "(.^)[.{0," + g.toInt + "}(.^)]{1," + (l.toInt - 1) + "}"
                sigma = o.toInt
            }
            // traditional constraints: no hierarchy, max length  (e.g. PrefixSpan): S(minSupport,maxLength)
            case r"T1\((\d+)$o,(\d+)$m\)" => {
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
                System.out.println("Do not know the pattern expression " + expression)
                System.exit(1)
            }
        }
    }

    /** Baseline algorithms and algorithm variants */
    def setAlgorithmVariant(algorithm: String) {
        //set some defaults
        sendNFAs = false
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
            case "D-SEQ.noGrid.noTrim" =>
                aggregateShuffleSequences = false
            case "D-SEQ.noGrid.noTrim.noStop" =>
                aggregateShuffleSequences = false
                stopAtLastPivotPos = false
            case "D-SEQ.noGrid" =>
                aggregateShuffleSequences = false
                trimInputSequences = true
            case "D-SEQ.noGrid.noStop" =>
                aggregateShuffleSequences = false
                trimInputSequences = true
                stopAtLastPivotPos = false
            case "D-SEQ.noTrim.noStop" =>
                aggregateShuffleSequences = false
                useGrid = true
                stopAtLastPivotPos = false
            case "D-SEQ.noTrim" =>
                aggregateShuffleSequences = false
                useGrid = true
            case "D-SEQ.noStop" =>
                aggregateShuffleSequences = false
                useGrid = true
                trimInputSequences = true
                stopAtLastPivotPos = false
            case "D-SEQ" =>
                aggregateShuffleSequences = false
                useGrid = true
                trimInputSequences = true
            case "D-SEQ.sendAllFreq" =>
                sendToAllFrequentItems = true
                aggregateShuffleSequences = false
            case "D-CAND" =>
                sendNFAs = true
                mergeSuffixes = true
                aggregateShuffleSequences = true
            case "D-CAND.constructWithGrid" =>
                sendNFAs = true
                mergeSuffixes = true
                aggregateShuffleSequences = true
                useGrid = true
            case "D-CAND.tries.noAgg" =>
                sendNFAs = true
                mergeSuffixes = false
                aggregateShuffleSequences = false
            case "D-CAND.tries" =>
                sendNFAs = true
                mergeSuffixes = false
                aggregateShuffleSequences = true
            case "D-CAND.noAgg" =>
                sendNFAs = true
                mergeSuffixes = true
                aggregateShuffleSequences = false
            case "DesqHybrid" =>
                sendNFAs = true
                mergeSuffixes = true
                aggregateShuffleSequences = true
                useHybrid = true
            case _ =>
                System.out.println("Unknown algorithm variant: " + algorithm)
                System.exit(0)
        }
    }

    implicit class Regex(sc: StringContext) {
        def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
    }

}
