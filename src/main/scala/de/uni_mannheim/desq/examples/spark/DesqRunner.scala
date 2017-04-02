package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.Desq
import de.uni_mannheim.desq.mining.spark.{DesqCount, DesqDfs}
import de.uni_mannheim.desq.mining.spark.{DesqDataset, DesqMiner, DesqMinerContext}
import org.apache.spark.{SparkConf, SparkContext}
import java.io.File
import org.apache.log4j.{LogManager, Logger}
import de.uni_mannheim.desq.patex.PatExUtils


/**
  * Conveniently running distributed DESQ
  *
  * Created by alexrenz on 08.10.2016.
  */
object DesqRunner {
    var sparkConf: SparkConf = _
    implicit var sc: SparkContext = _

    var sigma: Long = _
    var patternExp: String = _
    var dataFile: File = _
    var runVersion: String = _
    var verbose: Boolean = _
    var scenarioStr: String = _
    var useCase: String = _
    var scenario: Int = _
    var baseFolder: String = _
    var dataDir: String = _

    // Switches
    var sendNFAs: Boolean = _
    var mergeSuffixes: Boolean = _
    var useDesqCount: Boolean = _
    var aggregateShuffleSequences: Boolean = _

    val runConf = scala.collection.mutable.Map[String, String]()


    def main(args: Array[String]) {

        // defaults for local running
        if (args.length == 0) {
            runConf.put("master", "local[1]")
        }

        // defaults to 0: no repartitioning
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

        // set up spark context
        var appName = getClass.getName
        if (runConf.contains("case"))
            appName = runConf.get("case").get + " s" + runConf.get("scenario").get + " r" + runConf.get("run").get
        if (runConf.contains("master"))
            sparkConf = new SparkConf().setAppName(appName).setMaster(runConf.get("master").get)
        else
            sparkConf = new SparkConf().setAppName(appName)
        Desq.initDesq(sparkConf)
        sc = new SparkContext(sparkConf)

        // start application
        if (args.length > 0) { // use command line settings
            setDataLoc(runConf.get("loc").get)
            runDesq()
        } else { // use default settings
            setDataLoc("")
            runConf.put("count.patterns", "true")
            runDesq("I1@2", 2, 1)
        }
    }

    def runGrid() {
        val tests = Array("I1@1", "I1@2", "I2", "IA2", "IA4", "IX1", "IX2", "IX3", "IX4")
        val scenarios = Array(0, 1, 2, 3, 4)

        var output = ""
        for (testCase <- tests) {
            for (scenario <- scenarios) {
                val res = runDesq(testCase, scenario, 1)
                output += testCase + " // " + scenario + " // \t" + res._1 + "\t" + res._2 + "\n"
            }
            output += "\n"
        }

        System.out.println("------------------------------------------------------------------")
        System.out.println("------------------------------------------------------------------")
        System.out.println(output)
    }


    def runDesq(theCase: String, scenario: Int, run: Int): (Long, Long) = {
        runConf.put("case", theCase)
        runConf.put("scenario", scenario.toString)
        runConf.put("run", run.toString)

        runDesq()
    }

    def runDesq(): (Long, Long) = {

        setCase(runConf.get("case").get)
        setScenario(runConf.get("scenario").get.toInt)

        val logger = LogManager.getLogger("DesqRunner")

        System.out.println("------------------------------------------------------------------")
        System.out.println("Distributed Mining " + runConf.get("case").get + " @ " + scenarioStr + "  #" + runConf.get("run").get)
        System.out.println("------------------------------------------------------------------")

        println(sparkConf.toDebugString)

        println("Load dataset from " + dataDir)
        val data = DesqDataset.load(dataDir)

        // Build miner conf
        patternExp = PatExUtils.toFidPatEx(data.dict, patternExp)
        // translate pattern expression to fids
        var minerConf = DesqDfs.createConf(patternExp, sigma)
        if (useDesqCount) {
            minerConf = DesqCount.createConf(patternExp, sigma)
        }
        minerConf.setProperty("desq.mining.prune.irrelevant.inputs", "false")
        minerConf.setProperty("desq.mining.use.two.pass", "true")
        minerConf.setProperty("desq.mining.send.nfas", sendNFAs)
        minerConf.setProperty("desq.mining.merge.suffixes", mergeSuffixes)
        minerConf.setProperty("desq.mining.aggregate.shuffle.sequences", aggregateShuffleSequences)
        minerConf.setProperty("desq.mining.map.repartition", runConf.get("map.repartition").get)

        // Construct miner
        val ctx = new DesqMinerContext(minerConf)
        println("Miner properties: ")
        ctx.conf.prettyPrint()
        val miner = DesqMiner.create(ctx)

        // Mine
        val result = miner.mine(data)

        if (runConf.contains("count.patterns")) {
            // if count.only flag is passed, we just count the number of frequent sequences and aggregate their frequencies
            val (count, freq) = result.sequences.map(ws => (1, ws.weight)).fold((0, 0L))((a, b) => (a._1 + b._1, a._2 + b._2))
            val cfFolder = "countfreq/" + sc.getConf.get("spark.app.id") + "_" + useCase + "_" + scenario + "_" + runConf.get("run").get
            println("Writing (count,freq) (" + count + "," + freq + ") to " + cfFolder)
            sc.parallelize(Array(count, freq), 1).saveAsTextFile(baseFolder + cfFolder)
            (count, freq)
        } else {
            // otherwise, we store the patterns
            val outputFolder = "desq_results/" + sc.getConf.get("spark.app.id") + "_" + useCase + "_" + scenario + "_" + runConf.get("run").get
            result.sequences.saveAsTextFile(baseFolder + outputFolder);
            (0, 0)
        }
    }



    // -- pattern expressions ----------------------------------------------------------------------------------------

    def setCase(thisUseCase: String) {
        verbose = false
        useCase = thisUseCase
        useCase match {
            // -- New York Times Corpus ---------------------------------------------
            case "N0-1991" | "N0" => {
                patternExp = "flourisher@NN@ flourisher@NN@"
                sigma = 10
                if (useCase.contains("1991")) sigma = sigma / 10
                setNytData()
            }
            case "N1-1991" | "N1" => {
                patternExp = "ENTITY@ (VB@+ NN@+? IN@?) ENTITY@"
                sigma = 10
                if (useCase.contains("1991")) sigma = sigma / 10
                setNytData()
            }
            case "N2-1991" | "N2" => {
                patternExp = "(ENTITY@^ VB@+ NN@+? IN@? ENTITY@^)"
                sigma = 100
                if (useCase.contains("1991")) sigma = sigma / 10
                setNytData()
            }
            case "N3-1991" | "N3" => {
                patternExp = "(ENTITY@^ be@VB@=^) DT@? (RB@? JJ@? NN@)"
                sigma = 10
                if (useCase.contains("1991")) sigma = sigma / 10
                setNytData()
            }
            case "N4-1991" | "N4" => {
                patternExp = "(.^){3} NN@"
                sigma = 1000
                if (useCase.contains("1991")) sigma = sigma / 10
                setNytData()
            }
            case "N5-1991" | "N5" => {
                patternExp = "([.^ . .]|[. .^ .]|[. . .^])"
                sigma = 1000
                if (useCase.contains("1991")) sigma = sigma / 10
                setNytData()
            }
            case "N0-full" => {
                patternExp = "flourisher@NN flourisher@NN"
                sigma = 10
                setNytData()
            }
            case "N1-full" => {
                patternExp = "ENTITY (VB+ NN+? IN?) ENTITY"
                sigma = 10
                setNytData()
            }
            case "N2-full" => {
                patternExp = "(ENTITY^ VB+ NN+? IN? ENTITY^)"
                sigma = 100
                setNytData()
            }
            case "N3-full" => {
                patternExp = "(ENTITY^ be@VB=^) DT? (RB? JJ? NN)"
                sigma = 10
                setNytData()
            }
            case "N4-full" => {
                patternExp = "(.^){3} NN"
                sigma = 1000
                setNytData()
            }
            case "N5-full" => {
                patternExp = "([.^ . .]|[. .^ .]|[. . .^])"
                sigma = 1000
                setNytData()
            }
            case r"N4-full-o(\d+)$o" => {
                patternExp = "(.^){3} NN"
                sigma = o.toInt
                setNytData()
            }
            case r"N5-full-o(\d+)$o" => {
                patternExp = "([.^ . .]|[. .^ .]|[. . .^])"
                sigma = o.toInt
                setNytData()
            }
            // -- Amazon Reviews ---------------------------------------------
            case "A0" => {
                patternExp = "B000BM3MMK B000BM3MMK"
                sigma = 500
                setAmznData()
            }
            case "A1" => {
                patternExp = "(Electronics^)[.{0,2}(Electronics^)]{1,4}"
                sigma = 500
                setAmznData()
            }
            case "A2" => {
                patternExp = "(Books)[.{0,2}(Books)]{1,4}"
                sigma = 100
                setAmznData()
            }
            case "A3" => {
                patternExp = "Digital_Cameras@Electronics[.{0,3}(.^)]{1,4}"
                sigma = 100
                setAmznData()
            }
            case "A4" => {
                patternExp = "(Musical_Instruments^)[.{0,2}(Musical_Instruments^)]{1,4}"
                sigma = 100
                setAmznData()
            }
            case "A0-large" => {
                patternExp = "B000BM3MMK B000BM3MMK"
                sigma = 500
                setAmznData()
            }
            case "A1-large" => {
                patternExp = "(Electronics@^)[.{0,2}(Electronics@^)]{1,4}"
                sigma = 500
                setAmznData()
            }
            case "A2-large" => {
                patternExp = "(Books@)[.{0,2}(Books@)]{1,4}"
                sigma = 100
                setAmznData()
            }
            case "A3-large" => {
                patternExp = "\"Digital Cameras@Camera & Photo@Electronics@\"[.{0,3}(.^)]{1,4}"
                sigma = 100
                setAmznData()
            }
            case "A4-large" => {
                patternExp = "(\"Musical Instruments@\"^)[.{0,2}(\"Musical Instruments@\"^)]{1,4}"
                sigma = 100
                setAmznData()
            }
            // -- Traditional frequent sequence mining constraints ---------------------------------------------
            // traditional constraints: no hierarchy, max length, max gap (e.g. MG-FSM): M-[ds]-[omega]-[lambda]-[gamma]
            case r"M-(.+)$d-(\d+)$o-(\d+)$g-(\d+)$l" => {
                patternExp = "(.)[.{0," + g.toInt + "}(.)]{1," + (l.toInt - 1) + "}"
                sigma = o.toInt
                handleDataset(d)
            }
            // traditional constraints: with hierarchy, max length, max gap (e.g. LASH): L-[ds]-[omega]-[lambda]-[gamma]
            case r"L-(.+)$d-(\d+)$o-(\d+)$g-(\d+)$l" => {
                patternExp = "(.^)[.{0," + g.toInt + "}(.^)]{1," + (l.toInt - 1) + "}"
                sigma = o.toInt
                handleDataset(d)
            }
            // traditional constraints: no hierarchy, max length  (e.g. PrefixSpan): S-[ds]-[omega]-[maxLength]
            case r"S-(N|A|N1991|Nfull)$d-(\d+)$o-(\d+)$m" => {
                patternExp = "(.)[.*(.)]{," + m.toInt + "}"
                sigma = o.toInt
                handleDataset(d)
            }
            // -- Toy dataset examples ---------------------------------------------
            case "I1@1" => {
                patternExp = "[c|d]([A^|B=^]+)e"
                sigma = 1
                verbose = true
                setICDMData()
            }
            case "I1@2" => {
                patternExp = "[c|d]([A^|B=^]+)e"
                sigma = 2
                verbose = true
                setICDMData()
            }
            case "I2" => {
                patternExp = "([.^ . .])"
                sigma = 1
                verbose = true
                setICDMData()
            }
            case "IA2" => {
                patternExp = "(A)[.{0,2}(A)]{1,4}"
                sigma = 1
                verbose = true
                setICDMData()
            }
            case "IA4" => {
                patternExp = "(A^)[.{0,2}(A^)]{1,4}"
                sigma = 1
                verbose = true
                setICDMData()
            }
            case "IX1" => {
                patternExp = "[c|d](a2).*([A^|B=^]).*(e)"
                sigma = 1
                verbose = true
                setICDMData()
            }
            case "IX2" => {
                patternExp = "[c|d](a2).*([A^|B=^]).*(B^e)"
                sigma = 1
                verbose = true
                setICDMData()
            }
            case "IX3" => {
                patternExp = "(a1* b12 e)"
                sigma = 1
                verbose = true
                setICDMData()
            }
            case "IX4" => {
                patternExp = "([c|a1] .* [.* A]+ .* [d|e])"
                sigma = 1
                verbose = true
                setICDMData()
            }
            case _ => {
                System.out.println("Do not know the use case " + useCase)
                System.exit(1)
            }
        }
    }

    /** Algorithm variants */
    def setScenario(setScenario: Int) {
        //set some defaults
        scenario = setScenario
        sendNFAs = false
        mergeSuffixes = false
        useDesqCount = false
        aggregateShuffleSequences = false
        scenario match {
            case 0 =>
                scenarioStr = "DDesqCount"
                useDesqCount = true
            case 1 =>
                scenarioStr = "DDesqIP-I"
                aggregateShuffleSequences = false
            case 2 =>
                scenarioStr = "DDesqIP-NFA"
                sendNFAs = true
                mergeSuffixes = true
                aggregateShuffleSequences = true
            case 3 =>
                scenarioStr = "DDesqIP-Tree"
                sendNFAs = true
                mergeSuffixes = false
                aggregateShuffleSequences = false
            case 4 =>
                scenarioStr = "DDesqIP-NFA-NC"
                sendNFAs = true
                mergeSuffixes = true
                aggregateShuffleSequences = false
            case _ =>
                System.out.println("Unknown algorithm variant")
                System.exit(0)
        }
    }

    def setDataLoc(loc: String) {
        if (loc.startsWith("hdfs")) {
            baseFolder = "hdfs:///user/alex/"
        } else {
            if (System.getProperty("os.name").startsWith("Mac")) {
                baseFolder = "file:///Users/alex/"
            } else {
                baseFolder = "file:///home/alex/"
            }
        }
    }

    def handleDataset(d: String): Unit = {
        if (d.charAt(0).equals('N'))
            setNytData()
        else if (d.equals("A"))
            setAmznData()
        else if (d.equals("I"))
            setICDMData()
        else if (d.charAt(0).equals('C'))
            setCWData()
        else {
            println("Unkown dataset " + d + ". Exiting.")
            System.exit(1)
        }
    }

    def setAmznData() {
        var dataset = "amzn"
        if (useCase.contains("large"))
            dataset = "amazon-large"

        if (runConf.contains("read.partitioned.dataset")) {
            dataset += "-" + runConf.get("read.partitioned.dataset").get
        }
        dataDir = baseFolder + "Data/prep/" + dataset + "/"
    }

    def setICDMData() {
        dataDir = baseFolder + "Data/prep/icdm16fids/"
    }

    def setCWData() {
        var dataset = "50"
        if (useCase.contains("25")) {
            dataset = "25"
        }
        dataDir = "hdfs:///data/clueweb-mpii/cw-sen-seq-" + dataset + "-DesqDataset/"
    }

    def setNytData() {
        var dataset = "nyt"
        if (useCase.contains("1991")) {
            dataset = "nyt-1991"
        } else if (useCase.contains("full")) {
            dataset = "nyt-full"
        }
        if (runConf.contains("read.partitioned.dataset")) {
            dataset += "-" + runConf.get("read.partitioned.dataset").get
        }
        dataDir = baseFolder + "Data/prep/" + dataset + "/"
    }

    implicit class Regex(sc: StringContext) {
        def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
    }

}
