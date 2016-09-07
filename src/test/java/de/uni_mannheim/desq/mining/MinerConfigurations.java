package de.uni_mannheim.desq.mining;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

/** Helper methods to creates configurations for (all) miners. Used for testing. */
public class MinerConfigurations {
    public static Pair<String, Configuration> prefixGrowth(long sigma, int gamma, int lambda, boolean generalize) {
        String minerName = "PrefixGrowth";
        Configuration conf = PrefixGrowthMiner.createConf(sigma, gamma, lambda, generalize);
        return Pair.of(minerName, conf);
    }

    public static Pair<String, Configuration> compressedPrefixGrowth(long sigma, int gamma, int lambda, boolean generalize) {
        String minerName = "CompressedPrefixGrowth";
        Configuration conf = CompressedPrefixGrowthMiner.createConf(sigma, gamma, lambda, generalize);
        return Pair.of(minerName, conf);
    }

    public static Pair<String, Configuration> cSpade(long sigma, int gamma, int lambda, boolean generalize) {
        String minerName = "CSpade";
        Configuration conf = CSpadeMiner.createConf(sigma, gamma, lambda, generalize);
        return Pair.of(minerName, conf);
    }

    public static Pair<String, Configuration> desqCount(long sigma, int gamma, int lambda, boolean generalize, boolean useFlist,
                                          boolean iterative, boolean pruneIrrelevantInputs, boolean useTwoPass) {
        String patternExpression = DesqMiner.patternExpressionFor(gamma, lambda, generalize);
        Configuration conf = DesqCount.createConf(patternExpression, sigma);
        conf.setProperty("desq.mining.use.flist", useFlist);
        conf.setProperty("desq.mining.iterative", iterative);
        conf.setProperty("desq.mining.prune.irrelevant.inputs", pruneIrrelevantInputs);
        conf.setProperty("desq.mining.use.two.pass", useTwoPass);
        String minerName = "DesqCount-" + toLetter(useFlist) + toLetter(iterative) + toLetter(pruneIrrelevantInputs)
                + toLetter(useTwoPass);
        return Pair.of(minerName, conf);
    }

    public static Pair<String, Configuration> desqDfs(long sigma, int gamma, int lambda, boolean generalize,
                                          boolean pruneIrrelevantInputs, boolean useTwoPass) {
        String patternExpression = DesqMiner.patternExpressionFor(gamma, lambda, generalize);
        Configuration conf = DesqDfs.createConf(patternExpression, sigma);
        conf.setProperty("desq.mining.prune.irrelevant.inputs", pruneIrrelevantInputs);
        conf.setProperty("desq.mining.use.two.pass", useTwoPass);
        String minerName = "DesqDfs-" + toLetter(pruneIrrelevantInputs) + toLetter(useTwoPass);
        return Pair.of(minerName, conf);
    }

    public static List<Pair<String, Configuration>> all(long sigma, int gamma, int lambda, boolean generalize) {
        List<Pair<String, Configuration>> allMiners = new ArrayList<>();
        allMiners.add(prefixGrowth(sigma, gamma, lambda, generalize));
        allMiners.add(compressedPrefixGrowth(sigma, gamma, lambda, generalize));
        allMiners.add(cSpade(sigma, gamma, lambda, generalize));
        allMiners.add(desqCount(sigma, gamma, lambda, generalize, false, false, false, false));
        allMiners.add(desqCount(sigma, gamma, lambda, generalize, false, false, true, false));
        allMiners.add(desqCount(sigma, gamma, lambda, generalize, false, false, true, true));
        allMiners.add(desqCount(sigma, gamma, lambda, generalize, false, true, false, false));
        allMiners.add(desqCount(sigma, gamma, lambda, generalize, false, true, true, false));
        allMiners.add(desqCount(sigma, gamma, lambda, generalize, false, true, true, true));
        allMiners.add(desqCount(sigma, gamma, lambda, generalize, true, false, false, false));
        allMiners.add(desqCount(sigma, gamma, lambda, generalize, true, false, true, false));
        allMiners.add(desqCount(sigma, gamma, lambda, generalize, true, false, true, true));
        allMiners.add(desqCount(sigma, gamma, lambda, generalize, true, true, false, false));
        allMiners.add(desqCount(sigma, gamma, lambda, generalize, true, true, true, false));
        allMiners.add(desqCount(sigma, gamma, lambda, generalize, true, true, true, true));
        allMiners.add(desqDfs(sigma, gamma, lambda, generalize, false, false));
        allMiners.add(desqDfs(sigma, gamma, lambda, generalize, true, false));
        allMiners.add(desqDfs(sigma, gamma, lambda, generalize, true, true));
        return allMiners;
    }

    private static String toLetter(boolean b) {
        return b ? "t" : "f";
    }
}
