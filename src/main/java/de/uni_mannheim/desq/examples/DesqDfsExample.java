package de.uni_mannheim.desq.examples;

import de.uni_mannheim.desq.mining.DesqDfs;
import de.uni_mannheim.desq.util.DesqProperties;

import java.io.IOException;

public class DesqDfsExample {
	public static void nyt() throws IOException {
		int sigma = 10;
		int gamma = 1;
		int lambda = 3;
		boolean generalize = false;
		String patternExp = DesqDfs.patternExpressionFor(gamma, lambda, generalize);
		//patternExp = "(JJ@ JJ@ NN@)";

		DesqProperties conf = DesqDfs.createConf(patternExp, sigma);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", false);
		conf.setProperty("desq.mining.use.two.pass", false);
		ExampleUtils.runNyt(conf);
	}

	public static void icdm16() throws IOException {
		String patternExp= "[c|d]([A^|B=^]+)e";
		int sigma = 2;

		//patternExp= "(a1)..$";
		//sigma = 1;

		//patternExp= "^.(a1)";
		//sigma = 1;

		DesqProperties conf = DesqDfs.createConf(patternExp, sigma);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		conf.setProperty("desq.mining.use.two.pass", false);
		ExampleUtils.runIcdm16(conf);
	}

	public static void netflixFlat() throws IOException {
		String patternExp= "(.)";
        int sigma = 100000;
		// these patterns are all spurious due to the way the data is created (ratings on same day ordered by id)
		patternExp="(.).{0,3}(.).{0,3}('The Incredibles#2004#10947')";
		sigma = 1000;

		DesqProperties conf = DesqDfs.createConf(patternExp, sigma);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		conf.setProperty("desq.mining.use.two.pass", true);
		ExampleUtils.runNetflixFlat(conf);
	}

    public static void netflixDeep() throws IOException {
        String patternExp= "(.)";
        int sigma = 100000;

        // these patterns are all spurious due to the way the data is created (ratings on same day ordered by id)
        patternExp="(.).{0,3}(.).{0,3}('The Incredibles#2004#10947'=^)";
        sigma = 1000;
        patternExp="('5stars'{2})";
        sigma = 10000;

		DesqProperties conf = DesqDfs.createConf(patternExp, sigma);
        conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
        conf.setProperty("desq.mining.use.two.pass", true);
        ExampleUtils.runNetflixDeep(conf);
    }

    public static void protein() throws IOException {
		String patternExpression = "([S|T]).*(.).*([R|K])";
		//String patternExpression = "([S=|T=]).*(.).*([R=|K=])";
		int sigma = 500;

		DesqProperties conf = DesqDfs.createConf(patternExpression, sigma);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		conf.setProperty("desq.mining.use.two.pass", true);
		ExampleUtils.runProtein(conf);
	}

	public static void main(String[] args) throws IOException {
		//icdm16();
		//nyt();
		//netflixFlat();
        //netflixDeep();
		protein();
	}
}
