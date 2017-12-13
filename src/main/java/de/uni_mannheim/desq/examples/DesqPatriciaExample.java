package de.uni_mannheim.desq.examples;

import de.uni_mannheim.desq.mining.DesqCountPatricia;
import de.uni_mannheim.desq.mining.DesqPatricia;
import de.uni_mannheim.desq.patex.PatExToItemsetPatEx;
import de.uni_mannheim.desq.util.DesqProperties;

import java.io.IOException;

public class DesqPatriciaExample {

	public static void icdm16() throws IOException {

		String patternExpression = "(.)(.)";
		int sigma = 2;

		patternExpression = new PatExToItemsetPatEx(patternExpression).translate();

		DesqProperties conf = DesqPatricia.createConf(patternExpression, sigma);
		//DesqProperties conf = DesqCount.createConf(patternExpression, sigma);
		/*conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		conf.setProperty("desq.mining.use.lazy.dfa", true);
		conf.setProperty("desq.mining.use.two.pass", true);*/
		conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		conf.setProperty("desq.mining.use.two.pass", false); // has to be set for DesqCount due to bug
		conf.setProperty("desq.mining.optimize.permutations", true);
		ExampleUtils.runIcdm16(conf);
	}

	public static void main(String[] args) throws IOException {
		icdm16();
	}
}
