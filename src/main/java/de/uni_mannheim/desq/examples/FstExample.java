package de.uni_mannheim.desq.examples;


import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.patex.PatExToFst;

import java.io.IOException;
import java.net.URL;

public class FstExample {

	//Requires dot software
	void icdm16() throws IOException {
		URL dictFile = getClass().getResource("/icdm16-example/dict.json");
		URL dataFile = getClass().getResource("/icdm16-example/data.del");

		// load the dictionary
		Dictionary dict = Dictionary.loadFrom(dictFile);

		// update hierarchy
		SequenceReader dataReader = new DelSequenceReader(dataFile.openStream(), false);
		dict.incFreqs(dataReader);
		dict.recomputeFids();
		
		//String patternExpression = "[c|d]([A^|B=^]+)e";
		String patternExpression = "(A)*";
		//String patternExpression = DesqMiner.patternExpressionFor(0, 3, true);
		//String patternExpression = "([A|B]c*[d|e])";
		//String patternExpression = "([A|B]c+[d|e])";
		System.out.println(patternExpression);

		// create fst
		PatExToFst patExToFst = new PatExToFst(patternExpression, dict);
		Fst fst = patExToFst.translate();
		System.out.println("FST");
		fst.print();
		fst.exportGraphViz("fst-example.gv");
		fst.exportGraphViz("fst-example.pdf"); // graphviz needs to be installed
		String synPatternExpression = fst.toPatternExpression(); // synthesize a pattern expression for the FST

		// and minimize it
		fst.minimize();
		System.out.println("Minimized FST");
		fst.print();
		fst.exportGraphViz("fst-example-minimized.gv");
		fst.exportGraphViz("fst-example-minimized.pdf"); // graphviz needs to be installed

		// now use the synthesized pattern experssion and minimize it
		System.out.println("Minimized FST for synthesized pattern expression");
		fst = new PatExToFst(synPatternExpression, dict).translate();
		fst.minimize();
		fst.print();
	}
	
	public static void main(String[] args) throws IOException {
		new FstExample().icdm16();
	}
}
