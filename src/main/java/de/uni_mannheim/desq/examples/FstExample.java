package de.uni_mannheim.desq.examples;


import java.io.IOException;
import java.net.URL;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.patex.PatEx;

public class FstExample {

	//Requires dot software
	void icdm16() throws IOException {
		URL dictFile = getClass().getResource("/icdm16-example/dict.json");
		URL dataFile = getClass().getResource("/icdm16-example/data.del");

		// load the dictionary
		Dictionary dict = Dictionary.loadFrom(dictFile);

		// update hierarchy
		SequenceReader dataReader = new DelSequenceReader(dataFile.openStream(), false);
		dict.incCounts(dataReader);
		dict.recomputeFids();
		
		String patternExpression = "[c|d]([A^|B=^]+)e";
		//String patternExpression = DesqMiner.patternExpressionFor(0, 3, true);
		//String patternExpression = "([A|B]c*[d|e])";
		//String patternExpression = "([A|B]c+[d|e])";
		
		// create de.uni_mannheim.desq.old.fst
		PatEx patEx = new PatEx(patternExpression, dict);
		Fst fst = patEx.translate();
		fst.print("fst");
		
		fst.minimize();
		fst.print("fst-min");
	}
	
	public static void main(String[] args) throws IOException {
		new FstExample().icdm16();
	}
}
