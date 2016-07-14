package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.io.PatternWriter;

public class DesqMinerContext {
	public Dictionary dict;
	public PatternWriter patternWriter;
	public int sigma;
	public Fst fst;
}
