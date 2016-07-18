package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.io.PatternWriter;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DesqMinerContext {
	public Dictionary dict;
	public PatternWriter patternWriter;
	public Fst fst; /** TODO: this does not belong here */
	public Properties properties = new Properties();
}
