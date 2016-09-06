package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.PatternWriter;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;

public class DesqMinerContext {
	public Dictionary dict;
	public PatternWriter patternWriter;
	public Configuration conf = new PropertiesConfiguration();
}
