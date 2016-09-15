package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.PatternWriter;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;

public final class DesqMinerContext {
	public Dictionary dict = null;
	public PatternWriter patternWriter = null;
	public Configuration conf = new PropertiesConfiguration();
}
