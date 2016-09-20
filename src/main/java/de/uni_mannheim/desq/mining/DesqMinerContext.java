package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.PatternWriter;
import de.uni_mannheim.desq.util.DesqProperties;

public final class DesqMinerContext {
	public Dictionary dict = null;
	public PatternWriter patternWriter = null;
	public DesqProperties conf = new DesqProperties();
}
