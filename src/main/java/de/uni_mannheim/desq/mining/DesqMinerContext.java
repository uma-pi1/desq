package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.PatternWriter;
import de.uni_mannheim.desq.util.DesqProperties;

public final class DesqMinerContext {
	public Dictionary dict;
	public PatternWriter patternWriter; 	// if null, patterns are mined but not collected
	public DesqProperties conf;

	public DesqMinerContext(DesqProperties conf, Dictionary dict, PatternWriter patternWriter) {
		this.dict = dict;
		this.conf = conf;
		this.patternWriter = patternWriter;
	}

	public DesqMinerContext(DesqProperties conf, Dictionary dict) {
		this(conf, dict, null);
	}

	public DesqMinerContext(DesqProperties conf) {
		this(conf, null, null);
	}

	public DesqMinerContext(Dictionary dict) {
		this(new DesqProperties(), dict, null);
	}

	public DesqMinerContext() {
		this(new DesqProperties(), null, null);
	}
}
