package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.collector.PatternCollector;
import it.unimi.dsi.fastutil.ints.IntList;

public abstract class DesqMiner {
	// if null, patterns are mined but not collected
	protected PatternCollector patternCollector;
	
	protected DesqMiner(PatternCollector patternCollector) {
		setPatternCollector(patternCollector);
	}
	
	public void setPatternCollector(PatternCollector patternCollector) {
		this.patternCollector = patternCollector;
	}
	
	/** Adds a new input sequence. The provided sequence must not be buffered by this miner. */
	protected abstract void addInputSequence(IntList inputSequence);
	
	/** Mines all added input sequences */
	protected abstract void mine();
}
