package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.collector.PatternCollector;
import it.unimi.dsi.fastutil.ints.IntList;

public abstract class DesqMiner {
	protected PatternCollector patternCollector;
	protected boolean collectPatterns;
	
	DesqMiner(PatternCollector patternCollector, boolean collectPatterns) {
		this.patternCollector = patternCollector;
		this.collectPatterns = collectPatterns;
	}

	DesqMiner(PatternCollector patternCollector) {
		this(patternCollector, patternCollector != null);
	}
	
	/** Adds a new input sequence. The provided sequence must not be buffered by this miner. */
	protected abstract void addInputSequence(IntList inputSequence);
	
	/** Mines all added input sequences */
	protected abstract void mine();
}
