package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.ints.IntList;

public abstract class DesqMiner {
	// if null, patterns are mined but not collected
	protected DesqMinerContext ctx;
	
	protected DesqMiner(DesqMinerContext ctx) {
		this.ctx = ctx;
	}
	
	/** Adds a new input sequence. The provided sequence must not be buffered by this miner. */
	protected abstract void addInputSequence(IntList inputSequence);
	
	/** Mines all added input sequences */
	protected abstract void mine();
}
