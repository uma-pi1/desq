package de.uni_mannheim.desq.io;

import it.unimi.dsi.fastutil.ints.IntList;

public abstract class PatternWriter extends WithDictionary {
	/** Collects a pattern mined by Desq. The provided IntList must not be buffered by the collector. */
	public abstract void write(IntList itemFids, long count);
	
	public abstract void close();
}
