package de.uni_mannheim.desq.io;

import it.unimi.dsi.fastutil.ints.IntList;

public abstract class SequenceWriter extends WithDictionary {
	/** Collects an input sequence. */
	public abstract void collect(IntList itemFids);
	
	public abstract void close();
}
