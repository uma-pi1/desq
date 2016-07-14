package de.uni_mannheim.desq.collector;

import de.uni_mannheim.desq.dictionary.Dictionary;
import it.unimi.dsi.fastutil.ints.IntList;

public abstract class PatternCollector {
	// can be used by implementing classes to access item information
	Dictionary dict = null;
	
	/** Collects a pattern mined by Desq. The provided IntList should not be buffered by the collector. */
	public abstract void collect(IntList itemFids, long count);
	
	public abstract void close();
	
	public void setDictionary(Dictionary dict) {
		this.dict = dict;
	}
}
