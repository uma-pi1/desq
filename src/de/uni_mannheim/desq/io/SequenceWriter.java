package de.uni_mannheim.desq.io;

import de.uni_mannheim.desq.dictionary.Dictionary;
import it.unimi.dsi.fastutil.ints.IntList;

public abstract class SequenceWriter {
	// can be used by implementing classes to access item information
	Dictionary dict = null;
	
	/** Collects an input sequence. */
	public abstract void collect(IntList itemFids);
	
	public abstract void close();
	
	public void setDictionary(Dictionary dict) {
		this.dict = dict;
	}
}
