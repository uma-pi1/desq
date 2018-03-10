package de.uni_mannheim.desq.io;

import de.uni_mannheim.desq.dictionary.Dictionary;

public abstract class WithDictionary {
	// can be used by implementing classes to access item information
	protected Dictionary dict = null;

	public Dictionary getDictionary() {
		return dict;
	}
	
	public void setDictionary(Dictionary dict) {
		this.dict = dict;
	}
}
