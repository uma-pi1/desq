package de.uni_mannheim.desq.io;

import de.uni_mannheim.desq.dictionary.Dictionary;

abstract class WithDictionary {
	// can be used by implementing classes to access item information
	Dictionary dict = null;

	public Dictionary getDictionary() {
		return dict;
	}
	
	public void setDictionary(Dictionary dict) {
		this.dict = dict;
	}
}
