package de.uni_mannheim.desq.dictionary;

import it.unimi.dsi.fastutil.ints.IntIterator;

public abstract class Itemset {

	public abstract boolean contains(int item);
	
	public abstract IntIterator iterator();
	
	public static Itemset create(int item) {
		return new SingleItemset(item);
	}
}
