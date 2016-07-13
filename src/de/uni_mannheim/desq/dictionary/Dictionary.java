package de.uni_mannheim.desq.dictionary;

import java.util.Iterator;

import it.unimi.dsi.fastutil.ints.IntIterator;

public abstract class Dictionary {
	public abstract boolean contains(int item);
	public abstract Item getItem(int item);
	public abstract Iterator<Item> parents(int item);
	public abstract Itemset descendantsItemset(Itemset itemset);
	public abstract Dictionary descendantsDictionary(Itemset itemset);
	
	public Itemset descendantsItemset(int item) {
		return descendantsItemset(new SingleItemset(item));		
	}

	public Dictionary descendantsDictionary(int item) {
		return descendantsDictionary(new SingleItemset(item));		
	}
	
	public IntIterator ascendantsIterator(int item) {
		return null;
	}

}
