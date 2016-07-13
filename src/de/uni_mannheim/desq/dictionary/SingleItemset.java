package de.uni_mannheim.desq.dictionary;

public class SingleItemset extends Itemset {
	int item;
	
	public SingleItemset(int item) {
		this.item = item;
	}
	
	public boolean contains(int item) {
		return item == this.item;
	}
}
