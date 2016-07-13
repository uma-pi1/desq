package de.uni_mannheim.desq.fst;

import java.util.Iterator;

// don't keep abstract
public abstract class State {
	// set of transitions
	boolean isFinal;
	
	private class ItemStateIterator implements Iterator<ItemState> {
		int transitionId;
		ItemStateIterator it; // for current transition
		
		@Override
		public boolean hasNext() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public ItemState next() {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	
	public Iterator<ItemState> consume(int item) {
		return consume(item, null);
	}
	public abstract Iterator<ItemState> consume(int item, Iterator<ItemState> it);
	public boolean isFinal() { return isFinal; }
}
