package de.uni_mannheim.desq.fst;

import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.Iterator;

public abstract class Transition {
	State toState;

	public abstract IntIterator matchedFidIterator();

	public abstract boolean hasOutput();

	public abstract boolean matches(int item);

	public abstract boolean matchesAll();

	public boolean matchesWithFrequentOutput(int inputFid, int largestFrequentItemFid) {
		if (!hasOutput()) {
			return false;
		}
		Iterator<ItemState> it = consume(inputFid, null);
		while (it.hasNext()) {
			ItemState itemState = it.next();
			if (itemState.itemFid<=largestFrequentItemFid && itemState.itemFid>0) return true;
		}
		return false;
	}

	public abstract boolean matchesAllWithFrequentOutput(int largestFrequentItemFid);

	/** Whether the transition is relevant for each input item (each item matches and the transition either produces
	 * epsilon output or has at least one freuqent output item for each input item). */
	public boolean firesAll(int largestFreuentItemFid) {
		return (hasOutput() && matchesAllWithFrequentOutput(largestFreuentItemFid)) // (.) or (.^)
				|| (!hasOutput() && matchesAll()); // .
	}

	public abstract Iterator<ItemState> consume(int item, Iterator<ItemState> it);

	/** Copy that can share all data but toState */
	public abstract Transition shallowCopy();

	// setToState
	public void setToState(State state) {
		this.toState = state;
	}
	
	// getToState
	public State getToState() {
		return toState;
	}

	public abstract String labelString();

	public abstract String toPatternExpression();

	public abstract boolean isDotEps();
}
