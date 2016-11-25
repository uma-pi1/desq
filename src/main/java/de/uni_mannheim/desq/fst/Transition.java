package de.uni_mannheim.desq.fst;

import java.util.Iterator;

public abstract class Transition {
	State toState;
	
	public abstract boolean matches(int item);

	public abstract boolean hasOutput();

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

	public abstract Iterator<ItemState> consume(int item, Iterator<ItemState> it);

	// new transitions that can share all data but toStateId
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
