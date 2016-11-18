package de.uni_mannheim.desq.fst;

import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.BitSet;


/**
 * ExtendedDfaState.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public final class ExtendedDfaState {
	
	ExtendedDfaState[] transitionTable;

	// corresponding FST states
	BitSet fstStates;

	// these are set to true if the corresponding states
	// contain isFinal or isFinalComplete
	boolean isFinal = false;
	boolean isFinalComplete = false;

	public ExtendedDfaState(IntSet stateIdSet, Fst fst, int size) {
		initTransitionTable(size);
		setFstStates(stateIdSet, fst);
	}

	private void initTransitionTable(int size){
		transitionTable = new ExtendedDfaState[size+1];
	}

	public void addToTransitionTable(int itemFid, ExtendedDfaState toEDfaState) {
		transitionTable[itemFid] = toEDfaState;
	}
	
	public ExtendedDfaState consume(int itemFid) {
		return transitionTable[itemFid];
	}

	private void setFstStates(IntSet stateIdSet, Fst fst) {
		this.fstStates = new BitSet();
		for(int stateId : stateIdSet) {
			fstStates.set(stateId);
			State state = fst.getState(stateId);
			if (state.isFinal()) {
				isFinal = true;
			}
			if (state.isFinalComplete()) {
				isFinalComplete = true;
			}
		}
	}

	public boolean isFinal() {
		return isFinal;
	}

	public boolean isFinalComplete() {
		return isFinalComplete;
	}

	public BitSet getFstStates() {
		return fstStates;
	}
}
