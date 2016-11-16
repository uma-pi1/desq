package de.uni_mannheim.desq.fst;

import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/**
 * ExtendedDfaState.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public final class ExtendedDfaState {
	
	ExtendedDfaState[] transitionTable;

	BitSet fstStates;
	List<State> fstFinalStates;
	List<State> fstFinalCompleteStates;

	public ExtendedDfaState(IntSet stateIdSet, Fst fst, int size) {
		initTransitionTable(size);
		setFstStates(stateIdSet, fst);
	}

	public ExtendedDfaState(int stateId, Fst fst, int size) {
		initTransitionTable(size);
		setFstStates(stateId, fst);
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

	// TODO: all final complte states should also be in the final list
	@SuppressWarnings("unchecked")
	private void setFstStates(int stateId, Fst fst) {
		this.fstStates = new BitSet(fst.numStates());
		fstStates.set(stateId);
		State state = fst.getState(stateId);
		if (state.isFinal()) {
			fstFinalStates = new ArrayList<>(1);
			fstFinalStates.add(state);
			fstFinalCompleteStates = (List<State>)Collections.EMPTY_LIST;
		} else if(state.isFinalComplete()) {
			fstFinalCompleteStates = new ArrayList<>(1);
			fstFinalCompleteStates.add(state);
			fstFinalStates = (List<State>)Collections.EMPTY_LIST;
		}
		else {
			fstFinalStates = (List<State>)Collections.EMPTY_LIST;
			fstFinalCompleteStates = (List<State>)Collections.EMPTY_LIST;
		}
	}
	
	private void setFstStates(IntSet stateIdSet, Fst fst) {
		fstFinalStates = new ArrayList<>(fst.numStates());
		fstFinalCompleteStates = new ArrayList<>(fst.numStates());
		this.fstStates = new BitSet();
		for(int stateId : stateIdSet) {
			fstStates.set(stateId);
			State state = fst.getState(stateId);
			if (state.isFinal()) {
				fstFinalStates.add(state);
			}
			if (state.isFinalComplete()) {
				fstFinalCompleteStates.add(state);
			}
		}
	}

	public boolean isFinal() {
		return !fstFinalStates.isEmpty();
	}

	public boolean isFinalComplete() {
		return !fstFinalCompleteStates.isEmpty();
	}

	public BitSet getFstStates() {
		return fstStates;
	}

	public List<State> getFstFinalStates() {
		return fstFinalStates;
	}

	public List<State> getFstFinalCompleteStates() {
		return fstFinalCompleteStates;
	}
}
