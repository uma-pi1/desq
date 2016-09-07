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
	
	// parameters
	//int id;
	List<ExtendedDfaTransition> transitionList;

	//
	BitSet fstStates;
	List<State> fstFinalStates;
	
	public ExtendedDfaState(IntSet stateIdSet, Fst fst) {
		this.transitionList = new ArrayList<>();
		setFstStates(stateIdSet, fst);
	}

	public ExtendedDfaState(int stateId, Fst fst) {
		this.transitionList = new ArrayList<>();
		setFstStates(stateId, fst);
	}

	public void addTransition(ExtendedDfaTransition t) {
		transitionList.add(t);
	}
	
	/*public int consume(int itemFid) {
		for(ExtendedDfaTransition t : transitionList) {
			if(t.matches(itemFid))
				return t.getToStateId();
		}
		return -1;
	}*/
	
	public ExtendedDfaState consume(int itemFid) {
		for(ExtendedDfaTransition t : transitionList) {
			if(t.matches(itemFid))
				return t.getToState();
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private void setFstStates(int stateId, Fst fst) {
		this.fstStates = new BitSet(fst.numStates());
		fstStates.set(stateId);
		State state = fst.getState(stateId);
		if (state.isFinal()) {
			fstFinalStates = new ArrayList<>(1);
			fstFinalStates.add(state);
		} else {
			fstFinalStates = (List<State>)Collections.EMPTY_LIST;
		}
	}
	
	private void setFstStates(IntSet stateIdSet, Fst fst) {
		fstFinalStates = new ArrayList<>(fst.numStates());
		this.fstStates = new BitSet(fst.numStates());
		for(int stateId : stateIdSet) {
			fstStates.set(stateId);
			State state = fst.getState(stateId);
			if (state.isFinal()) {
				fstFinalStates.add(state);
			}
		}
	}

	public boolean isFinal() {
		return !fstFinalStates.isEmpty();
	}

	public BitSet getFstStates() {
		return fstStates;
	}

	public List<State> getFstFinalStates() {
		return fstFinalStates;
	}

}
