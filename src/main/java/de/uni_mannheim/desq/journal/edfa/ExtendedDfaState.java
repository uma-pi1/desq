package de.uni_mannheim.desq.journal.edfa;

import de.uni_mannheim.desq.fst.Fst;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * ExtendedDfaState.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class ExtendedDfaState {
	
	// parameters
	//int id;
	List<ExtendedDfaTransition> transitionList;

	//
	BitSet fstStates;
	IntList fstFinalStates;
	
	public ExtendedDfaState(IntSet stateIdSet, Fst fst) {
		this.transitionList = new ArrayList<ExtendedDfaTransition>();
		setFstStates(stateIdSet, fst);
	}

	public ExtendedDfaState(int stateId, Fst fst) {
		this.transitionList = new ArrayList<ExtendedDfaTransition>();
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
	
	private void setFstStates(int stateId, Fst fst) {
		this.fstStates = new BitSet(fst.numStates());
		fstStates.set(stateId);
		if (fst.getState(stateId).isFinal()) {
			fstFinalStates = new IntArrayList(1);
			fstFinalStates.add(stateId);
		} else {
			fstFinalStates = new IntArrayList(0);
		}
	}
	
	private void setFstStates(IntSet stateIdSet, Fst fst) {
		fstFinalStates = new IntArrayList(fst.numStates());
		this.fstStates = new BitSet(fst.numStates());
		for(int stateId : stateIdSet) {
			fstStates.set(stateId);
			if (fst.getState(stateId).isFinal()) {
				fstFinalStates.add(stateId);
			}
		}
	}

	public boolean isFinal() {
		return !fstFinalStates.isEmpty();
	}

	public BitSet getFstStates() {
		return fstStates;
	}
}
