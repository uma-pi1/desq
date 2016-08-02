package de.uni_mannheim.desq.journal.edfa;

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
	boolean isFinal;
	
	//
	BitSet fstStates;
	
	public ExtendedDfaState() {
		this.transitionList = new ArrayList<ExtendedDfaTransition>();
		this.isFinal = false;
	}
	
	public ExtendedDfaState(boolean isFinal) {
		this.transitionList = new ArrayList<ExtendedDfaTransition>();
		this.isFinal = isFinal;
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
	
	
	public void setFstStates(int stateId, int numFstStates) {
		this.fstStates = new BitSet(numFstStates);
		fstStates.set(stateId);
	}
	
	public void setFstStates(IntSet stateIdSet, int numFstStates) {
		this.fstStates = new BitSet(numFstStates);
		for(int stateId : stateIdSet) {
			fstStates.set(stateId);
		}
	}
	

	public BitSet getFstStates() {
		return fstStates;
	}
}
