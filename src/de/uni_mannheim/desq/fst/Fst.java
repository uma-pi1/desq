package de.uni_mannheim.desq.fst;

// don't keep abstract 
public abstract class Fst {
	// initial stateId
	// final states
	// array of states
	
	public abstract State getInitialState();
	public abstract State getState(int stateId);
	
	public abstract Fst shallowCopy();
}
