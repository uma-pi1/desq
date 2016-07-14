package de.uni_mannheim.desq.fst;

// don't keep abstract 
public abstract class Fst {
	// initial stateId
	// final states
	// array of states
	
	public abstract State getInitialState();
	public abstract void setInitialState(State state);
	public abstract State getState(int stateId);
	public abstract State addState();
	public abstract Fst shallowCopy();
	
	// sets state numbers
	public abstract void updateStateNumbers();
}
