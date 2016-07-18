package de.uni_mannheim.desq.fst;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;


 
public class Fst {
	// initial state
	State initialState;
	
	// list of states; initialized only after state numbers are updated; see updateStateNumbers()
	ArrayList<State> states;
	
	public Fst() {
		initialState = new State();
	}
	
	public State getInitialState() {
		return initialState;
	}
	
	public void setInitialState(State state) {
		this.initialState = state;
	}
	
	
	public Set<State> getFinalStates() {
		HashSet<State> finalStates = new HashSet<State>();
		HashSet<State> visited = new HashSet<State>();
		LinkedList<State> worklist = new LinkedList<State>();
		worklist.add(initialState);
		visited.add(initialState);
		while (worklist.size() > 0) {
			State s = worklist.removeFirst();
			if (s.isFinal())
				finalStates.add(s);
			for (Transition t : s.transitionSet)
				if (!visited.contains(t.getToState())) {
					visited.add(t.getToState());
					worklist.add(t.getToState());
				}
		}
		return finalStates;
	}
	
	// sets state numbers and add all states indexed by stateIds to this fst
	public void updateStateNumbers() {
		states = new ArrayList<State>();
		int number = 0;
		Set<State> visited = new HashSet<State>();
		LinkedList<State> worklist = new LinkedList<State>();
		worklist.add(initialState);
		
		while (worklist.size() > 0) {
			State s = worklist.removeFirst();
			if (!visited.contains(s)) {
				s.setStateId(number++);
				states.add(s);
				for (Transition t : s.transitionSet) {
					worklist.add(t.toState);
				}
				visited.add(s);
			}
		}
	}
	
	public State getState(int stateId) {
		return states.get(stateId);
	}
	
	public Set<State> getStates() {
		Set<State> visited 	= new HashSet<State>();
		LinkedList<State> worklist = new LinkedList<State>();
		worklist.add(initialState);
		visited.add(initialState);
		while (worklist.size() > 0) {
			State s = worklist.removeFirst();
			for (Transition t : s.transitionSet)
				if (!visited.contains(t.toState)) {
					visited.add(t.toState);
					worklist.add(t.toState);
				}
		}
		return visited;
	}
	
	public int numStates() {
		assert states!= null;
		return states.size();
	}
	
	// returns a copy of FST with shallow copy of its transitions
	public Fst shallowCopy() {
		Fst fstCopy = new Fst();
		Map<State, State> stateMap = new HashMap<State, State>();
		Set<State> states = getStates();
		for(State state : states) {
			stateMap.put(state, new State());
		}
		for(State state : states) {
			State stateCopy = stateMap.get(state);
			stateCopy.isFinal = state.isFinal;
			if(state == initialState) {
				fstCopy.initialState = stateCopy;
			}
			for(Transition t : state.transitionSet) {
				Transition tCopy = t.shallowCopy();
				tCopy.setToState(stateMap.get(t.toState));
				stateCopy.addTransition(tCopy);
			}
		}
		return fstCopy;
	}
}
