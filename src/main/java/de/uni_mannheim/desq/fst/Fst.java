package de.uni_mannheim.desq.fst;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
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
	
	public int numStates() {
		assert states!= null;
		return states.size();
	}
}
