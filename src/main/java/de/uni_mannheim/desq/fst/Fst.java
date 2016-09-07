package de.uni_mannheim.desq.fst;


import java.util.*;

import de.uni_mannheim.desq.visual.FstVisualizer;


 
public class Fst {
	// initial state
	State initialState;
	
	// list of states; initialized only after state numbers are updated; see updateStates()
	List<State> states = new ArrayList<>();
    List<State> finalStates = new ArrayList<>();

    public Fst() {
        this(false);
    }

	public Fst(boolean isFinal) {
		initialState = new State();
        initialState.isFinal = isFinal;
        updateStates();
	}
	
	public State getInitialState() {
		return initialState;
	}
	
	public void setInitialState(State state) {
		this.initialState = state;
	}
	
	
	public Collection<State> getFinalStates() {
		return finalStates;
	}
	
	/** Recomputes state and final state list. Must be called whenever any state in the FST is modified. */
	public void updateStates() {
		states.clear();
		finalStates.clear();
		int number = 0;
		Set<State> visited = new HashSet<>();
		LinkedList<State> worklist = new LinkedList<>(); // TODO: make stack
		worklist.add(initialState);
		
		while (worklist.size() > 0) {
			State s = worklist.removeFirst();
			if (!visited.contains(s)) {
				s.setId(number++);
				states.add(s);
				if (s.isFinal) finalStates.add(s);
				for (Transition t : s.transitionList) {
					worklist.add(t.toState);
				}
				visited.add(s);
			}
		}
	}
	
	public State getState(int stateId) {
		return states.get(stateId);
	}
	
	public List<State> getStates() {
		return states;
	}
	
	public int numStates() {
		assert states != null;
		return states.size();
	}
	
	// returns a copy of FST with shallow copy of its transitions
	public Fst shallowCopy() {
		Fst fstCopy = new Fst();
		Map<State, State> stateMap = new HashMap<>();
		for(State state : states) {
			stateMap.put(state, new State());
		}
		for(State state : states) {
			State stateCopy = stateMap.get(state);
			stateCopy.isFinal = state.isFinal;
			fstCopy.states.add(stateCopy);
			if (state.isFinal) fstCopy.finalStates.add(stateCopy);
			if(state == initialState) {
				fstCopy.initialState = stateCopy;
			}
			for (Transition t : state.transitionList) {
				Transition tCopy = t.shallowCopy();
				tCopy.setToState(stateMap.get(t.toState));
				stateCopy.addTransition(tCopy);
			}
		}
		return fstCopy;
	}
	
	
	// print the de.uni_mannheim.desq.old.fst to a file
	public void print(String file) {
		FstVisualizer fstVisualizer = new FstVisualizer(file);
		fstVisualizer.beginGraph();
		for(State s : states) {
			for(Transition t : s.transitionList)
                fstVisualizer.add(String.valueOf(s.id), t.toString(), String.valueOf(t.getToState().id));
			if(s.isFinal)
				fstVisualizer.addAccepted(String.valueOf(s.id));
		}
		fstVisualizer.endGraph();
	}
	
	// reverses the edges of the de.uni_mannheim.desq.old.fst and creates one initial state
	// uptates state ids
	public List<State> reverse() {
		return reverse(true);
	}
	
	public List<State> reverse(boolean createNewInitialState) {
		return FstOperations.reverse(this, createNewInitialState);
	}
	
	public void minimize() {
		FstOperations.minimize(this);
	}
}
