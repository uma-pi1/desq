package old.fst;


import old.visual.Vdfa;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;



/**
 * Fst.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class Fst {

	State initialState;
	int numStates;
	
	Fst() {
		this.initialState = new State();
		this.numStates = -1;
	}


	Fst(State initialState) {
		this.initialState = initialState;
		this.numStates = -1;
	}

	
	public State getInitialState() {
		return initialState;
	}

	
	public void setInitialState(State initialState) {
		this.initialState = initialState;
	}

	
	public Set<State> getAcceptStates() {
		HashSet<State> accepts = new HashSet<>();
		HashSet<State> visited = new HashSet<>();
		LinkedList<State> worklist = new LinkedList<>();
		worklist.add(initialState);
		visited.add(initialState);
		while (worklist.size() > 0) {
			State s = worklist.removeFirst();
			if (s.accept)
				accepts.add(s);
			for (Transition t : s.transitions)
				if (!visited.contains(t.getToState())) {
					visited.add(t.getToState());
					worklist.add(t.getToState());
				}
		}
		return accepts;
	}

	
	public int getNumStates() {
		if (numStates < 0) {
			HashSet<State> visited = new HashSet<>();
			LinkedList<State> worklist = new LinkedList<>();
			worklist.add(initialState);
			visited.add(initialState);
			while (worklist.size() > 0) {
				State s = worklist.removeFirst();
				for (Transition t : s.transitions)
					if (!visited.contains(t.getToState())) {
						visited.add(t.getToState());
						worklist.add(t.getToState());
					}
			}
			numStates = visited.size();
		}
		return numStates;
	}


	public void prioritize() {
		FstOperations.prioritize(this);
	}
	
	
	public Set<State> reverse() {
		return FstOperations.reverse(this);
	}

	
	/**
	 * Minimizes the FST
	 */
	public void minimize() {
		FstOperations.minimize(this);
	}
	
	/**
	 * Reset state ids by assigning consecutive numbers
	 * @return number of states in FST
	 */
	public void resetStateIds() {
		int number = 0;
		Set<State> visited = new HashSet<>();
		LinkedList<State> worklist = new LinkedList<>();
		worklist.add(initialState);
		
		while (worklist.size() > 0) {
			State s = worklist.removeFirst();
			if (!visited.contains(s)) {
				s.id = number++;
				for (Transition t : s.transitions) {
					worklist.add(t.to);
				}
				visited.add(s);
			}
		}
		// Update numStates here as well
		numStates = number;
	}
	
	
	public XFst optimizeForExecution() {
		return optimizeForExecution(true, false);
	}
	
	public XFst optimizeForExecution(boolean resetStateIds, boolean reverse) {
		XFst xFst = null;
		
		if (resetStateIds) {
			this.resetStateIds();
		}
		HashSet<State> visited = new HashSet<>();
		LinkedList<State> worklist = new LinkedList<>();
		
		if(reverse) {
			worklist.addAll(FstOperations.reverse(this, false));
			xFst = new XFst(getNumStates());
		} else {
			worklist.add(initialState);
			xFst = new XFst(initialState.id, getNumStates());
		}
		
		while(worklist.size() != 0) {
			State s = worklist.removeFirst();
			if(!visited.contains(s)) {
				xFst.initializeState(s.id, s.transitions.size());
				for(Transition t : s.transitions) {
					worklist.add(t.to);
					xFst.addTransition(s.id, t.iLabel, t.oLabel, t.to.id);
				}
				if(s.accept) {
					xFst.addFinalState(s.id);
				}
			}
			visited.add(s);
		}
		return xFst;
	}


	// for quick and dirty debugging
	
	public void print(String file) {
		print(file, true);
	}
	
	public void print(String file, boolean resetStateIds) {
		if(resetStateIds)
			resetStateIds();
		
		Vdfa vdfa = new Vdfa(file);
		vdfa.beginGraph();
		
		LinkedList<State> worklist = new LinkedList<>();
		HashSet<State> visited = new HashSet<>();
		worklist.add(initialState);
		
		while (worklist.size() > 0) {
			State s = worklist.removeFirst();
			if (!visited.contains(s)) {
				for (Transition t : s.transitions) {
					worklist.add(t.to);
					vdfa.add(String.valueOf(s.id), String.valueOf(t.iLabel), t.oLabel.toString(), String.valueOf(t.to.id));
				}
			}
			visited.add(s);
		}
		
		for (State s : getAcceptStates()) {
			vdfa.addAccepted(String.valueOf(s.id));
		}
		vdfa.endGraph();
	}
	

	public Set<State> getStates() {
		Set<State> visited 	= new HashSet<>();
		LinkedList<State> worklist = new LinkedList<>();
		worklist.add(initialState);
		visited.add(initialState);
		while (worklist.size() > 0) {
			State s = worklist.removeFirst();
			for (Transition t : s.transitions)
				if (!visited.contains(t.to)) {
					visited.add(t.to);
					worklist.add(t.to);
				}
		}
		return visited;
	}
	
	public Fst clone() {
		Fst nClone = new Fst();
		
		HashMap<State, State> stateMap = new HashMap<>();
		Set<State> states = getStates();
		for (State state : states)
			stateMap.put(state, new State());
		
		for (State state : states) {
			State sClone = stateMap.get(state);
			sClone.accept = state.accept;
			if(state == initialState) {
				nClone.initialState = sClone;
			}
			for(Transition t : state.transitions) {
				sClone.transitions.add(new Transition(t.iLabel, t.oLabel, stateMap.get(t.to)));
			}
		}
		return nClone;
	}

	public void writeToStdout() {
		
		LinkedList<State> worklist = new LinkedList<>();
		HashSet<State> visited = new HashSet<>();
		worklist.add(initialState);
		
		// Write initial state
		System.out.println(initialState.id + "\t" + this.getNumStates());
		//System.out.println("State transitions");
		while (worklist.size() > 0) {
			State s = worklist.removeFirst();
			if (!visited.contains(s)) {
				for (Transition t : s.transitions) {
					worklist.add(t.to);
					//vdfa.add(t.to.gid, s.gid, t.yield, t.label, false);
					String olabel = null;
					if (t.oLabel.type == OutputLabel.Type.EPSILON) {
						olabel = "eps";
					} else if (t.oLabel.type == OutputLabel.Type.CONSTANT) {
						olabel = String.valueOf(t.oLabel.item);
					} else if (t.oLabel.type == OutputLabel.Type.SELF) {
						olabel = "$";
					} else if (t.oLabel.type == OutputLabel.Type.SELFGENERALIZE) {
						olabel = "$-" + String.valueOf(t.oLabel.item);
					}
					
					System.out.println(s.id + "\t" + t.to.id + "\t" + t.iLabel + "\t" + olabel);
				}
			}
			visited.add(s);
		}
		
		//System.out.println("Accepted states:");
		//Write final states
		for (State s : getAcceptStates()) {
			System.out.println(s.id);		
		}		
	}

}
