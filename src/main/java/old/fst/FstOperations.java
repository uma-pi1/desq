package old.fst;


import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import old.utils.KPair;


/**
 * FstOperations.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public final class FstOperations {

	private FstOperations() {
	}

    /** Returns an FST that is concatenation of two FSTs */
	public static Fst concatenate(Fst a, Fst b) {
		for (State state : a.getAcceptStates()) {
			state.accept = false;
			state.addEpsilon(b.initialState);
		}
		return a;
	}

	/** Returns an FST that is a union of two FSTs */
	public static Fst union(Fst a, Fst b) {
		State s = new State();
		s.addEpsilon(a.initialState);
		s.addEpsilon(b.initialState);
		a.initialState = s;
		return a;
	}

	/** Returns an FST that accepts a kleene star of a given FST */
	public static Fst kleene(Fst a) {
		State s = new State();
		s.accept = true;
		s.addEpsilon(a.initialState);
		for (State p : a.getAcceptStates())
			p.addEpsilon(s);
		a.initialState = s;
		return a;
	}

	/** Returns an FST that accepts a kleene plus of a given FST */
	public static Fst plus(Fst a) {
		// return concatenate(n, kleene(n));
		for (State s : a.getAcceptStates()) {
			s.addEpsilon(a.initialState);
		}
		return a;
	}

	/** Returns an FST that accepts zero or one of a given NFA */
	public static Fst optional(Fst a) {
		State s = new State();
		s.addEpsilon(a.initialState);
		s.accept = true;
		a.initialState = s;
		return a;
	}

	public static Fst repeatMax(Fst a, int max) {
		if (max == 0) {
			System.err.println("ERROR");
			System.exit(-1);
		}
		Fst[] fstList = new Fst[max - 1];
		for (int i = 0; i < fstList.length; ++i) {
			fstList[i] = a.clone();
		}
		for (int i = 0; i < fstList.length; ++i) {
			for (State state : a.getAcceptStates()) {
				state.accept = false;
				state.addEpsilon(fstList[i].initialState);
			}
		}
		return a;
	}
	
	public static Fst repeatMin(Fst a, int min) {
		Fst aPlus = plus(a.clone());
		Fst aMax = repeatMax(a.clone(), min - 1);
		return concatenate(aMax, aPlus);
	}
	
	public static Fst repeatMinMax(Fst a, int min, int max) {
		max -= min;
		Fst fst;
		if (min == 0) {
			fst = new Fst();
			fst.initialState.accept = true;
		} else if (min == 1) {
			fst = a.clone();
		} else {
			fst = repeatMax(a.clone(), min);
		}
		if (max > 0) {
			Fst aa = a.clone();
			while (--max > 0) {
				Fst ab = a.clone();
				for (State state : ab.getAcceptStates()) {
					state.addEpsilon(aa.initialState);
				}
				aa = ab;
			}
			for (State state : fst.getAcceptStates()) {
				state.addEpsilon(aa.initialState);
			}
		}
		return fst;
	}
	
	/** 
	 * Partially determinze the Fst
	 */
	public static void prioritize(Fst fst) {
		Set<State> initialStates = new HashSet<>();
		initialStates.add(fst.initialState);
		partiallyDeterminize(fst, initialStates);
	}
	
	/** 
	 * Partially determinze the given FST given a set of initial states 
	 */
	public static void partiallyDeterminize(Fst fst, Set<State> initialStates) {
		

		fst.initialState = new State();
		
		HashMap<Set<State>, State> pFstStates = new HashMap<>();
		pFstStates.put(initialStates, fst.initialState);

		// Map from input-output label to cFST states
		HashMap<KPair<Integer, OutputLabel>, Set<State>> M = new HashMap<>();

		// Unprocessed pFST states
		LinkedList<Set<State>> unprocessedStates = new LinkedList<>();
		unprocessedStates.add(initialStates);

		// Processed pFST states
		HashSet<Set<State>> processedStates = new HashSet<>();

		while (unprocessedStates.size() > 0) {
			// Process a pFST state
			Set<State> fromCFstStates = unprocessedStates.removeFirst();

			if (!processedStates.contains(fromCFstStates)) {

				State fromPFstState = pFstStates.get(fromCFstStates);

				M.clear();

				// for (input-output lable,toState) pairs
				for (State cFstState : fromCFstStates) {
					for (Transition t : cFstState.transitions) {
						KPair<Integer, OutputLabel> label = new KPair<>(t.iLabel, t.oLabel);
						Set<State> reachableStates = M.get(label);
						if (reachableStates == null) {
							reachableStates = new HashSet<>();
							M.put(label, reachableStates);
						}
						reachableStates.add(t.to);
					}
				}

				// Add pFST transitions and (new state)
				for (KPair<Integer, OutputLabel> label : M.keySet()) {
					Set<State> reachableCFstStates = M.get(label);
					if (!processedStates.contains(reachableCFstStates)) {
						unprocessedStates.add(reachableCFstStates);
						// pFstStates.put(cFstStates, new State());
					}

					State toPFstState = pFstStates.get(reachableCFstStates);
					if (toPFstState == null) {
						toPFstState = new State();
						pFstStates.put(reachableCFstStates, toPFstState);
					}

					fromPFstState.addTransition(new Transition(label.getLeft(), label.getRight(), toPFstState));
				}

			}

			// Mark ss as processed
			processedStates.add(fromCFstStates);

			// Final state?
			// TODO: Integrate above
			for (State s : fromCFstStates) {
				if (s.accept) {
					pFstStates.get(fromCFstStates).accept = true;
				}
			}
		}
	}
	
	
	/** Reverses a FST 
	 *  Sets final states as initial states
	 *  Returns set of initial states
	 */
	public static Set<State> reverse(Fst fst) {
		return reverse(fst, true);
	}
	
	public static Set<State> reverse(Fst fst, boolean createNewInitialState) {
		HashMap<State, HashSet<Transition>> reverseTMap = new HashMap<>();
		Set<State> visited 	= new HashSet<>();
		LinkedList<State> worklist = new LinkedList<>();
		
		worklist.add(fst.initialState);
		
		reverseTMap.put(fst.initialState, new HashSet<>());
	
		while (worklist.size() > 0) {
			State s = worklist.removeFirst();
			if (!visited.contains(s)) {
				for (Transition t : s.transitions) {
					HashSet<Transition> tSet = reverseTMap.get(t.to);
					if (tSet == null) {
						tSet = new HashSet<>();
						reverseTMap.put(t.to, tSet);
					}
					tSet.add(new Transition(t.iLabel, t.oLabel, s));
					worklist.add(t.to);
				}
				visited.add(s);
			}
		}
		
		// Create final states as initial states
		Set<State> initialStates = new HashSet<>();
		for(State s : visited) {
			s.transitions = reverseTMap.get(s);
			if(s.accept) {
				initialStates.add(s);
				s.accept = false;
			}
		}
		fst.initialState.accept = true;
	
		if(createNewInitialState) {
			// If we want one initial state
			fst.initialState = new State();
			for(State a : initialStates) {
				fst.initialState.addEpsilon(a);
			}
		}
		return initialStates;
	}
	
	
	/**
	 * Minimizes a FST along the lines of Brzozowski's algorithm
	 * @param fst
	 */
	public static void minimize(Fst fst) {
		partiallyDeterminize(fst, fst.reverse());
		partiallyDeterminize(fst, fst.reverse());
	}
	
}
