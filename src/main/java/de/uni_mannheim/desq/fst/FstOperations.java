package de.uni_mannheim.desq.fst;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class FstOperations {

	private FstOperations() {
	};

	/** Returns an FST that is concatenation of two FSTs */
	public static Fst concatenate(Fst a, Fst b) {
		for (State state : a.getFinalStates()) {
			state.isFinal = false;
			state.simulateEpsilonTransition(b.initialState);
		}
		a.updateStates();
        return a;
	}

	/** Returns an FST that is a union of two FSTs */
	public static Fst union(Fst a, Fst b) {
		State s = new State();
		s.simulateEpsilonTransition(a.initialState);
		s.simulateEpsilonTransition(b.initialState);
		a.initialState = s;
		a.updateStates();
		return a;
	}

	/** Returns an FST that accepts a kleene star of a given FST */
	public static Fst kleene(Fst a) {
		State s = new State();
		s.isFinal = true;
		s.simulateEpsilonTransition(a.initialState);
		for (State p : a.getFinalStates())
			p.simulateEpsilonTransition(s);
		a.initialState = s;
		a.updateStates();
		return a;
	}

	/** Returns an FST that accepts a kleene plus of a given FST */
	public static Fst plus(Fst a) {
		// return concatenate(n, kleene(n));
		for (State s : a.getFinalStates()) {
			s.simulateEpsilonTransition(a.initialState);
		}
		a.updateStates();
		return a;
	}

	/** Returns an FST that accepts zero or one of a given NFA */
	public static Fst optional(Fst a) {
		State s = new State();
		s.simulateEpsilonTransition(a.initialState);
		s.isFinal = true;
		a.initialState = s;
		a.updateStates();
		return a;
	}

	public static Fst repeat(Fst a, int n) {
		if (n == 0) {
		    return new Fst(true);
		}
		Fst[] fstList = new Fst[n - 1];
		for (int i = 0; i < fstList.length; ++i) {
			fstList[i] = a.shallowCopy();
		}
		for (int i = 0; i < fstList.length; ++i) {
			for (State state : a.getFinalStates()) {
				state.isFinal = false;
				state.simulateEpsilonTransition(fstList[i].initialState);
			}
			a.updateStates();
		}
		return a;
	}
	
	public static Fst repeatMin(Fst a, int min) {
		Fst aPlus = plus(a.shallowCopy());
		Fst aMax = repeat(a.shallowCopy(), min - 1);
		return concatenate(aMax, aPlus);
	}
	
	public static Fst repeatMinMax(Fst a, int min, int max) {
		max -= min;
		assert max>=0;
        if (max==0) return new Fst(true);
        Fst fst;
		if (min == 0) {
			fst = new Fst(true);
		} else if (min == 1) {
			fst = a.shallowCopy();
		} else {
			fst = repeat(a.shallowCopy(), min);
		}
		if (max > 0) {
			Fst aa = a.shallowCopy();
			while (--max > 0) {
				Fst ab = a.shallowCopy();
				for (State state : ab.getFinalStates()) {
					state.simulateEpsilonTransition(aa.initialState);
				}
				aa = ab;
			}
			for (State state : fst.getFinalStates()) {
				state.simulateEpsilonTransition(aa.initialState);
			}
		}
		fst.updateStates();
		return fst;
	}
	
	// Minimizes a FST along the lines of Brzozowski's algorithm
	public static void minimize(Fst fst) {
		partiallyDeterminize(fst, fst.reverse());
		partiallyDeterminize(fst, fst.reverse());
	}
	
	public static List<State> reverse(Fst fst) {
		return reverse(fst, true);
	}
	
	public static List<State> reverse(Fst fst, boolean createNewInitialState) {
	
		Int2ObjectMap<List<Transition>> reverseTMap = new Int2ObjectOpenHashMap<>();
		reverseTMap.put(fst.initialState.id, new ArrayList<Transition>());

		for (State s : fst.states) {
			for (Transition t : s.transitionSet) {
				List<Transition> tSet = reverseTMap.get(t.toState.id);
				if (tSet == null) {
					tSet = new ArrayList<Transition>();
					reverseTMap.put(t.toState.id, tSet);
				}
				Transition r = t.shallowCopy();
				r.setToState(s);
				tSet.add(r);
			}
		}

		// Update states with reverse transtitions
		List<State> initialStates = new ArrayList<>();
		for (State s : fst.states) {
			s.transitionSet = reverseTMap.get(s.id);
			if(s.isFinal) {
				initialStates.add(s);
				s.isFinal = false;
			}
		}
		
		fst.initialState.isFinal = true;

		if (createNewInitialState) {
			// If we want one initial state
			fst.initialState = new State();
			for (State a : initialStates) {
				fst.initialState.simulateEpsilonTransition(a);
			}
			fst.updateStates();
			initialStates.clear();
			initialStates.add(fst.initialState);
		}
		return initialStates;
	}
	
	
	public static void partiallyDeterminize(Fst fst) {
		List<State> initialStates = new ArrayList<>();
		initialStates.add(fst.initialState);
		partiallyDeterminize(fst, initialStates);
	}
	
	public static void partiallyDeterminize(Fst fst, List<State> initialStates) {
		fst.initialState = new State();
		
		IntSet initialStateIdSet = new IntOpenHashSet();
		for(State s : initialStates) {
			initialStateIdSet.add(s.id);
		}
		//Maps cFststatesIds to a pFststate
		Map<IntSet, State> pFstStates = new HashMap<>();
		pFstStates.put(initialStateIdSet, fst.initialState);
		
		//Maps transitions(input-output label) to toState ids
		Map<Transition, IntSet> M = new HashMap<>();
		
		//Unprocessed pFstStates (cFstStateIds)
		LinkedList<IntSet> unprocessedStates = new LinkedList<>();
		unprocessedStates.add(initialStateIdSet);
		
		//Processed pFstStates (cFstStateIds)
		Set<IntSet> processedStates = new HashSet<>();
		
		while(unprocessedStates.size() > 0) {
			//Process a pFststate (cFstStateIds)
			IntSet fromCFstStates = unprocessedStates.removeFirst();
			boolean isFinal = false;
			
			
			if(!processedStates.contains(fromCFstStates)) {
				State fromPFstState = pFstStates.get(fromCFstStates);
				M.clear();
				
				//for (input-output label, toStateId) pairs
				for(int cFstStateId : fromCFstStates) {
					State cFstState = fst.getState(cFstStateId);
					if(cFstState.isFinal)
						isFinal = true;
					for(Transition t : cFstState.transitionSet) {
						IntSet reachableStateIds = M.get(t);
						if(reachableStateIds == null) {
							reachableStateIds = new IntOpenHashSet();
							M.put(t, reachableStateIds);
						}
						reachableStateIds.add(t.toState.id);
					}
				}
				
				// Add pFst transition and new state
				for(Transition t : M.keySet()) {
					IntSet reachableCFstStateIds = M.get(t);
					if(!processedStates.contains(reachableCFstStateIds)) {
						unprocessedStates.add(reachableCFstStateIds);
					}
					
					State toPFstState = pFstStates.get(reachableCFstStateIds);
					if(toPFstState == null) {
						toPFstState = new State();
						pFstStates.put(reachableCFstStateIds, toPFstState);
					}
					
					Transition r = t.shallowCopy();
					r.setToState(toPFstState);
					fromPFstState.addTransition(r);
					
				}
			}
			
			// Mark as processed
			processedStates.add(fromCFstStates);
			
			//Final state?
			if(isFinal)
				pFstStates.get(fromCFstStates).isFinal = true;
			
			
		}
		fst.updateStates();
	}
	
}
