package de.uni_mannheim.desq.fst;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.*;

public final class FstOperations {

	private FstOperations() {
	}

	/** Returns an FST that unions all FST permutations */
	public static Fst handlePermute(HashMap<Fst,int[]> inputFsts) {
		ArrayList<Fst> fsts = new ArrayList<>();
		//handle frequencies
		int connectorSize = 0;
		Fst connector = null;
		for (Map.Entry<Fst,int[]> entry: inputFsts.entrySet()){
			if(entry.getValue() != null){
				//min occurrences
				int min = entry.getValue()[0];
				int max = entry.getValue()[1];
				if (min > 0){
					fsts.addAll(addExactly(entry.getKey(),min));
				}
				if(max == 0) {
					//no max -> find all occurrences (kleene *) in all combinations -> use as connector
					connector = (connector != null)
							? concatenate(connector, kleene(entry.getKey().shallowCopy()))
							: kleene(entry.getKey().shallowCopy());
					connectorSize++;

				}else if (max > min){
					//min and max provided
					int dif = max - min;
					//Difference between min and max represented with optionals
					fsts.addAll(addExactly(optional(entry.getKey().shallowCopy()), dif));
				}
			}else{
				//No frequencies -> add just once
				fsts.add(entry.getKey());
			}
		}
		//Ensure that connector is optional and can repeat itself -> kleene *
		if(connector != null && connectorSize > 1){
			connector = kleene(connector);
		}

		//start recursion
		Fst permuted = (fsts.size() > 0) ? permute(null, fsts, connector) : null;

		//add connector at beginning and end as well (if defined)
		if (connector != null) {
			//connector.exportGraphViz("connector.pdf");
			if(permuted != null) {
				permuted = concatenate(connector.shallowCopy(), permuted);
				permuted = concatenate(permuted, connector.shallowCopy());
			}else{
				//If nothing to permute (only connector left) -> return connector
				permuted = connector;
			}
		}
		return permuted;
	}

	private static ArrayList<Fst> addExactly(Fst a, int n){
		ArrayList<Fst> fstList = new ArrayList<>();
		for (int i = 0; i < n; ++i) {
			fstList.add(a.shallowCopy());
		}
		return fstList;
	}

	/** Recursive method to permute all elements - each recursion: define prefix + remaining elements */
	private static Fst permute(Fst prefixFst, List<Fst> fsts, Fst connector) {
		HashSet<String> usedFst = new HashSet<>();

		assert fsts.size() > 0;
		if(fsts.size() > 1){
			//more than one item -> permute (recursion step)
			Fst unionFst = null;
			for (Fst fst: fsts){
				if(!usedFst.contains(fst.toPatternExpression())) {
					usedFst.add(fst.toPatternExpression());
					//create copy of to be added fst (avoid wrong state transitions)
					Fst addedFst = fst.shallowCopy();
					//add connector (eg "[A.*]*.*")to Fst (A.*B.* -> A.*[A.*]*B.*)
					if (connector != null) addedFst = concatenate(addedFst, connector.shallowCopy());
					//copy remaining fsts into list (as shallow copy)
					List<Fst> partFsts = new ArrayList<>();
					for (Fst copy : fsts) {
						if (copy != fst) {
							partFsts.add(copy.shallowCopy());
						}
					}
					//handle new prefix
					Fst newPrefixFst = (prefixFst != null)
							//Within recursion: concat items (prefix + new first item)
							? concatenate(prefixFst.shallowCopy(), addedFst)
							//Or first recursion step (no existing prefix yet)
							: addedFst;

					//recursions combined by union
					unionFst = (unionFst != null)
							//union further recursion paths
							? union(unionFst, permute(newPrefixFst, partFsts, connector))
							//First loop iteration(first element of union)
							: permute(newPrefixFst, partFsts, connector);
				}
			}
			return unionFst;
		}else{
			//end recursion (last concatenation)
			return (prefixFst != null)
					? concatenate(prefixFst.shallowCopy(), fsts.get(0).shallowCopy())
					: fsts.get(0).shallowCopy(); //only one item, no permutation
		}
	}

    /** Returns an FST that is concatenation of two FSTs */
	public static Fst concatenate(Fst a, Fst b) {
		for (State state : a.getFinalStates()) {
			state.isFinal = false;
			state.simulateEpsilonTransitionTo(b.initialState);
		}
		a.updateStates();
        return a;
	}

	/** Returns an FST that is a union of two FSTs */
	public static Fst union(Fst a, Fst b) {
		State s = new State();
		s.simulateEpsilonTransitionTo(a.initialState);
		s.simulateEpsilonTransitionTo(b.initialState);
		a.initialState = s;
		a.updateStates();
		return a;
	}

	/** Returns an FST that accepts a kleene star of a given FST */
	public static Fst kleene(Fst a) {
		State s = new State();
		s.isFinal = true;
		s.simulateEpsilonTransitionTo(a.initialState);
		for (State p : a.getFinalStates())
			p.simulateEpsilonTransitionTo(s);
		a.initialState = s;
		a.updateStates();
		return a;
	}

	/** Returns an FST that accepts a kleene plus of a given FST */
	public static Fst plus(Fst a) {
		// return concatenate(n, kleene(n));
		for (State s : a.getFinalStates()) {
			s.simulateEpsilonTransitionTo(a.initialState);
		}
		a.updateStates();
		return a;
	}

	/** Returns an FST that accepts zero or one of a given NFA */
	public static Fst optional(Fst a) {
		State s = new State();
		s.simulateEpsilonTransitionTo(a.initialState);
		s.isFinal = true;
		a.initialState = s;
		a.updateStates();
		return a;
	}

	public static Fst repeatExactly(Fst a, int n) {
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
				state.simulateEpsilonTransitionTo(fstList[i].initialState);
			}
			a.updateStates();
		}
		return a;
	}
	
	public static Fst repeatMin(Fst a, int min) {
		Fst aPlus = plus(a.shallowCopy());
		Fst aMax = repeatExactly(a.shallowCopy(), min - 1);
		return concatenate(aMax, aPlus);
	}

	public static Fst repeatMinMax(Fst a, int min, int max) {
		if(min == max) {
			return repeatExactly(a, min);
		}
		max -= min;
		assert max>=0;
        if (max==0) return new Fst(true);
        Fst fst;
		if (min == 0) {
			fst = new Fst(true);
		} else if (min == 1) {
			fst = a.shallowCopy();
		} else {
			fst = repeatExactly(a.shallowCopy(), min);
		}
		if (max > 0) {
			Fst aa = a.shallowCopy();
			while (--max > 0) {
				Fst ab = a.shallowCopy();
				for (State state : ab.getFinalStates()) {
					state.simulateEpsilonTransitionTo(aa.initialState);
				}
				aa = ab;
			}
			for (State state : fst.getFinalStates()) {
				state.simulateEpsilonTransitionTo(aa.initialState);
			}
		}
		fst.updateStates();
		return fst;
	}
	
	// Minimizes a FST along the lines of Brzozowski's algorithm
	public static void minimize(Fst fst) {
		// reverse and determinize
		List<State> initialStates = fst.reverse(false);
		partiallyDeterminize(fst, initialStates);

		// reverse back and determinize
		initialStates = fst.reverse(false);
		partiallyDeterminize(fst, initialStates);

		fst.optimize();
	}
	
	public static List<State> reverse(Fst fst) {
		return reverse(fst, true);
	}

	//TODO: handle fst annotations (remove annotations before reversing?)
	public static List<State> reverse(Fst fst, boolean createNewInitialState) {
	
		Int2ObjectMap<ArrayList<Transition>> reversedIncomingTransitionsOfState = new Int2ObjectOpenHashMap<>();

		// Handle reverse FST for two-pass
		if(!fst.initialState.isFinal)
			reversedIncomingTransitionsOfState.put(fst.initialState.id, new ArrayList<>());
		else {
			for(State state : fst.getFinalStates()) {
				reversedIncomingTransitionsOfState.put(state.id, new ArrayList<>());
			}
		}

		for (State fromState : fst.states) {
			for (Transition transition : fromState.transitionList) {
				ArrayList<Transition> reversedIncomingTransitionsOfToState
						= reversedIncomingTransitionsOfState.get(transition.toState.id);
				if (reversedIncomingTransitionsOfToState == null) {
					reversedIncomingTransitionsOfToState = new ArrayList<>();
					reversedIncomingTransitionsOfState.put(transition.toState.id, reversedIncomingTransitionsOfToState);
				}
				Transition reversedTransition = transition.shallowCopy();
				reversedTransition.setToState(fromState);
				reversedIncomingTransitionsOfToState.add(reversedTransition);
			}
		}

		// Update states with reverse transtitions
		List<State> newInitialStates = new ArrayList<>(); // will be old final states
		for (State s : fst.states) {
			s.transitionList = reversedIncomingTransitionsOfState.get(s.id);
			if (s.transitionList==null)
				s.transitionList = new ArrayList<>();
			/*if(s.isFinal) {
				newInitialStates.add(s);
				s.isFinal = false;
			}*/
		}

		// Handle reverse FST for two-pass
		if(!fst.initialState.isFinal) {
			fst.initialState.isFinal = true;
			for (State state : fst.getFinalStates()) {
				state.isFinal = false;
				newInitialStates.add(state);
			}
		}
		else {
			fst.initialState.isFinal = false;
			for(State state : fst.getFinalStates()) {
				state.isFinal = true;
				newInitialStates.add(state);
			}
		}

		if (createNewInitialState) {
			// If we want one initial state
			if(newInitialStates.size() > 1) {
				fst.initialState = new State();
				for (State state : newInitialStates) {
					fst.initialState.simulateEpsilonTransitionTo(state);
				}
			} else {
				fst.initialState = newInitialStates.get(0);
			}
			fst.updateStates();
			newInitialStates.clear();
			newInitialStates.add(fst.initialState);
		}

		fst.optimize();
		return newInitialStates;
	}
	
	
	public static void partiallyDeterminize(Fst fst) {
		List<State> initialStates = new ArrayList<>();
		initialStates.add(fst.initialState);
		partiallyDeterminize(fst, initialStates);
	}

	//TODO: handle fst annotations (remove annotations before determinizing?)
	public static void partiallyDeterminize(Fst fst, List<State> initialStates) {
		fst.initialState = new State();
		
		IntSet initialStateIds = new IntOpenHashSet();
		for(State s : initialStates) {
			initialStateIds.add(s.id);
		}

		// Maps old states to new state
		Map<IntSet, State> newStateForStateIdSet = new HashMap<>();
		newStateForStateIdSet.put(initialStateIds, fst.initialState);
		
		//Maps transitions(input-output label) to toState ids
		Map<Transition, IntSet> toStateIdSetForTransition = new HashMap<>();
		
		//Unprocessed pFstStates (cFstStateIds)
		LinkedList<IntSet> unprocessedStateIdSets = new LinkedList<>();
		unprocessedStateIdSets.add(initialStateIds);
		
		//Processed pFstStates (cFstStateIds)
		Set<IntSet> processedStateIdSets = new HashSet<>();
		
		while(unprocessedStateIdSets.size() > 0) {
			//Process a pFststate (cFstStateIds)
			IntSet stateIdSet = unprocessedStateIdSets.removeFirst();
			boolean isFinal = false;
			
			
			if(!processedStateIdSets.contains(stateIdSet)) {
				State newState = newStateForStateIdSet.get(stateIdSet);
				toStateIdSetForTransition.clear();
				
				//for (input-output label, toStateId) pairs
				for(int stateId : stateIdSet) {
					State state = fst.getState(stateId);
					if(state.isFinal)
						isFinal = true;
					for(Transition t : state.transitionList) {
						IntSet reachableStateIds = toStateIdSetForTransition.get(t);
						if(reachableStateIds == null) {
							reachableStateIds = new IntOpenHashSet();
							toStateIdSetForTransition.put(t, reachableStateIds);
						}
						reachableStateIds.add(t.toState.id);
					}
				}
				
				// Add pFst transition and new state
				for(Transition transition : toStateIdSetForTransition.keySet()) {
					IntSet reachableStateIds = toStateIdSetForTransition.get(transition);
					if(!processedStateIdSets.contains(reachableStateIds)) {
						unprocessedStateIdSets.add(reachableStateIds);
					}
					
					State newToState = newStateForStateIdSet.get(reachableStateIds);
					if(newToState == null) {
						newToState = new State();
						newStateForStateIdSet.put(reachableStateIds, newToState);
					}
					
					Transition newTransition = transition.shallowCopy();
					newTransition.setToState(newToState);
					newState.addTransition(newTransition);
				}
			}
			
			// Mark as processed
			processedStateIdSets.add(stateIdSet);
			
			//Final state?
			if(isFinal)
				newStateForStateIdSet.get(stateIdSet).isFinal = true;
			
			
		}
		fst.updateStates();
	}
	
}
