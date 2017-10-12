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
	public static Fst permute(List<Fst> fsts, HashMap<Fst,int[]> frequencies) {
		//handle frequencies
		int concatinatorSize = 0;
		Fst concatenator = null;
		for (Map.Entry<Fst,int[]> entry: frequencies.entrySet()){
			if(fsts.contains(entry.getKey())){
				if(entry.getValue().length == 1) { //only min -> find all occurrences (kleene *) in all combinations (use as concatenator)
					concatenator = (concatenator != null)
							? concatenate(concatenator, kleene(entry.getKey().shallowCopy()))
							: kleene(entry.getKey().shallowCopy());
					concatinatorSize++;
					int min = entry.getValue()[0];
					if (min == 0){
						//entry in concatenator is sufficient
						fsts.remove(entry.getKey());
					}else if (min > 1){
						fsts.addAll(addExactly(entry.getKey(),min-1));
					}
				}else if (entry.getValue().length == 2){
					int min = entry.getValue()[0];
					int max = entry.getValue()[1];
					if(max >= min){
						int dif = max - min;
						//Remove existing entry if min = 0 (will be replaced with optionals)
						if(min < 1) fsts.remove(entry.getKey());
						//Fill up min with mandatory matches
						else if(min > 1) fsts.addAll(addExactly(entry.getKey(),min-1));
						//Difference between min and max represented with optionals
						if(dif > 0) fsts.addAll(addExactly(
								optional(entry.getKey().shallowCopy()),
								min-1));
					}
				}
			}
		}
		//Ensure that concatenator is optional and can repeat itself -> kleene *
		if(concatenator != null && concatinatorSize > 1){
			concatenator = kleene(concatenator);
		}

		//start recursion
		Fst permuted = (fsts.size() > 0) ? permute(null, fsts, concatenator) : null;

		//add concatenator at beginning and end as well (if defined)
		if (concatenator != null) {
			concatenator.exportGraphViz("concatenator_raw.pdf");
			if(permuted != null) {
				permuted = concatenate(concatenator.shallowCopy(), permuted);
				permuted = concatenate(permuted, concatenator.shallowCopy());
			}else{
				//If nothing to permute (only concatenor left) -> just return concatenator
				permuted = concatenator;
			}
		}
		permuted.exportGraphViz("permuted_raw.pdf");

		//optimize permutation
		//permuted.optimize();
		//permuted.updateStates();
		//permuted.exportGraphViz("permuted_optimized.pdf");
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
	private static Fst permute(Fst prefixFst, List<Fst> fsts, Fst concatenator) {
		assert fsts.size() > 0;
		if(fsts.size() > 1){
			Fst unionFst = null;
			for (Fst fst: fsts){
				//create copy of to be added fst (avoid wrong state transitions)
				Fst addedFst = fst.shallowCopy();
				//add concatenator (eg "[A.*]*.*")to Fst (A.*B.* -> A.*[A.*]*B.*)
				if (concatenator != null) addedFst = concatenate(addedFst, concatenator.shallowCopy());
				//copy remaining fsts into list (as shallow copy)
				List<Fst> partFsts = new ArrayList<>();
				for (Fst copy: fsts){
					if (copy != fst){ partFsts.add(copy.shallowCopy());	}
				}
				//handle new prefix
				Fst newPrefixFst = null;
				if(prefixFst != null) {
					//Within recursion: concatenate elements (prefix + new first element)
					newPrefixFst = concatenate(prefixFst.shallowCopy(), addedFst);
				}else{
					//First Recursion Step (no existing prefix yet)
					newPrefixFst = addedFst;
				}
				//recursions combined by union
				if(unionFst != null) {
					//union further recursion paths
					unionFst = union(unionFst, permute(newPrefixFst, partFsts, concatenator));
				}else{
					//First loop iteration(first element of union)
					unionFst = permute(newPrefixFst, partFsts, concatenator);
				}
			}
			unionFst.updateStates();
			return unionFst;
		}else{
			//end recursion (last concatenation)
			if(prefixFst != null) {
				Fst lastConcat = concatenate(prefixFst.shallowCopy(), fsts.get(0).shallowCopy());
				lastConcat.updateStates();
				return lastConcat;
			}else{
				return fsts.get(0).shallowCopy();
			}
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
