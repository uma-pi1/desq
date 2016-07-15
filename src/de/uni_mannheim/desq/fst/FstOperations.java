package de.uni_mannheim.desq.fst;


import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;



public final class FstOperations {

	private FstOperations() {
	};

	/** Returns an FST that is concatenation of two FSTs */
	public static Fst concatenate(Fst a, Fst b) {
		for (State state : a.getFinalStates()) {
			state.isFinal = false;
			state.addEpsilonTransition(b.initialState);
		}
		return a;
	}

	/** Returns an FST that is a union of two FSTs */
	public static Fst union(Fst a, Fst b) {
		State s = new State();
		s.addEpsilonTransition(a.initialState);
		s.addEpsilonTransition(b.initialState);
		a.initialState = s;
		return a;
	}

	/** Returns an FST that accepts a kleene star of a given FST */
	public static Fst kleene(Fst a) {
		State s = new State();
		s.isFinal = true;
		s.addEpsilonTransition(a.initialState);
		for (State p : a.getFinalStates())
			p.addEpsilonTransition(s);
		a.initialState = s;
		return a;
	}

	/** Returns an FST that accepts a kleene plus of a given FST */
	public static Fst plus(Fst a) {
		// return concatenate(n, kleene(n));
		for (State s : a.getFinalStates()) {
			s.addEpsilonTransition(a.initialState);
		}
		return a;
	}

	/** Returns an FST that accepts zero or one of a given NFA */
	public static Fst optional(Fst a) {
		State s = new State();
		s.addEpsilonTransition(a.initialState);
		s.isFinal = true;
		a.initialState = s;
		return a;
	}

	//TODO: 
	public static Fst repeatMax(Fst a, int max) {
		/*if (max == 0) {
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
		return a;*/
		return null;
	}
	
	//TODO:
	public static Fst repeatMin(Fst a, int min) {
		/*Fst aPlus = plus(a.clone());
		Fst aMax = repeatMax(a.clone(), min - 1);
		return concatenate(aMax, aPlus);*/
		return null;
	}
	
	//TODO:
	public static Fst repeatMinMax(Fst a, int min, int max) {
		/*max -= min;
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
		return fst;*/
		return null;
	}
	
	
}
