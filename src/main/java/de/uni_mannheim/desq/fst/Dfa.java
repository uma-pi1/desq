package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.Dictionary;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/** A DFA corresponding (and linked to) an {@link Fst}. */
public final class Dfa {
	/** The FST used when creating this DFA. */
	Fst fst = null;
	Dictionary dict;
	int largestFrequentItemFid;
	boolean processFinalCompleteStates;

	// map from set of transition labels (=key) to some DFA state for these transitions (used to avoid duplicate computations)
	// whenever two DFA states have the same set of outgoing transition transition labels (ignoring where they go
	// and how often), we share indexByFid between those states
	Map<String, DfaState> stateByTransitions = new HashMap<>();

	/** The initial state. */
	DfaState initial;

	/** Maps a set of FST states (as given in the bitset) to a DfaState of this DFA, if present. */
	Map<BitSet, DfaState> states = new HashMap<BitSet, DfaState>();

	Dfa(Dictionary dict, int largestFrequentItemFid, boolean processFinalCompleteStates) {
		this.dict = dict;
		this.largestFrequentItemFid = largestFrequentItemFid;
		this.processFinalCompleteStates = processFinalCompleteStates;
	}

	/** Creates a DFA for the given FST. The DFA accepts each input for which the FST has an accepting run
	 * with all output items <= largestFrequentItemFid. */
	public static Dfa createDfa(Fst fst, Dictionary dict, int largestFrequentItemFid,
								boolean processFinalCompleteStates, boolean useLazyDfa) {
		Dfa dfa = new Dfa(dict, largestFrequentItemFid, processFinalCompleteStates);
		dfa.create(fst, false, useLazyDfa);
		return dfa;
	}

	/** Creates a reverse DFA for the given FST and modifies the FST for efficient use in Desq's two-pass
	 * algorithms. The DFA accepts each reversed input for which the (unmodified) FST has an accepting
	 * run with all output items <= largestFrequentItemFid. Note that the modified FST should not be used
	 * directly anymore, but only in conjunction with {@link #acceptsReverse(IntList, List, IntList)}. */
	public static Dfa createReverseDfa(Fst fst, Dictionary dict, int largestFrequentItemFid,
									   boolean processFinalCompleteStates, boolean useLazyDfa) {
		Dfa dfa = new Dfa(dict, largestFrequentItemFid, processFinalCompleteStates);
		dfa.create(fst, true, useLazyDfa);
		return dfa;
	}

	private void create(Fst fst, boolean reverse, boolean useLazyDfa) {
		// compute the initial states
		BitSet initialStates = new BitSet(fst.numStates());
		if(reverse) { // create a DFA for the reverse FST (original FST is destroyed)
			fst.dropAnnotations();
			List<State> initialStatesList = fst.reverse(false);
			for(State s : initialStatesList) {
				initialStates.set(s.id);
			}
			fst.annotate();
			fst.dropCompleteFinalTransitions();
			this.fst = fst.shallowCopy(); // because we "unreverse" below but want to keep the reversed one here
		} else { // don't reverse, original FST remains unmodified
			initialStates.set(fst.getInitialState().getId());
			this.fst = fst;
		}

		// construct the DFA
		DfaState initial = useLazyDfa ? new LazyDfaState(this, initialStates)
				: new EagerDfaState(this, initialStates);
		initial.construct();

		// when we are reversing, reverse back the FST to get an optimized new FST
		if (reverse) {
			// does not affect the FST stored within this DFA
			fst.dropAnnotations();
			fst.reverse(false);
			fst.annotate();
		}
	}

	/** Returns true if the input sequence is relevant (DFA accepts). */
	public boolean accepts(IntList inputSequence) {
		DfaState state = initial;
		int pos = 0;
		while(pos < inputSequence.size()) {
			state = state.consume(inputSequence.getInt(pos++));
			// In this case is ok to return false, if there was a final state before
			// we already retured true, final state can not be reached if state
			// was null
			if(state == null)
				return false;
			if(state.isFinalComplete())
				return true;
		}
		return state.isFinal(); // last state; pos == inputSequence.size()
	}

	/** Returns true if the reverse input sequence is relevant (DFA accepts reverse input).
	 *
	 * The method reads the input sequence backwards and records the sequence of states being traversed.
	 *
	 * @param inputSequence the input sequence
	 * @param stateSeq sequence of DFA states being traversed on the reversed sequence, i.e.,
	 *                    <code>stateSeq[inputSequence.size() - (pos+1)] = state before consuming inputSequence[pos]</code>
	 * @param initialPos positions from which the FST (as modified by
	 * 					{@link #createReverseDfa(Fst, Dictionary, int, boolean, boolean)}) needs
	 *                   to be started to find all accepting runs. Empty if the FST does not accept. Should be empty
	 *                   initially.
	 */
	public boolean acceptsReverse(IntList inputSequence, List<DfaState> stateSeq, IntList initialPos) {
		DfaState state = initial;
		stateSeq.add(state);
		int pos = inputSequence.size();
		while(pos > 0) {
			state = state.consume(inputSequence.getInt(--pos));
			if(state == null)
				break; // we may return true or false, as we might have reached a final state before
			stateSeq.add(state);
			if(state.isFinalComplete() || (state.isFinal() && pos == 0)) {
				initialPos.add(pos);
			}
		}
		return (!initialPos.isEmpty());
	}

	public int numStates() {
		return states.size();
	}
}
