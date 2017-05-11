package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.fst.graphviz.AutomatonVisualizer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.commons.io.FilenameUtils;

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
	Map<BitSet, DfaState> states = new HashMap<>();

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

	/** Exports the DFA using graphviz (type bsed on extension, e.g., "gv" (source file), "pdf", ...). Works for eager
	 * and lazy DFAs.
	 *
	 * The initial state has number 1; all other state numbers are assigned arbitrarily. The method tries to assign
	 * useful edge labels. Each edge is labeled with a conjunction of item expressions and is taken by an item if
	 * it matches every one of those item expressions. Unlabeled edges mean "all other items".
	 *
	 * If the output file is a PDF document, may take a very (very very) long time.
	 */
	public void exportGraphViz(String file) {
		// number the states
		Object2IntOpenHashMap<DfaState> numOf = new Object2IntOpenHashMap<>();
		numOf.put(initial, 1);
		int i = 1;
		for (DfaState s : states.values()) {
			if (!numOf.containsKey(s)) {
				numOf.put(s, ++i);
			}
		}

		AutomatonVisualizer automatonVisualizer = new AutomatonVisualizer(
				FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));

		automatonVisualizer.beginGraph();
		for (DfaState s : states.values()) {
			for (i=0; i<s.reachableDfaStates.size(); i++) {
				DfaState t = s.reachableDfaStates.get(i);
				String label;
				if (i==0) {
					if (t==null) continue;
					label = ""; // unlabeled edges mean "all other"
				} else {
					label = "";
					String sep = "";
					BitSet firedTransitions = s.firedTransitionsByIndex.get(i);
					for (int j = firedTransitions.nextSetBit(0); j >= 0; j = firedTransitions.nextSetBit(j+1)) {
						label = label + sep + s.transitionLabels[j];
						sep = ";";
					}
				}
				automatonVisualizer.add(String.valueOf(numOf.getInt(s)), label, String.valueOf(numOf.getInt(t)));
			}
			if(s.isFinal)
				automatonVisualizer.addFinalState(String.valueOf(numOf.getInt(s)));
			if(s.isFinalComplete)
				automatonVisualizer.addFinalState(String.valueOf(numOf.getInt(s)), true);
		}
		automatonVisualizer.endGraph(String.valueOf(numOf.getInt(initial)));
	}

}
