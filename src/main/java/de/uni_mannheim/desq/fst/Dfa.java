package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.util.IntSetUtils;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.*;


/** A DFA corresponding (and linked to) an {@link Fst}. */
public final class Dfa {
	/** The initial state. */
	protected DfaState initial;

	/** Maps a set of FST states (as given in the bitset) to a DfaState of this DFA, if present. */
	protected Map<BitSet, DfaState> states = new HashMap<BitSet, DfaState>();

	/** Creates a DFA for the given FST. The DFA accepts each input for which the FST has an accepting run
	 * with all output items <= largestFrequentItemFid. */
	public static Dfa createDfa(Fst fst, Dictionary dict, int largestFrequentItemFid,
								boolean processFinalCompleteStates) {
		return create(fst, dict, largestFrequentItemFid, false, processFinalCompleteStates);
	}

	/** Creates a reverse DFA for the given FST and modifies the FST for efficient use in Desq's two-pass
	 * algorithms. The DFA accepts each reversed input for which the (unmodified) FST has an accepting
	 * run with all output items <= largestFrequentItemFid. Note that the modified FST should not be used
	 * directly anymore, but only in conjunction with {@link #acceptsReverse(IntList, List, IntList)}. */
	public static Dfa createReverseDfa(Fst fst, Dictionary dict, int largestFrequentItemFid,
									   boolean processFinalCompleteStates) {
		return create(fst, dict, largestFrequentItemFid, true, processFinalCompleteStates);
	}

	private static Dfa create(Fst fst, Dictionary dict, int largestFrequentItemFid, boolean reverse,
							  boolean processFinalCompleteStates) {
		// compute the initial states
		BitSet initialStateIdSet = new BitSet(fst.numStates());
		if(reverse) { // create a DFA for the reverse FST (original FST is destroyed)
			fst.dropAnnotations();
			List<State> initialStates = fst.reverse(false);
			for(State s : initialStates) {
				initialStateIdSet.set(s.id);
			}
			fst.annotate();
			fst.dropCompleteFinalTransitions();
		} else { // don't reverse, original FST remains unmodified
			initialStateIdSet.set(fst.getInitialState().getId());
		}

		// construct the DFA
		Dfa dfa = new Dfa();
		dfa.initial = new EagerDfaState(initialStateIdSet, fst);
		dfa.constructEager(initialStateIdSet, fst, dict, largestFrequentItemFid, processFinalCompleteStates);

		// when we are reversing, reverse back the FST to get an optimized new FST
		if (reverse) {
			fst.dropAnnotations();
			fst.reverse(false);
			fst.annotate();
		}

		return dfa;
	}

	/** Construct an extended DFA from the given FST */
	private void constructEager(BitSet initialStateIdSet, Fst fst, Dictionary dict, int largestFrequentItemFid,
								boolean processFinalCompleteStates) {
		states.clear();

		// unprocessed dfa states
		Stack<BitSet> unprocessedToStates = new Stack<>();

		// map from transition label to items that fire (used as cache)
		Map<String, IntList> firedItemsFor = new HashMap<>();

		// map from transition label to reachable FST states (for currently processed DFA state)
		Map<String, BitSet> toStatesFor = new HashMap<>();

		// fst states reachable by all items (for currently processed DFA state)
		BitSet defaultTransition = new BitSet(fst.numStates());

		// for all items that are not covered by the defaultTransition above, the set of reachable fst states
		// (for currently processed DFA state)
		BitSet activeTransitions = new BitSet(dict.lastFid()+1); // indexed by item
		BitSet[] transitions = new BitSet[dict.lastFid()+1]; // indexed by item
		for (int i=0; i<transitions.length; i++) {
			transitions[i] = new BitSet(fst.numStates());
		}

		// we start with the initial state
		states.put(initialStateIdSet, initial);
		unprocessedToStates.push(initialStateIdSet);

		// while there is an unprocessed state, compute all its transitions
next:	while (!unprocessedToStates.isEmpty()) {
			// get next state
			BitSet fromStates = unprocessedToStates.pop();
			EagerDfaState fromDfaState = (EagerDfaState)states.get(fromStates);

			// System.out.println("Processing " + fromStates.toString());

			// if a states is final complete, we may stop
			if (!processFinalCompleteStates && fromDfaState.isFinalComplete()) {
				fromDfaState.freeze();
				continue next;
			}


			// iterate over all relevant transitions and compute reachable FST states per transition label encounterd
			// if we see a label that we haven't seen before, we also compute the set of items that fire the transition
			defaultTransition.clear();
			toStatesFor.clear();
			for (int stateId = fromStates.nextSetBit(0);
				 stateId >= 0;
				 stateId = fromStates.nextSetBit(stateId+1)) {

				// ignore outgoing transitions from final complete states
				State state = fst.getState(stateId);
				if (!state.isFinalComplete()) {
					for (Transition t : state.getTransitions()) {
						if (t.firesAll(largestFrequentItemFid)) {
							// this is an optmization which often helps when the pattern expression contains .
							defaultTransition.set(t.getToState().getId());
						} else {
							// otherwise we remember the transition
							String label = t.toPatternExpression();
							BitSet toStates = toStatesFor.get(label);
							if (toStates == null) {
								toStates = new BitSet(fst.numStates());
								toStatesFor.put(label, toStates);
							}
							toStates.set(t.getToState().getId());

							// if it was a new label, compute the fired items
							if (!firedItemsFor.containsKey(label)) {
								IntArrayList firedItems = new IntArrayList(dict.lastFid()+1);
								IntIterator it = t.matchedFidIterator();
								while (it.hasNext()) {
									int fid = it.nextInt();
									boolean matches = t.hasOutput()
											? t.matchesWithFrequentOutput(fid, largestFrequentItemFid) : true;
									if (matches) {
										firedItems.add(fid);
									}
								}
								firedItems.trim();
								firedItemsFor.put(label, firedItems);
								// System.out.println(label + " fires for " + firedItems.size() + " items");
							}
						}
					}
				}
			}

			// now set the default transition in case there where any . transitions
			if (!defaultTransition.isEmpty()) {
				EagerDfaState toDfaState = (EagerDfaState)states.get(defaultTransition);
				if (toDfaState == null) {
					BitSet toStates = IntSetUtils.copyOf(defaultTransition);
					toDfaState = new EagerDfaState(toStates, fst);
					states.put(toStates, toDfaState);
					unprocessedToStates.add(toStates);
				}
				fromDfaState.setDefaultTransition(toDfaState);
			}

			// now compute for each item not covered by the default transition the set of fst states that can be reached
			activeTransitions.clear();
			for (Map.Entry<String,BitSet> entry : toStatesFor.entrySet()) {
				String label = entry.getKey();
				BitSet toStatesToAdd = entry.getValue();
				IntList firedItems = firedItemsFor.get(label);
				for (int i=0; i<firedItems.size(); i++) {
					int fid = firedItems.get(i);
					if (!activeTransitions.get(fid)) {
						// activate and initialize fid if not yet seen
						activeTransitions.set(fid);
						transitions[fid].clear();
						transitions[fid].or(defaultTransition);
					}

					// add the states we can reach with this fid
					transitions[fid].or(toStatesToAdd);
				}
			}

			// finally, iterate over those fids and add transitions to the DFA
			fromDfaState.transitions = new Int2ObjectOpenHashMap<>(activeTransitions.cardinality());
			for (int fid=activeTransitions.nextSetBit(0); fid>=0; fid=activeTransitions.nextSetBit(fid+1)) {
				BitSet toStates = transitions[fid];
				EagerDfaState toDfaState = (EagerDfaState)states.get(toStates);
				if (toDfaState == null) {
					toStates = IntSetUtils.copyOf(toStates);
					toDfaState = new EagerDfaState(toStates, fst);
					states.put(toStates, toDfaState);
					unprocessedToStates.add(toStates);
				}
				fromDfaState.setTransition(fid, toDfaState);
			}
		}

		// freeze all states (allows them to optimize)
		for (DfaState state : states.values()) {
			state.freeze();
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
	 * @param initialPos positions from which the FST (modified by {@link #createReverseDfa(Fst, Dictionary, int)}) needs
	 *                   to be started to find all accepting runs.
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
