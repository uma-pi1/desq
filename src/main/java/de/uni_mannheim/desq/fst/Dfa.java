package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.Dictionary;
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
	public static Dfa createDfa(Fst fst, Dictionary dict, int largestFrequentItemFid) {
		return create(fst, dict, largestFrequentItemFid, false);
	}

	/** Creates a reverse DFA for the given FST and modifies the FST for efficient use in Desq's two-pass
	 * algorithms. The DFA accepts each reversed input for which the (unmodified) FST has an accepting
	 * run with all output items <= largestFrequentItemFid. Note that the modified FST should not be used
	 * directly anymore, but only in conjunction with {@link #acceptsReverse(IntList, List, IntList)}. */
	public static Dfa createReverseDfa(Fst fst, Dictionary dict, int largestFrequentItemFid) {
		return create(fst, dict, largestFrequentItemFid, true);
	}

	private static Dfa create(Fst fst, Dictionary dict, int largestFrequentItemFid, boolean reverse) {
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
		dfa.initial = new EagerDfaState(initialStateIdSet, fst, dict.size());
		dfa.constructEager(initialStateIdSet, fst, dict, largestFrequentItemFid);

		// when we are reversing, reverse back the FST to get an optimized new FST
		if (reverse) {
			fst.dropAnnotations();
			fst.reverse(false);
			fst.annotate();
		}

		return dfa;
	}

	/** Construct an extended DFA from the given FST */
	private void constructEager(BitSet initialStateIdSet, Fst fst, Dictionary dict, int largestFrequentItemFid) {
		states.clear();

		// Unprocessed dfa states
		Stack<BitSet> unprocessedStateIdSets = new Stack<>();

		// processed dfa states
		Set<BitSet> processedStateIdSets = new HashSet<>();

		Map<BitSet, IntList> reachableStatesForItemIds = new HashMap<>();

		// Initialize conversion
		states.put(initialStateIdSet, initial);
		unprocessedStateIdSets.push(initialStateIdSet);

		while(!unprocessedStateIdSets.isEmpty()) {
			// process fst states
			BitSet stateIdSet = unprocessedStateIdSets.pop();

			if(!processedStateIdSets.contains(stateIdSet)) {
				reachableStatesForItemIds.clear();
				EagerDfaState fromDfaState = (EagerDfaState)states.get(stateIdSet);

				// for all items, for all transitions
				IntIterator intIt = dict.fids().iterator();
				while (intIt.hasNext()) {
					int itemFid = intIt.nextInt();
					// compute reachable states for this item
					BitSet reachableStateIds = new BitSet(fst.numStates());

					for (int stateId = stateIdSet.nextSetBit(0);
						 stateId >= 0;
						 stateId = stateIdSet.nextSetBit(stateId+1)) {

						// ignore outgoing transitions from final complete states
						State state = fst.getState(stateId);
						if (!state.isFinalComplete()) {
							for (Transition t : state.getTransitions()) {
								boolean matches = t.hasOutput()
										? t.matchesWithFrequentOutput(itemFid, largestFrequentItemFid) : t.matches(itemFid);
								if (matches) {
									reachableStateIds.set(t.getToState().getId());
								}
							}
						}
					}
					if (!reachableStateIds.isEmpty()) {
						IntList itemIds = reachableStatesForItemIds.get(reachableStateIds);
						if (itemIds == null) {
							itemIds = new IntArrayList();
							reachableStatesForItemIds.put(reachableStateIds, itemIds);
						}
						itemIds.add(itemFid);
					}
				}

				for(Map.Entry<BitSet, IntList> entry : reachableStatesForItemIds.entrySet()) {
					BitSet reachableStateIds = entry.getKey();
					IntList itemFids = entry.getValue();

					//check if we already processed these reachableStateIds
					if(!processedStateIdSets.contains(reachableStateIds))
						unprocessedStateIdSets.push(reachableStateIds);

					//create new extended dfa state if required
					EagerDfaState toDfaState = (EagerDfaState)states.get(reachableStateIds);
					if(toDfaState == null) {
						toDfaState = new EagerDfaState(reachableStateIds, fst, dict.size());
						states.put(reachableStateIds, toDfaState);
					}

					for(int itemFid : itemFids) {
						// add to dfa transition table
						fromDfaState.setTransition(itemFid, toDfaState);
					}

				}
				processedStateIdSets.add(stateIdSet);
			}
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
}
