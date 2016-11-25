package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.Dictionary;
import it.unimi.dsi.fastutil.ints.*;

import java.util.*;


/** A DFA corresponding (and linked to) an {@link Fst}. */
public final class Dfa {
	/** The initial state */
	private DfaState initial;

	private Dfa(Fst fst, Dictionary dict, int largestFrequentItemFid, boolean reverse) {
		IntSet initialStateIdSet;
		if(reverse) { // create a DFA for the reverse FST (original FST is destroyed)
			fst.dropAnnotations();
			List<State> initialStates = fst.reverse(false);
			initialStateIdSet = new IntOpenHashSet();
			for(State s : initialStates) {
				initialStateIdSet.add(s.id);
			}
			fst.annotate();
			fst.dropCompleteFinalTransitions();
		} else { // don't reverse, original FST remains unmodified
			initialStateIdSet = IntSets.singleton(fst.getInitialState().getId());
		}

		// construct the DFA
		this.initial = new DfaState(initialStateIdSet, fst, dict.size());
		this.constructDfa(initialStateIdSet, fst, dict, largestFrequentItemFid);

		// when we are reversing, reverse back the FST to get an optimized new FST
		if (reverse) {
			fst.dropAnnotations();
			fst.reverse(false);
			fst.annotate();
		}
	}

	/** Creates a DFA for the given FST. The DFA accepts each input for which the FST has an accepting run
	 * with all output items <= largestFrequentItemFid. */
	public static Dfa createDfa(Fst fst, Dictionary dict, int largestFrequentItemFid) {
		return new Dfa(fst, dict, largestFrequentItemFid, false);
	}

	/** Creates a reverse DFA for the given FST and modifies the FST for efficient use in Desq's two-pass
	 * algorithms. The DFA accepts each reversed input for which the (unmodified) FST has an accepting
	 * run with all output items <= largestFrequentItemFid. Note that the modified FST should not be used
	 * directly anymore, but only in conjunction with {@link #acceptsReverse(IntList, List, IntList)}. */
	public static Dfa createReverseDfa(Fst fst, Dictionary dict, int largestFrequentItemFid) {
		return new Dfa(fst, dict, largestFrequentItemFid, true);
	}

	/** Construct an extended DFA from the given FST */
	private void constructDfa(IntSet initialStateIdSet, Fst fst, Dictionary dict, int largestFrequentItemFid) {
		// Map old states to new state
		Map<IntSet, DfaState> newStateForStateIdSet = new HashMap<>();

		// Unprocessed edfa states
		Stack<IntSet> unprocessedStateIdSets = new Stack<>();

		// processed edfa states
		Set<IntSet> processedStateIdSets = new HashSet<>();

		Map<IntSet, IntList> reachableStatesForItemIds = new HashMap<>();

		// Initialize conversion
		newStateForStateIdSet.put(initialStateIdSet, initial);
		unprocessedStateIdSets.push(initialStateIdSet);

		while(!unprocessedStateIdSets.isEmpty()) {
			// process fst states
			IntSet stateIdSet = unprocessedStateIdSets.pop();

			if(!processedStateIdSets.contains(stateIdSet)) {

				reachableStatesForItemIds.clear();
				DfaState fromEDfaState = newStateForStateIdSet.get(stateIdSet);

				// for all items, for all transitions
				IntIterator intIt = dict.fids().iterator();
				while (intIt.hasNext()) {
					int itemFid = intIt.nextInt();
					// compute reachable states for this item
					IntSet reachableStateIds = new IntOpenHashSet();

					for (int stateId : stateIdSet) {
						// ignore outgoing transitions from final complete states
						State state = fst.getState(stateId);
						if (!state.isFinalComplete()) {
							for (Transition t : state.getTransitions()) {
								boolean matches = t.hasOutput()
										? t.matchesWithFrequentOutput(itemFid, largestFrequentItemFid) : t.matches(itemFid);
								if (matches) {
									reachableStateIds.add(t.getToState().getId());
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

				for(Map.Entry<IntSet, IntList> entry : reachableStatesForItemIds.entrySet()) {
					IntSet reachableStateIds = entry.getKey();
					IntList itemFids = entry.getValue();

					//check if we already processed these reachableStateIds
					if(!processedStateIdSets.contains(reachableStateIds))
						unprocessedStateIdSets.push(reachableStateIds);

					//create new extended dfa state if required
					DfaState toEDfaState = newStateForStateIdSet.get(reachableStateIds);
					if(toEDfaState == null) {
						toEDfaState = new DfaState(reachableStateIds, fst, dict.size());
						newStateForStateIdSet.put(reachableStateIds, toEDfaState);
					}

					for(int itemFid : itemFids) {
						// add to dfa transition table
						fromEDfaState.addToTransitionTable(itemFid, toEDfaState);
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
