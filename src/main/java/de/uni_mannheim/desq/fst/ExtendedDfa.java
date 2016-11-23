package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.Dictionary;
import it.unimi.dsi.fastutil.ints.*;

import java.util.*;


/**
 * ExtendedDfa.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public final class ExtendedDfa {
	// initial state for ExtendedDfa
	ExtendedDfaState initialDfaState;

	public ExtendedDfa(Fst fst, Dictionary dict) {
		this(fst,dict,false);
	}

	public ExtendedDfa(Fst fst, Dictionary dict, boolean reverse) {
        IntSet dfaInitialState;
        IntSet dfaStickyStates;
        if (!reverse) {
			dfaInitialState = IntSets.singleton( fst.getInitialState().getId() );
			dfaStickyStates = IntSets.EMPTY_SET;
		} else {
        	fst = fst.shallowCopy(); // don't change provided fst
			List<State> initialStates = fst.reverse(false);
            dfaInitialState = new IntOpenHashSet();
			dfaStickyStates = new IntOpenHashSet();
            for (State s : initialStates) {
                dfaInitialState.add(s.id);
                if (s.isFinalComplete()) {
					dfaStickyStates.add(s.id);
				}
            }
			fst.annotateFinalStates();
		}
        this.initialDfaState = new ExtendedDfaState(dfaInitialState, fst, dict.size());
        this.constructExtendedDfa(fst, dict, dfaInitialState, dfaStickyStates);
	}

	/** Construct an extended-DFA from a given FST */
	private void constructExtendedDfa(Fst fst, Dictionary dict, IntSet initialStateIdSet, IntSet stickyStateIdSet) {
		// Map old states to new state
		Map<IntSet, ExtendedDfaState> newStateForStateIdSet = new HashMap<>();

		// Unprocessed edfa states
		Stack<IntSet> unprocessedStateIdSets = new Stack<>();

		// processed edfa states
		Set<IntSet> processedStateIdSets = new HashSet<>();

		Map<IntSet, IntList> reachableStatesForItemIds = new HashMap<>();

		// Initialize conversion
		newStateForStateIdSet.put(initialStateIdSet, initialDfaState);
		unprocessedStateIdSets.push(initialStateIdSet);

		while(!unprocessedStateIdSets.isEmpty()) {
			// process fst states
			IntSet stateIdSet = unprocessedStateIdSets.pop();

			if(!processedStateIdSets.contains(stateIdSet)) {

				reachableStatesForItemIds.clear();
				ExtendedDfaState fromEDfaState = newStateForStateIdSet.get(stateIdSet);

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
								// TODO: check that if transition produces output, it can produce a frequent item
								if (t.matches(itemFid)) {
									reachableStateIds.add(t.getToState().getId());
								}
							}
						}
					}
					reachableStateIds.addAll(stickyStateIdSet); // sticky initial states
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
					ExtendedDfaState toEDfaState = newStateForStateIdSet.get(reachableStateIds);
					if(toEDfaState == null) {
						toEDfaState = new ExtendedDfaState(reachableStateIds, fst, dict.size());
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

	/**
	 * Returns true if the reverse input sequence is relevant
	 */
	public boolean isRelevantReverse(IntList inputSequence) {
		ExtendedDfaState state = initialDfaState;
		int pos = inputSequence.size();
		while (pos > 0) {
			state = state.consume(inputSequence.getInt(--pos));
			if(state == null) {
				// we return false because we can't consume the current input and haven't seen a final-complete
				// state before
				return false;
			}
			if (state.isFinalComplete()) {
				return true;
			}
		}
		return state.isFinal(); // all read and final state reached
	}

	/**
	 * Returns true if the reverse input sequence is relevant
	 *
	 * This method, reads the input sequence backwards and also adds
	 * the given list with the sequence of states being visited before
	 * consuming each item + initial one,
	 * i.e., stateSeq[inputSequence.size() - (pos+1)] = state before consuming inputSequence[pos]
     * The method also adds the given list with initial positions
     * from which FST can be simulated
	 */
    public boolean isRelevantReverse(IntList inputSequence, List<ExtendedDfaState> stateSeq, IntList initialPos) {
        ExtendedDfaState state = initialDfaState;
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
