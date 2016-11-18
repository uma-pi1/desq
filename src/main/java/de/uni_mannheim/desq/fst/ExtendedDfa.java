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

	// helpers
	Fst fst;
	Dictionary dict;


	public ExtendedDfa(Fst fst, Dictionary dict) {
		this(fst,dict,false);
	}

	public ExtendedDfa(Fst fst, Dictionary dict, boolean reverse) {
        this.fst = fst;
        this.dict = dict;
        IntSet initialStateIdSet;
		// We reverse is true, we create a DFA for the reverse FST
		if(reverse) {
            List<State> initialStates = fst.reverse(false);
            initialStateIdSet = new IntOpenHashSet();
            for(State s : initialStates) {
                initialStateIdSet.add(s.id);
            }
            fst.annotateFinalStates();
		} else {
            initialStateIdSet = IntSets.singleton(fst.getInitialState().getId());
		}
        this.initialDfaState = new ExtendedDfaState(initialStateIdSet, fst, dict.size());
        this.constructExtendedDfa(initialStateIdSet);
	}

	/** Construct an extended-DFA from a given FST */
	private void constructExtendedDfa(IntSet initialStateIdSet) {
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
	 * Returns true if the input sequence is relevant
	 */
	public boolean isRelevant(IntList inputSequence) {
		ExtendedDfaState eDfaState = initialDfaState;
		int pos = 0;
		while(pos < inputSequence.size()) {
			eDfaState = eDfaState.consume(inputSequence.getInt(pos++));
			// In this case is ok to return false, if there was a final state before
			// we already retured true, final state can not be reached if state 
			// was null
			if(eDfaState == null)
				return false;
			if(eDfaState.isFinalComplete())
				return true;
		}
		return eDfaState.isFinal(); //pos == inputSequence.size()
	}

	/**
	 * Returns true if the input sequence is relevant
	 *
	 * This method, reads the input sequence backwards and also adds
	 * the given list with the sequence of states being visited before
	 * consuming each item + initial one,
	 * i.e., stateSeq[inputSequence.size() - (pos+1)] = state before consuming inputSequence[pos]
     * The method also adds the given list with initial positions
     * from which FST can be simulated
	 */
    public boolean isRelevant(IntList inputSequence, List<ExtendedDfaState> stateSeq, IntList initialPos) {
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
