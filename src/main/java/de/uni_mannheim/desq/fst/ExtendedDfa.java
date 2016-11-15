package de.uni_mannheim.desq.fst;

import com.google.common.base.Stopwatch;
import de.uni_mannheim.desq.dictionary.Dictionary;
import it.unimi.dsi.fastutil.ints.*;

import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * ExtendedDfa.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public final class ExtendedDfa {

	ExtendedDfaState initialDfaState;

	Fst fst;
	Dictionary dict;

	public ExtendedDfa(Fst fst, Dictionary dict) {
		this.fst = fst;
		this.dict = dict;
		this.initialDfaState = new ExtendedDfaState(fst.getInitialState().getId(), fst, dict.size());
		this.constructExtendedDfa();
	}

	/** Construct an extended-DFA from a given FST */
	private void constructExtendedDfa() {
		// Map old states to new state
		Map<IntSet, ExtendedDfaState> newStateForStateIdSet = new HashMap<>();

		// Unprocessed edfa states
		Stack<IntSet> unprocessedStateIdSets = new Stack<>();

		// processed edfa states
		Set<IntSet> processedStateIdSets = new HashSet<>();

		Map<IntSet, IntSet> reachableStatesForItemIds = new HashMap<>();

		// Initialize conversion
		IntSet initialStateIdSet = IntSets.singleton(fst.getInitialState().getId());
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
								if (t.matches(itemFid)) {
									reachableStateIds.add(t.getToState().getId());
								}
							}
						}
					}
					if (!reachableStateIds.isEmpty()) {
						IntSet itemIds = reachableStatesForItemIds.get(reachableStateIds);
						if (itemIds == null) {
							itemIds = new IntOpenHashSet();
							reachableStatesForItemIds.put(reachableStateIds, itemIds);
						}
						itemIds.add(itemFid);
					}
				}
				for(Map.Entry<IntSet, IntSet> entry : reachableStatesForItemIds.entrySet()) {
					IntSet reachableStateIds = entry.getKey();
					IntSet itemFids = entry.getValue();

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
		newStateForStateIdSet = null;
		unprocessedStateIdSets = null;
		processedStateIdSets = null;
	}


	/**
	 * Returns true if the input sequence is relevant
	 */
	public boolean isRelevant(IntList inputSequence) {
		ExtendedDfaState state = initialDfaState;
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
		return state.isFinal(); //pos == inputSequence.size()
	}



	/**
	 * Returns true if the input sequence is relevant
	 *
	 * Also adds to the given list with the sequence of states being visited before consuming each item + final one
	 * i.e., stateSeq[pos] = state before consuming inputSequence[pos]
	 */
	public boolean isRelevant(IntList inputSequence, List<ExtendedDfaState> stateSeq, IntList finalPos) {
		ExtendedDfaState state = initialDfaState;
		stateSeq.add(state);
		int pos = 0;
		while(pos < inputSequence.size()) {
			state = state.consume(inputSequence.getInt(pos++));
			if(state == null)
				break; // we may return true or false, as we might have reached a final state before
			stateSeq.add(state);
			if(state.isFinalComplete() || (state.isFinal() && pos == inputSequence.size())) {
				finalPos.add(pos);
			}
		}
		return (!finalPos.isEmpty());
	}
}
