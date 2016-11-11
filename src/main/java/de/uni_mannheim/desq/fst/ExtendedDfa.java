package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.Item;
import it.unimi.dsi.fastutil.ints.*;

import java.util.*;


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
		this.initialDfaState = new ExtendedDfaState(fst.getInitialState().getId(), fst);
		this.constructExtendedDfa();
	}

	/** Construct an extended-DFA from a given FST */
	private void constructExtendedDfa() {
		// Map old states to new state
		Map<IntSet, ExtendedDfaState> newStateForStateIdSet = new HashMap<>();
		
		// Map from item to reachable states
		Int2ObjectMap<IntSet> reachableStatesFromItemId = new Int2ObjectOpenHashMap<>(); 
		
		// Unprocessed edfa states
		Stack<IntSet> unprocessedStateIdSets = new Stack<>();
		
		// processed edfa states
		Set<IntSet> processedStateIdSets = new HashSet<>();
		
		//helper
		List<Transition> transitionList = new ArrayList<>();
		
		// Initialize conversion
		IntSet initialStateIdSet = IntSets.singleton(fst.getInitialState().getId());
		newStateForStateIdSet.put(initialStateIdSet, initialDfaState);
		unprocessedStateIdSets.push(initialStateIdSet);

		while(!unprocessedStateIdSets.isEmpty()) {
			// process fst states
			IntSet stateIdSet = unprocessedStateIdSets.pop();

			if(!processedStateIdSets.contains(stateIdSet)) {
				ExtendedDfaState fromEDfaState = newStateForStateIdSet.get(stateIdSet);
				
				reachableStatesFromItemId.clear();

				//TODO: optimize
				transitionList.clear();
				for(int stateId : stateIdSet) {
					// ignore outgoing transitions from final complete states
					if(!fst.getState(stateId).isFinalComplete())
						transitionList.addAll(fst.getState(stateId).getTransitions());
				}
				
				// for all items, for all transitions
				for(Item item : dict.getItems()) {
                    int itemFid = item.fid;
					IntSet reachableStateIds = new IntOpenHashSet();
					for(Transition t : transitionList) {
						if(t.matches(itemFid)) {
							reachableStateIds.add(t.getToState().getId());
						}
					}
					reachableStatesFromItemId.put(itemFid, reachableStateIds);
				}

				Iterator it = reachableStatesFromItemId.entrySet().iterator();
				while(it.hasNext()) {
					Int2ObjectMap.Entry<IntSet> entry = (Int2ObjectMap.Entry<IntSet>) it.next();
					int itemFid = entry.getIntKey();
					IntSet reachableStateIds = entry.getValue();

					//check if we already processed these reachableStateIds
					if(!processedStateIdSets.containsAll(reachableStateIds))
						unprocessedStateIdSets.push(reachableStateIds);

					//create new extended dfa state if required
					ExtendedDfaState toEDfaState = newStateForStateIdSet.get(reachableStateIds);
					if(toEDfaState == null) {
						toEDfaState = new ExtendedDfaState(reachableStateIds, fst);
						newStateForStateIdSet.put(reachableStateIds, toEDfaState);
					}

					// add to dfa transition table
					fromEDfaState.addToTransitionTable(itemFid, toEDfaState);
				}
			}
			processedStateIdSets.add(stateIdSet);
		}
		reachableStatesFromItemId.clear();
		processedStateIdSets.clear();
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
