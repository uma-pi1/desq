package de.uni_mannheim.desq.journal.edfa;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.Item;
import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.Transition;


/**
 * ExtendedDfa.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class ExtendedDfa {
	

	//TODO: probably not needed
	//List<ExtendedDfaState> states = new ArrayList<>();
	Fst fst;
	Dictionary dict;
	
	// an index of eDfa stateIds for fst stateIds
	ExtendedDfaState[] eDfaStateIdForFstStateId;
	
	public ExtendedDfa(Fst fst, Dictionary dict) {
		this.fst = fst;
		this.dict = dict;
		this.eDfaStateIdForFstStateId = new ExtendedDfaState[fst.numStates()];
		for(int fstStateId = 0; fstStateId < fst.numStates(); fstStateId++) {
			this.eDfaStateIdForFstStateId[fstStateId] = new ExtendedDfaState();
		}
		this.constructExtendedDfa(fst);
	}
	
	/** Construct an extended-DFA from a given FST */
	private void constructExtendedDfa(Fst fst) {
		// Map old states to new state
		Map<IntSet, ExtendedDfaState> newStateForStateIdSet = new HashMap<>();
		
		// Map from item to reachable states
		Int2ObjectMap<IntSet> reachableStatesFromItemId = new Int2ObjectOpenHashMap<>(); 
		
		// Map reachable states to transitions
		Map<IntSet, BitSet> incTransitionToStates = new HashMap<>();
		
		// Unprocessed edfa states
		Stack<IntSet> unprocessedStateIdSets = new Stack<>();
		
		// processed edfa states
		Set<IntSet> processedStateIdSets = new HashSet<>();
		
		//helper
		List<Transition> transitionList = new ArrayList<>();
		
		
		// Initialize newStateForStateIdSet
		// Initially old states contain all fst state as potential initial states
		for(int fstStateId = 0; fstStateId < fst.numStates(); fstStateId++) {
			//ExtendedDfaState eDfaState = new ExtendedDfaState();
			IntSet initialStateIdSet = createIntSet(fstStateId); 
			newStateForStateIdSet.put(initialStateIdSet, eDfaStateIdForFstStateId[fstStateId]);
			//states.add(eDfaState);
			
			// add to unprocessed states
			unprocessedStateIdSets.push(initialStateIdSet);
			
			
		}
		
		while(!unprocessedStateIdSets.isEmpty()) {
			// process fst states
			IntSet stateIdSet = unprocessedStateIdSets.pop();
			boolean isFinal = false;
			
			if(!processedStateIdSets.contains(stateIdSet)) {
				ExtendedDfaState fromEDfaState = newStateForStateIdSet.get(stateIdSet);
				
				reachableStatesFromItemId.clear();
				incTransitionToStates.clear();
				
				//TODO: optimize
				transitionList.clear();
				for(int stateId : stateIdSet) {
					transitionList.addAll(fst.getState(stateId).getTransitions());
					if(fst.getState(stateId).isFinal())
						isFinal = true;
				}
				
				//for all items, for all transitions
				for(Item item : dict.allItems()) {
					int itemFid = item.fid;
					
					for(Transition t : transitionList){
						if(t.matches(itemFid)) {
							IntSet reachableStates = reachableStatesFromItemId.get(itemFid);
							if(reachableStates == null) {
								reachableStates = new IntOpenHashSet();
								reachableStatesFromItemId.put(itemFid, reachableStates);
							}
							reachableStates.add(t.getToState().getId());
						}
					}
				}
				
				for(int itemFid : reachableStatesFromItemId.keySet()) {
					IntSet reachableStates = reachableStatesFromItemId.get(itemFid);
					BitSet eDfaTransition = incTransitionToStates.get(reachableStates);
					if(eDfaTransition == null) {
						eDfaTransition = new BitSet(dict.allItems().size() + 1);
						incTransitionToStates.put(reachableStates, eDfaTransition);
					}
					eDfaTransition.set(itemFid);
				}
				
				// Add transitions to extendedDfa
				for(IntSet reachableStateIds : incTransitionToStates.keySet()) {
					//check if we already processed it
					if(!processedStateIdSets.containsAll(reachableStateIds)) {
						unprocessedStateIdSets.add(reachableStateIds);
					}
					
					ExtendedDfaState toEDfaState = newStateForStateIdSet.get(reachableStateIds); 
					if(toEDfaState == null) {
						toEDfaState = new ExtendedDfaState();
						newStateForStateIdSet.put(reachableStateIds, toEDfaState);
						//states.add(toEDfaState);
					}
					
					ExtendedDfaTransition eDfaTransition = 
							new ExtendedDfaTransition(incTransitionToStates.get(reachableStateIds), toEDfaState);
					fromEDfaState.addTransition(eDfaTransition);
				}
			}
			processedStateIdSets.add(stateIdSet);
			
			if(isFinal) {
				newStateForStateIdSet.get(stateIdSet).isFinal = true;
			}
		}
		reachableStatesFromItemId.clear();
		incTransitionToStates.clear();
		processedStateIdSets.clear();
		//finalizeEDfa();
	}
	
	/*private void finalizeEDfa() {
		for(ExtendedDfaState state : states) {
			state.id = states.size();
		}
	}*/
	
	/*private List<Transition> getTransitions(IntSet stateIdSet) {
		List<Transition> transitionList = new ArrayList<>();
		for(int stateId : stateIdSet) {
			transitionList.addAll(fst.getState(stateId).getTransitions());
		}
		return transitionList;
	}*/
	
	
	private IntSet createIntSet(int id) {
		IntSet initialStateIds = new IntOpenHashSet(1);
		initialStateIds.add(id);
		return initialStateIds;
	}

	
	/**
	 * Returns true if the fst snapshot is relevant, i.e., leads to a final state
	 * otherwise returns false
	 */
	public boolean isRelevant(IntList inputSequence, int position, int fstStateId) {
		ExtendedDfaState state = eDfaStateIdForFstStateId[fstStateId];
		while(position < inputSequence.size()) {
			state = state.consume(inputSequence.getInt(position++));
			if(state.isFinal)
				return true;
		}
		return false;
	}
	
	
	public static void main(String[] args) {
		//ExtendedDfa eDfa = new ExtendedDfa();
	}

}
