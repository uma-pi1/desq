package de.uni_mannheim.desq.fst;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;


public final class State {
	int id;
	// List of transitions
	ArrayList<Transition> transitionList;
	boolean isFinal;
	boolean isFinalComplete = false;

	
	public State() {
		this(false);
	}
	
	
	public State(boolean isFinal) {
		this.transitionList = new ArrayList<>();
		this.isFinal = isFinal;
	}
	
	public int getId(){
		return id;
	}
	
	public void setId(int id){
		this.id = id;
	}
	
	public void addTransition(Transition t) {
		transitionList.add(t);
	}
	
	public void simulateEpsilonTransitionTo(State to) {
		if (to.isFinal)
			isFinal = true;
		for (Transition t : to.transitionList) {
			transitionList.add(t);
		}
	}
	
	public static final class ItemStateIterator implements Iterator<ItemState> {
		private ArrayList<Transition> transitions;
		private int nextTransitionIndex;
		private Iterator<ItemState> currentIt;
		private boolean currentItHasNext;
		private BitSet validToStates;
		private int fid;

		public boolean hasNext() {
			if (!currentItHasNext) {
				moveToNextTransition();
			}
			return currentItHasNext;
		}

		public ItemState next() {
			ItemState result = currentIt.next();
			currentItHasNext = currentIt.hasNext();
			return result;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}

		void moveToNextTransition() {
			if (validToStates == null) {
				do {
					if (nextTransitionIndex >= transitions.size()) {
						currentItHasNext = false;
						return;
					}
					Transition nextTransition = transitions.get(nextTransitionIndex++);
					currentIt = nextTransition.consume(fid, currentIt);
					currentItHasNext = currentIt.hasNext();
				} while (!currentItHasNext);
			} else { // validToStates != null
				currentItHasNext = false;
				do {
					if (nextTransitionIndex >= transitions.size()) {
						return;
					}
					Transition nextTransition = transitions.get( nextTransitionIndex++ );
					if (validToStates.get(nextTransition.toState.getId())) {
						currentIt = nextTransition.consume(fid, currentIt);
						currentItHasNext = currentIt.hasNext();
					}
				} while (!currentItHasNext);
			}

		}
	}
	
	public ItemStateIterator consume(int itemFid) {
		return consume(itemFid, null, null);
	}
	
	public ItemStateIterator consume(int itemFid, ItemStateIterator it) {
		return consume(itemFid, it, null);
	}

	/** Returns an iterator over (output item, next state)-pairs consistent with the given input item. Only
	 * produces pairs for which the next state is contained in validToStates (BitSet indexed by state ids).
	 *
	 * If the output item is epsilon, returns (0, next state) pair.
	 *
	 * @param fid input item
	 * @param it iterator to reuse
	 * @param validToStates set of next states to consider
	 *
	 * @return an iterator over (output item fid, next state) pairs
	 */
	public ItemStateIterator consume(int fid, ItemStateIterator it, BitSet validToStates) {
		if (it == null) {
			it = new ItemStateIterator();
		}

		it.transitions = transitionList;
		it.nextTransitionIndex = 0;
		it.validToStates = validToStates;
		it.fid = fid;
		it.moveToNextTransition();

		return it;
	}

	public boolean isFinal() { 
		return isFinal; 
	}

	public boolean isFinalComplete() {
		return isFinalComplete;
	}
	
	public List<Transition> getTransitions() {
		return transitionList;
	}
	
	private static class StateIterator implements Iterator<State> {
		Iterator<Transition> transitionsIt;
		Transition transition;
		int fid;
		State toState;
		BitSet toStatesOutput = new BitSet(); // to states already output
		@Override
		public boolean hasNext() {
			while(transitionsIt.hasNext()) {
				transition = transitionsIt.next();
				if(!toStatesOutput.get(transition.toState.id) && transition.matches(fid)) { // returns false if toStatesOutput is to small
					toState = transition.toState; 
					toStatesOutput.set(toState.id); // automatically resizes upwards if necessary
					return true;
				}
			}
			toState = null;
			return false;
		}

		@Override
		public State next() {
			return toState;
		}
		
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
	
	public Iterator<State> toStateIterator(int itemFid) {
		return toStateIterator(itemFid, null);
	}
	
	public Iterator<State> toStateIterator(int itemFid, Iterator<State> it) {
		StateIterator resultIt ;
		if(it != null && it instanceof StateIterator) {
			resultIt = (StateIterator) it;
			resultIt.toStatesOutput.clear();
		} else {
			resultIt = new StateIterator();
		}
		
		resultIt.transitionsIt = transitionList.iterator();
		resultIt.fid = itemFid;
		resultIt.toState = null;
		return resultIt;
	}

	public void removeTransition(Transition t) {
		//TODO:swap with the last element and delete the last element
		int i = 0;
		for(; i < transitionList.size(); i++) {
			if(t == transitionList.get(i))
				break;
		}
		transitionList.remove(i);
	}

	/** Remove all outgoing transitions from this state */
	public void removeAllTransitions() {
		transitionList.clear();
	}
}
