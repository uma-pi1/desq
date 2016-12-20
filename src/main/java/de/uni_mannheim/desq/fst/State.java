package de.uni_mannheim.desq.fst;

import java.util.*;
import java.util.stream.Collectors;


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
		BasicTransition tr = (BasicTransition) t;
		tr.addFromState(this);
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
		private final Transition.ItemStateIteratorCache itCache;

		public ItemStateIterator(boolean isForest) {
			 itCache = new Transition.ItemStateIteratorCache(isForest);
		}

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
					currentIt = nextTransition.consume(fid, itCache);
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
						currentIt = nextTransition.consume(fid, itCache);
						currentItHasNext = currentIt.hasNext();
					}
				} while (!currentItHasNext);
			}
		}
	}

	private static final class CompressedTransitionIterator implements Iterator<Transition> {
		Iterator<Transition> transitionsIt;
		//Iterator<ItemState> currentIt;
		Transition nextTransition;
		BitSet validToStates;
		int fid;
		boolean isNew;

		@Override
		public boolean hasNext() {
			do {
				nextTransition = nextTransition();
				if(nextTransition == null) {
					return false;
				}
			} while(!nextTransition.matches(fid));
			return true;
			/*
			if (currentIt == null || isNew) {
				Transition nextTransition = nextTransition();
				if (nextTransition != null) {
					currentIt = nextTransition.consume(fid, currentIt);
					isNew = false;
				} else {
					return false;
				}
			}
			while (!currentIt.hasNext()) {
				Transition nextTransition = nextTransition();
				if (nextTransition != null) {
					currentIt = nextTransition.consume(fid, currentIt);
				} else {
					return false;
				}
			}
			return true;*/
		}

		@Override
		public Transition next() {
			return nextTransition;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		private Transition nextTransition() {
			if (validToStates==null) {
				if (transitionsIt.hasNext()) {
					return transitionsIt.next();
				} else {
					return null;
				}
			} else {
				while (transitionsIt.hasNext()) {
					Transition nextTransition = transitionsIt.next();
					if (validToStates.get(nextTransition.toState.getId())) {
						return nextTransition;
					}
				}
				return null;
			}
		}
	}
	
	public ItemStateIterator newIterator(boolean isForest) {
		return new ItemStateIterator(isForest);
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
		it.transitions = transitionList;
		it.nextTransitionIndex = 0;
		it.validToStates = validToStates;
		it.fid = fid;
		it.moveToNextTransition();
		return it;
	}


	@Deprecated
	public Iterator<Transition> consumeCompressed(int itemFid) {
		return consumeCompressed(itemFid, null, null);
	}

	@Deprecated
	public Iterator<Transition> consumeCompressed(int itemFid, Iterator<Transition> it) {
		return consumeCompressed(itemFid, it, null);
	}

	/** Returns an iterator over (output item, next state)-pairs consistent with the given input item. Only
	 * produces pairs for which the next state is contained in validToStates (BitSet indexed by state ids).
	 *
	 * If the output item is epsilon, returns (0, next state) pair.
	 *
	 * @param itemFid input item
	 * @param it iterator to reuse
	 * @param validToStates set of next states to consider
	 *
	 * @return an iterator over (output item fid, next state) pairs
	 */
	public Iterator<Transition> consumeCompressed(int itemFid, Iterator<Transition> it, BitSet validToStates) {
		CompressedTransitionIterator resultIt;
		if(it != null && it instanceof CompressedTransitionIterator)
			resultIt = (CompressedTransitionIterator)it;
		else
			resultIt = new CompressedTransitionIterator();

		resultIt.transitionsIt = transitionList.iterator();
		resultIt.validToStates = validToStates;
		resultIt.fid = itemFid;
		resultIt.isNew = true;

		return resultIt;
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

	public List<BasicTransition> getSortedBasicTransitions() {
		// quick & easy. to be improved
		List<BasicTransition> basicTransitionList = transitionList.stream().map(e -> (BasicTransition) e).collect(Collectors.toList());
		basicTransitionList.sort(Comparator.comparing((BasicTransition t)->t.outputLabelType)
			.thenComparing(t->t.outputLabel)
			.thenComparing(t->t.inputLabelType)
			.thenComparing(t->t.inputLabel));
		// now that we have sorted them, store them in this order
		transitionList = basicTransitionList.stream().map(e -> (Transition) e).collect(Collectors.toList());
		return basicTransitionList;
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


	
	public StateIterator toStateIterator(int itemFid) {
		return toStateIterator(itemFid, null);
	}
	
	public StateIterator toStateIterator(int itemFid, StateIterator it) {
		if(it != null) {
			it.toStatesOutput.clear();
		} else {
			it = new StateIterator();
		}
		
		it.transitionsIt = transitionList.iterator();
		it.fid = itemFid;
		it.toState = null;
		return it;
	}

	/** Remove all outgoing transitions from this state */
	public void removeAllTransitions() {
		transitionList.clear();
	}
}
