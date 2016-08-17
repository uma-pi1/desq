package de.uni_mannheim.desq.fst;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class State {
	
	int id;
	// List of transitions
	List<Transition> transitionList;
	boolean isFinal;
	
	public State() {
		this(false);
	}
	
	
	public State(boolean isFinal) {
		this.transitionList = new ArrayList<Transition>();
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
	
	private static class TransitionIterator implements Iterator<ItemState> {
		Iterator<Transition> transitionsIt;
		Iterator<ItemState> currentIt;
		int fid;
        boolean isNew;

		@Override
		public boolean hasNext() {
			if (currentIt == null || isNew) {
				if (transitionsIt.hasNext()) {
					currentIt = transitionsIt.next().consume(fid, currentIt);
                    isNew = false;
				} else {
					return false;
				}
			}
			while (!currentIt.hasNext()) {
				if (transitionsIt.hasNext()) {
					currentIt = transitionsIt.next().consume(fid, currentIt);
				} else {
					return false;
				}
			}
			return true;
		}

		@Override
		public ItemState next() {
			return currentIt.next();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
	
	public Iterator<ItemState> consume(int itemFid) {
		return consume(itemFid, null);
	}
	
	public Iterator<ItemState> consume(int itemFid, Iterator<ItemState> it) {
		TransitionIterator resultIt = null;
		if(it != null && it instanceof TransitionIterator)
			resultIt = (TransitionIterator)it;
		else
			resultIt = new TransitionIterator();
		
		resultIt.transitionsIt = transitionList.iterator();
		resultIt.fid = itemFid;
		resultIt.isNew = true;

		return resultIt;
	}
	
	public boolean isFinal() { 
		return isFinal; 
	}
	
	public List<Transition> getTransitions() {
		return transitionList;
	}
	
	private static class StateIterator implements Iterator<State> {
		Iterator<Transition> transitionsIt;
		Transition transition;
		int fid;
		State toState;
		IntSet toStatesSet = new IntOpenHashSet();
		@Override
		public boolean hasNext() {
			while(transitionsIt.hasNext()) {
				transition = transitionsIt.next();
				if(transition.matches(fid) &&  !toStatesSet.contains(transition.toState.id)) {
					toState = transition.toState; 
					toStatesSet.add(toState.id);
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
		StateIterator resultIt = null;
		if(it != null && it instanceof StateIterator) 
			resultIt = (StateIterator) it;
		else
			resultIt = new StateIterator();
		
		resultIt.transitionsIt = transitionList.iterator();
		resultIt.fid = itemFid;
		resultIt.toStatesSet.clear();
		resultIt.toState = null;
		return resultIt;
	}
}
