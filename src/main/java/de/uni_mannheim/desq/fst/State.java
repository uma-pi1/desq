package de.uni_mannheim.desq.fst;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class State {
	
	int id;
	// List of transitions
	List<Transition> transitionSet;
	boolean isFinal;
	
	public State() {
		this(false);
	}
	
	
	public State(boolean isFinal) {
		this.transitionSet = new ArrayList<Transition>();
		this.isFinal = isFinal;
	}
	
	
	public int getId(){
		return id;
	}
	
	public void setId(int id){
		this.id = id;
	}
	
	public void addTransition(Transition t) {
		transitionSet.add(t);
	}
	
	public void simulateEpsilonTransition(State to) {
		if (to.isFinal)
			isFinal = true;
		for (Transition t : to.transitionSet) {
			if(to == t.toState) {//if self loop
				t.toState = this;
			}
			transitionSet.add(t);
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
			return currentIt.hasNext();
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
		
		resultIt.transitionsIt = transitionSet.iterator();
		resultIt.fid = itemFid;
		resultIt.isNew = true;

		return resultIt;
	}
	
	public boolean isFinal() { 
		return isFinal; 
	}
}
