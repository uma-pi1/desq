package de.uni_mannheim.desq.fst;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


 
public class State {
	
	int stateId;
	// set of transitions
	Set<Transition> transitionSet;
	boolean isFinal;
	
	public State() {
		this(false);
	}
	
	
	public State(boolean isFinal) {
		this.transitionSet = new HashSet<Transition>();
		this.isFinal = isFinal;
	}
	
	
	public int getStateId(){
		return stateId;
	}
	
	public void setStateId(int stateId){
		this.stateId = stateId;
	}
	
	public void addTransition(Transition t) {
		transitionSet.add(t);
	}
	
	public void addEpsilonTransition(State to) {
		if (to.isFinal)
			isFinal = true;
		for (Transition t : to.transitionSet) {
			transitionSet.add(t);
		}
	}
	
	private class TransitionIterator implements Iterator<Transition> {
		Iterator<Transition> it;
		
		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public Transition next() {
			return it.next();
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub
		}
	}
	
	public Iterator<Transition> consume(int item) {
		return consume(item, null);
	}
	
	public Iterator<Transition> consume(int item, Iterator<Transition> it) {
		TransitionIterator it2 = null;
		if(it != null && it instanceof TransitionIterator)
			it2 = (TransitionIterator)it;
		else
			it2 = new TransitionIterator();
		
		it2.it = transitionSet.iterator();
		
		return it2;
	}
	
	public boolean isFinal() { 
		return isFinal; 
	}
}
