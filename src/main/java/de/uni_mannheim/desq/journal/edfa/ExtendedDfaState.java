package de.uni_mannheim.desq.journal.edfa;

import java.util.ArrayList;
import java.util.List;

/**
 * ExtendedDfaState.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class ExtendedDfaState {
	
	// parameters
	//int id;
	List<ExtendedDfaTransition> transitionList;
	boolean isFinal;
	
	public ExtendedDfaState() {
		this(false);
	}
	
	public ExtendedDfaState(boolean isFinal) {
		this.transitionList = new ArrayList<ExtendedDfaTransition>();
		this.isFinal = isFinal;
	}
	
	public void addTransition(ExtendedDfaTransition t) {
		transitionList.add(t);
	}
	
	/*public int consume(int itemFid) {
		for(ExtendedDfaTransition t : transitionList) {
			if(t.matches(itemFid))
				return t.getToStateId();
		}
		return -1;
	}*/
	
	public ExtendedDfaState consume(int itemFid) {
		for(ExtendedDfaTransition t : transitionList) {
			if(t.matches(itemFid))
				return t.getToState();
		}
		return null;
	}
}
