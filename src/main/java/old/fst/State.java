package old.fst;

import java.util.HashSet;
import java.util.Set;


/**
 * State.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */

public class State{

	public static int next_id;
	
	int id;
	
	Set<Transition> transitions;

	boolean accept;

	State() {
		this(false);
	}

    State(boolean accept) {
		this.transitions = new HashSet<>();
		this.accept = accept;
		id = next_id++;
	}

	public void addTransition(Transition t) {
		transitions.add(t);
	}

	public void addTransitions(Set<Transition> ts) {
		for (Transition t : ts) {
			transitions.add(t);
		}
	}
	
	public Set<Transition> getTransitions() {
		return transitions;
	}

	public void addEpsilon(State to) {
		if (to.accept)
			accept = true;
		for (Transition t : to.transitions) {
			transitions.add(t);
		}
	}

		
	public boolean equals(Object obj) {
		State u = (State) obj;

		return u.id == id;
	}

	/*@Override
	public int hashCode() {
		return gid;
	}*/

}
