package de.uni_mannheim.desq.fst;


import com.google.common.base.Strings;
import de.uni_mannheim.desq.fst.graphviz.FstVisualizer;
import org.apache.commons.io.FilenameUtils;

import java.io.PrintStream;
import java.util.*;


 
public final class Fst {
	// initial state
	State initialState;
	
	// list of states; initialized only after state numbers are updated; see updateStates()
	List<State> states = new ArrayList<>();
    List<State> finalStates = new ArrayList<>();

	// flag indicating whether full input must be consumed
	boolean requireFullMatch = false;

    public Fst() {
        this(false);
    }

	public Fst(boolean isFinal) {
		initialState = new State();
        initialState.isFinal = isFinal;
        updateStates();
	}

	public boolean getRequireFullMatch() {
		return requireFullMatch;
	}

	public void setRequireFullMatch(boolean requireFullMatch) {
		this.requireFullMatch = requireFullMatch;
	}

	public State getInitialState() {
		return initialState;
	}
	
	public void setInitialState(State state) {
		this.initialState = state;
	}
	
	
	public Collection<State> getFinalStates() {
		return finalStates;
	}
	
	/** Recomputes state and final state list. Must be called whenever any state in the FST is modified. */
	public void updateStates() {
		states.clear();
		finalStates.clear();
		int number = 0;
		Set<State> visited = new HashSet<>();
		LinkedList<State> worklist = new LinkedList<>(); // TODO: make stack
		worklist.add(initialState);
		
		while (worklist.size() > 0) {
			State s = worklist.removeFirst();
			if (!visited.contains(s)) {
				s.setId(number++);
				states.add(s);
				if (s.isFinal) finalStates.add(s);
				for (Transition t : s.transitionList) {
					worklist.add(t.toState);
				}
				visited.add(s);
			}
		}
	}
	
	public State getState(int stateId) {
		return states.get(stateId);
	}
	
	public List<State> getStates() {
		return states;
	}
	
	public int numStates() {
		assert states != null;
		return states.size();
	}
	
	// returns a copy of FST with shallow copy of its transitions
	public Fst shallowCopy() {
		Fst fstCopy = new Fst();
		Map<State, State> stateMap = new HashMap<>();
		for(State state : states) {
			stateMap.put(state, new State());
		}
		for(State state : states) {
			State stateCopy = stateMap.get(state);
			stateCopy.isFinal = state.isFinal;
			fstCopy.states.add(stateCopy);
			if (state.isFinal) fstCopy.finalStates.add(stateCopy);
			if(state == initialState) {
				fstCopy.initialState = stateCopy;
			}
			for (Transition t : state.transitionList) {
				Transition tCopy = t.shallowCopy();
				tCopy.setToState(stateMap.get(t.toState));
				stateCopy.addTransition(tCopy);
			}
		}
		return fstCopy;
	}

	public void print() {
		print(System.out, 2);
	}

	public void print(PrintStream out, int indent) {
		String indentString = Strings.repeat(" ", indent);

		out.print(indentString);
		out.print("States : ");
		out.print(numStates());
		out.print(" states, initial=");
		out.print(getInitialState().id);
		out.print(", final=[");
		String separator = "";
		for (State state : getFinalStates()) {
			out.print(separator);
			out.print(state.id);
			separator = ",";
		}
		out.println("]");

		out.print(indentString);
		out.print("Trans. : ");
		String newIndent = "";
		for (State state : getStates()) {
			out.print(newIndent);
			newIndent = "         " + indentString;

			out.print(state.id);
			out.print(": ");

			separator = "";
			for (Transition t : state.getTransitions()) {
				out.print(separator);
				out.print(t.toString());
				separator = ", ";
			}
			out.println();
		}
	}
	
	/** Exports the fst using graphviz (type bsed on extension, e.g., "gv" (source file), "pdf", ...) */
	public void exportGraphViz(String file) {
		FstVisualizer fstVisualizer = new FstVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
		fstVisualizer.beginGraph();
		for(State s : states) {
			for(Transition t : s.transitionList)
                fstVisualizer.add(String.valueOf(s.id), t.labelString(), String.valueOf(t.getToState().id));
			if(s.isFinal)
				fstVisualizer.addAccepted(String.valueOf(s.id));
		}
		fstVisualizer.endGraph();
	}
	
	// reverses the edges of the fst and creates one initial state
	// uptates state ids
	public List<State> reverse() {
		return reverse(true);
	}
	
	public List<State> reverse(boolean createNewInitialState) {
		return FstOperations.reverse(this, createNewInitialState);
	}
	
	public void minimize() {
		FstOperations.minimize(this);
	}
}
