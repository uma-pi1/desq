package de.uni_mannheim.desq.fst;


import com.google.common.base.Strings;
import de.uni_mannheim.desq.fst.graphviz.FstVisualizer;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.commons.io.FilenameUtils;

import java.io.PrintStream;
import java.util.*;


 
public final class Fst {
	/** initial state */
	State initialState;
	
	/** list of states; initialized only after state numbers are updated; see updateStates() */
	List<State> states = new ArrayList<>();
    List<State> finalStates = new ArrayList<>();

	/** flag indicating whether full input must be consumed
	 * TODO: remove once we annotate states with isComplete
	 */
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
	
	/* Returns a copy of FST with shallow copy of its transitions */
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

	/** Reverses the edges of this FST and returns a list of new initial states.
	 *
	 * @param createNewInitialState if true, the reversed FST will have just one initial state
	 * @return the list of initial states of the reversed FST
	 */
	public List<State> reverse(boolean createNewInitialState) {
		return FstOperations.reverse(this, createNewInitialState);
	}

	/** Minimizes this FST to the extent possible */
	public void minimize() {
		FstOperations.minimize(this);
	}

	// -- printing ----------------------------------------------------------------------------------------------------

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
		out.print("]");
        out.print(", finalComplete=[");
        separator = "";
        for (State state : getFinalStates()) {
            if(state.isFinalComplete) {
                out.print(separator);
                out.print(state.id);
                separator = ",";
            }
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

		out.print(indentString);
		out.print("PatEx  : ");
		out.println(toPatternExpression());
	}
	
	/** Exports the fst using graphviz (type bsed on extension, e.g., "gv" (source file), "pdf", ...) */
	public void exportGraphViz(String file) {
		FstVisualizer fstVisualizer = new FstVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
		fstVisualizer.beginGraph();
		for(State s : states) {
			for(Transition t : s.transitionList)
                fstVisualizer.add(String.valueOf(s.id), t.labelString(), String.valueOf(t.getToState().id));
			if(s.isFinal)
				fstVisualizer.addFinalState(String.valueOf(s.id), s.isFinalComplete);
		}
		fstVisualizer.endGraph();
	}

	/** Creates a pattern expression that is equivalent to this FST. In general, the resulting pattern experssions
	 * are quite long (they are not "minimized" in any way). */
	public String toPatternExpression() {
		// we do this via a variant of the Brzozowski and McCluskey state elimination algorithm

		// first we collect all edges
		// we add a new dummy-initial state with an edge to the initial state (-2)
		// and another dummy-final state with edges from all final states (-1)
		List<Edge> edges = new LinkedList<>();
		for (State s : getStates()) {
			for (Transition t : s.getTransitions()) {
				edges.add(new Edge(s.id, t.toState.id, t.toPatternExpression()));
			}
			if (s.isFinal()) {
				edges.add(new Edge(s.id, -1, "")); // from final to dummy-final
			}
			if (s == initialState) {
				edges.add(new Edge(-2, s.id, "")); // from dummy-initial to initial
			}
		}

		// now eliminate all but the dummy states
		for (State s : getStates()) {
			// collect incoming, loop, and outgoing edges for s and remove them from the edge list
			// after this step, s has not more edges
			Map<Integer, List<Edge>> inEdgesMap = new HashMap<>(); // indexed by fromState
			List<Edge> loopEdges = new ArrayList<>();
			Map<Integer, List<Edge>> outEdgesMap = new HashMap<>(); // indexed by toState
			Iterator<Edge> it = edges.iterator();
			while (it.hasNext()) {
				Edge e = it.next();
				if (e.to == s.id) {
					if (e.from == s.id) {
						loopEdges.add(e);
					} else {
						List inEdges = inEdgesMap.get(e.from);
						if (inEdges == null) inEdges = new ArrayList<>();
						inEdges.add(e);
						inEdgesMap.put(e.from, inEdges);
					}
					it.remove();
				} else if (e.from == s.id) {
					List outEdges = outEdgesMap.get(e.to);
					if (outEdges ==null) outEdges = new ArrayList<>();
					outEdges.add(e);
					outEdgesMap.put(e.to, outEdges);
					it.remove();
				}
			}

			// create new edges to skip over s; after this, s is eliminated
			String loopExp = loopEdges.isEmpty() ? " " : ("[" + combinedExp(loopEdges) + "]* ");
			for (List<Edge> inEdges : inEdgesMap.values()) {
				String inExp = combinedExp(inEdges);
				for (List<Edge> outEdges : outEdgesMap.values()) {
					String outExp = combinedExp(outEdges);
					edges.add(new Edge(inEdges.get(0).from, outEdges.get(0).to, inExp+loopExp+outExp));
				}
			}
		}

		// now we have the desired pattern expression
		assert edges.size() == 1; // from dummy-initial to dummy-final
		return edges.get(0).label; // this edge's label is the result
	}


	/** Helper class for storing edges in {@link #toPatternExpression()} */
	private static class Edge {
		int from;
		int to;
		String label;

		Edge(int from, int to, String label) {
			this.from = from;
			this.to = to;
			this.label = label;
		}
	}

	/** Returns a pattern experssion that combines all edge labels via an alternativ (|) */
	private static String combinedExp(List<Edge> edges) {
		Collections.sort(edges, (o1, o2) -> o1.label.compareTo(o2.label)); // just to make output more readable
		String exp = edges.size() > 1 ? "[" : "";
		String sep = "";
		for (Edge e : edges) {
			exp += sep + e.label;
			sep = "|";
		}
		if (edges.size()>1)
			exp += "]";
		return exp;
	}

	/** Annotates final states of the FST. An final FST state is finalComplete
     * if it accepts .* without further output and there is no state with output
     * reachable.
     */
	public void annotateFinalStates() {
        // Convert FST starting at final states to DFA by only looking of .:EPS transitions

		// Map fst states to xdfa state
		Map<IntSet, XDfaState> xDfaStateForFstStateIdSet = new HashMap<>();
		// Unprocessed xdfa states
		Stack<IntSet> unprocessedStateIdSets = new Stack<>();
		// Processed xdfa states
		Set<IntSet> processedStateIdSets = new HashSet<>();

		// Initialize list of final state ids
        // if fst is reversed it will have only one final state
        IntList finalStateIdList = new IntArrayList();
        for(State finalState : finalStates) {
            // if fst is reversed without creating a new initial state as in two pass
            // then finalStates are not updated
            if(finalState.isFinal)
                finalStateIdList.add(finalState.id);
        }
        if(initialState.isFinal)
            finalStateIdList.add(initialState.id);

		for(int finalStateId : finalStateIdList) {
			IntSet initialStateIdSet = IntSets.singleton(finalStateId);
			xDfaStateForFstStateIdSet.put(initialStateIdSet, new XDfaState(true));
			unprocessedStateIdSets.push(initialStateIdSet);
		}

		while(!unprocessedStateIdSets.isEmpty()) {
			// process fst states
			IntSet stateIdSet = unprocessedStateIdSets.pop();

			if(!processedStateIdSets.contains(stateIdSet)) {
				IntSet nextStateIdSet = new IntOpenHashSet();
				boolean isFinal = false;
				boolean hasNonEpsOutput = false;

				// we look at only outgoing .:EPS transitions
				for(int stateId : stateIdSet) {
					for(Transition transition : getState(stateId).transitionList) {
						isFinal = transition.toState.isFinal;
						if(transition.isDotEps()) {
							//add toState
							nextStateIdSet.add(transition.toState.id);
						} else {
							// if there is an outgoing transition with non eps output
							hasNonEpsOutput = true;
						}
					}
				}
				// create the next xdfa state
				XDfaState nextXDfaState = xDfaStateForFstStateIdSet.get(nextStateIdSet);
				if(nextXDfaState == null) {
					nextXDfaState = new XDfaState(isFinal);
				}
				XDfaState xDfaState = xDfaStateForFstStateIdSet.get(stateIdSet);
				xDfaState.nextState = nextXDfaState;

				// if there was an outgoing transition with non eps output
				xDfaState.hasNonEpsOutput = hasNonEpsOutput;
			}
			// mark as processed
			processedStateIdSets.add(stateIdSet);
		}


		// annotate final states
        // If DFA starting at final state is a chain with all states final and a self loop at the end
        // and there is no reachable transition that produces an output then the corresponding final
        // fst state is complete
		for(int finalStateId : finalStateIdList) {
            // get xdfa state
            XDfaState xDfaState = xDfaStateForFstStateIdSet.get(IntSets.singleton(finalStateId));
            while(true) {
                XDfaState nextXDfaState = xDfaState.nextState;
                if(nextXDfaState == null || xDfaState.hasNonEpsOutput || !xDfaState.isFinal){
                    // finalState.isFinalComplete = false; // false by construction
                    break;
                }
                if(nextXDfaState == xDfaState) {
                    getState(finalStateId).isFinalComplete = true;
                    break;
                }
                xDfaState = nextXDfaState;
            }
		}

	}

	/** Helper class for DFA states for annotating final fst states in {@link #annotateFinalStates()}*/
	private class XDfaState {
		XDfaState nextState = null;
		boolean hasNonEpsOutput = false;
		boolean isFinal = false;

		XDfaState(boolean isFinal) {
			this.isFinal = isFinal;
		}
	}

	public void removeAnnotations() {
        for(State state : getStates()) {
            state.isFinalComplete = false;
        }
    }
}
