package fst;


import java.util.ArrayList;
import java.util.Map;

import org.apache.lucene.util.FixedBitSet;


import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import tools.FstEdge;
import tools.FstGraph;
import utils.Dictionary;
import visual.Vdfa;



/**
 * XFst.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class XFst {
	
	protected int[][] ilabels;
	
	protected OutputLabel[][] olabels;
	
	protected int[][] toStates;
	
	protected int initialState;
	
	private int[] stateSize;
	
	private boolean[] finalStates;
	
	private Int2ObjectOpenHashMap<FixedBitSet> dIndex = new Int2ObjectOpenHashMap<FixedBitSet>();
	
	private Dictionary dictionary = Dictionary.getInstance();
	
	public XFst(int numStates) {
		this(-1, numStates);
	}
	
	public XFst(int initialState, int numStates) {
		this.initialState = initialState;
		ilabels = new int[numStates][];
		olabels = new OutputLabel[numStates][];
		toStates = new int[numStates][];
		finalStates = new boolean[numStates];
		
		stateSize = new int[numStates];
		for(int i = 0; i < numStates; ++i) {
			stateSize[i] = 0;
		}
		
		//Set the default return value for dIndex
		dIndex.defaultReturnValue(null);
	}
	
	public void initializeState(int state, int numTransitions) {
		ilabels[state] = new int[numTransitions];
		olabels[state] = new OutputLabel[numTransitions];
		toStates[state] = new int[numTransitions];
	}
	
	public void addTransition(int fromState, int ilabel, OutputLabel olabel, int toState) {
		ilabels[fromState][stateSize[fromState]] = ilabel;
		olabels[fromState][stateSize[fromState]] = olabel;
		toStates[fromState][stateSize[fromState]] = toState;
		stateSize[fromState]++;
		
		// Compute reachability index
		if (ilabel > 0) {
			FixedBitSet bits = dIndex.get(ilabel);
			if (bits == null) {
				bits = new FixedBitSet(dictionary.getTotalItems());
				for (int descItemId : dictionary.getDescendants(ilabel)) {
					bits.set(descItemId);
				}
				dIndex.put(ilabel, bits);
			}
		}
	}
	
	public void addFinalState(int state) {
		finalStates[state] = true;
	}
	
	public boolean isFinalState(int state) {
		return finalStates[state];
	}
	
	public int getInitialState() {
		return initialState;
	}
	public int[] getInputLabels(int state) {
		return ilabels[state];
	}
	
	public OutputLabel[] getOutputLabels(int state) {
		return olabels[state];
	}
	
	public int getInputLabel(int state, int transitionId) {
		return ilabels[state][transitionId];
	}
	
	public OutputLabel getOutputLabel(int state, int transitionId) {
		return olabels[state][transitionId];
	}
	
	public int getToState(int state, int transitionId) {
		return toStates[state][transitionId];
	}
	
	public boolean canStep(int itemId, int state, int transitionId) {
		int ilabel = ilabels[state][transitionId];
		
		// If input label is a wild card or a=
		if(0 == ilabel || itemId == -ilabel) {
			return true;
		}
		// Return the reachability bit
		return dIndex.get(ilabel).get(itemId);
	}
	
	public boolean hasOutgoingTransition(int state, int itemId) {
		//TODO: compute bit index for it there is a transition
		return true;
	}
	
	public int numTransitions(int state) {
		return ilabels[state].length;
		//return stateSize[state]; 
	}
	
	public int numStates() {
		return stateSize.length;
	}
	
	
	/**
	 * @param a
	 * @param b
	 * @return true if item a is reachable from item b in the DAG
	 */
	public boolean isReachable(int a, int b) {
		return (0 == a) ? true : dIndex.get(a).get(b);
	}
	
	public FstGraph convertToFstGraph() {
		Int2ObjectArrayMap<ArrayList<FstEdge>> graphEdges = new Int2ObjectArrayMap<ArrayList<FstEdge>>();
				
			for(int s = 0; s < numStates(); ++s) {
				for(int tId = 0; tId < numTransitions(s); ++ tId) {
					ArrayList<FstEdge> edges = graphEdges.get(s);
					if(edges == null) {
						edges = new ArrayList<FstEdge>();
					}
					if(0 == ilabels[s][tId]) {
						edges.add(new FstEdge(s, toStates[s][tId], olabels[s][tId], true));
					} else {
						edges.add(new FstEdge(s, toStates[s][tId], olabels[s][tId], false));
					}
					graphEdges.put(s, edges);
					
				}
			}
			
			/* Add the final states with no output edges */
			for (int i = 0; i < finalStates.length; i++) {
				if (finalStates[i]) {
					ArrayList<FstEdge> edges = graphEdges.get(i);
					if(edges == null) {
						edges = new ArrayList<FstEdge>();
					}
					graphEdges.put(i, edges);
				}
			}
			
			return new FstGraph(graphEdges, finalStates);
//					vdfa.add(String.valueOf(s), String.valueOf(ilabels[s][tId]), olabels[s][tId].toString(), String.valueOf(toStates[s][tId]));
//				}
//				if(isFinalState(s)) {
//					vdfa.addAccepted(String.valueOf(s));
//				}
//			}
//			for (Map.Entry<Integer, Integer> entry : stateTable[i].entrySet()) {
//				int label = entry.getKey();
//				int offset = entry.getValue();
//				while (offset < toStates.size()) {
//					if (toStates.get(offset) == 0)
//						break;
//					OutputLabel yield = outLabels.get(offset);
//					int to = toStates.get(offset);
//					yield.item = label;
//					ArrayList<FstEdge> edges = graphEdges.get(from);
//					if(edges == null) {
//						edges = new ArrayList<FstEdge>();
//					}
//					edges.add(new FstEdge(from, to, yield, false));
//					graphEdges.put(from, edges);
//					offset++;
//				}
//			}
//		}
//		
//		
//		/* For all wildcard transitions */
//		for (int i = 0; i < wcStateTable.length; ++i) {
//			if (wcStateTable[i] != -1) {
//				int from = i;
//				int label = 0;
//				int offset = wcStateTable[i];
//				while (offset < toStates.size()) {
//					if (toStates.get(offset) == 0)
//						break;
//					OutputLabel yield = outLabels.get(offset);
//					int to = toStates.get(offset);
//					yield.item = label;
//					ArrayList<FstEdge> edges = graphEdges.get(from);
//					if(edges == null) {
//						edges = new ArrayList<FstEdge>();
//					}
//					edges.add(new FstEdge(from, to, yield, true));
//					graphEdges.put(from, edges);
//					offset++;
//				}
//			}
//		}
//		
//		/* Add the final states with no output edges */
//		for (int i = 0; i < acceptedStates.length; i++) {
//			if (acceptedStates[i]) {
//				ArrayList<FstEdge> edges = graphEdges.get(i);
//				if(edges == null) {
//					edges = new ArrayList<FstEdge>();
//				}
//				graphEdges.put(i, edges);
//			}
//		}
	}
	
	public void print(String file) {
		
		Vdfa vdfa = new Vdfa(file);
		vdfa.beginGraph();

		for(int s = 0; s < numStates(); ++s) {
			for(int tId = 0; tId < numTransitions(s); ++ tId) {
				vdfa.add(String.valueOf(s), String.valueOf(ilabels[s][tId]), olabels[s][tId].toString(), String.valueOf(toStates[s][tId]));
			}
			if(isFinalState(s)) {
				vdfa.addAccepted(String.valueOf(s));
			}
		}
		vdfa.endGraph();
	}
	
}
