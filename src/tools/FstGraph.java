package tools;

import it.unimi.dsi.fastutil.ints.Int2IntArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FstGraph {
	
	private boolean[] acceptedStates;
	private Int2ObjectArrayMap<ArrayList<FstEdge>> fstEdgesPerState;
	private Int2ObjectArrayMap<List<FstEdge>> reachableEdgesPerState;
	private Int2IntArrayMap maxTransitionsToFinalState = new Int2IntArrayMap();
	private ArrayList<ArrayList<FstEdge>> edgesToState = new ArrayList<ArrayList<FstEdge>>();

	public FstGraph(Int2ObjectArrayMap<ArrayList<FstEdge>> fstEdgesPerState, boolean[] acceptedStates) {
		this.fstEdgesPerState = fstEdgesPerState;
		this.acceptedStates = acceptedStates;
		
		reachableEdgesPerState = new Int2ObjectArrayMap<List<FstEdge>>();
		
		// an edge is defined by from state, to state and output label
		// the ID is added to more easily compare the edges
		
		List<FstEdge[]> cycles = getAllCycles();
		
		int edgeId = 0;
		for (int i = 0; i < fstEdgesPerState.size(); i++) {
			/* for all edges from that vertex */
			for (FstEdge edge : fstEdgesPerState.get(i)) {
				edgeId++;
				edge.id = edgeId;
				
				for (FstEdge[] fstEdges : cycles) {
					for (int j = 0; j < fstEdges.length; j++) {
						if(fstEdges[j].equals(edge)) {
							edge.isPartOfCycle = true;
							break;
						}
					}
					if(edge.isPartOfCycle == true) {
						break;
					}
				}
			}
			
			reachableEdgesPerState.put(i, getReachableEdges(i));
			
			List<FstEdge[]> stateCycles = getCyclesPerState(i);
			if(stateCycles.size() != 0) {
				maxTransitionsToFinalState.put(i, Integer.MAX_VALUE);
			} else {
				int maxTransitions = 0;
				for(int j=0; j<acceptedStates.length;j++) {
					if(acceptedStates[j]) {
						getPathsToState(i, j, null);
						for (Iterator<ArrayList<FstEdge>> iterator = edgesToState.iterator(); iterator.hasNext();) {
							ArrayList<FstEdge> pathToFinalState = (ArrayList<FstEdge>) iterator.next();
							maxTransitions = Integer.max(maxTransitions, pathToFinalState.size());
						}
					}
				}
				maxTransitionsToFinalState.put(i, maxTransitions);
			}
		}
		System.out.println("Done");
	}
	
	public Int2ObjectArrayMap<ArrayList<FstEdge>> getFstEdges() {
		return fstEdgesPerState;
	}

	public List<FstEdge[]> getAllCycles() {
		GraphAnalyzer cycleDetector = new GraphAnalyzer();
		return cycleDetector.getAllCycles();
	}
	
	public List<FstEdge[]> getCyclesPerState(int state) {
		GraphAnalyzer cycleDetector = new GraphAnalyzer();
		return cycleDetector.getCyclesForState(state);
	}
	
	public List<FstEdge> getReachableEdgesPerState(int state) {
		return reachableEdgesPerState.get(state);
	}
	
	public int[] getStates() {
		int[] states = new int[fstEdgesPerState.size()];
		for(int i = 0; i<fstEdgesPerState.size(); i++) {
			states[i] = i;
		}
		return states;
	}
	
	private void getPathsToState(int fromState, int toState, ArrayList<FstEdge> path) {
		
		if(path == null) {
			path = new ArrayList<FstEdge>();
			edgesToState.clear();
		}
		
		for (FstEdge edge : fstEdgesPerState.get(fromState)) {
			
			path.add(edge);
			getPathsToState(edge.toState, toState, path);
			
			if(edge.getToState() == toState) {
				ArrayList<FstEdge> newpath = new ArrayList<FstEdge>();
				newpath.addAll(path);
				edgesToState.add(newpath);
			}
			
			path.remove(path.size()-1);
		}
	}
	
	public int getMaxTransitionsToFinalState(int state) {
		return maxTransitionsToFinalState.get(state);
	}
	
	
	
	private List<FstEdge> getReachableEdges(int state) {
		/* for all edges from that vertex */
		GraphAnalyzer graphAnalyzer = new GraphAnalyzer();
		
		return graphAnalyzer.getReachableEdges(state);
	}
	
	// graph analyzing methods
	private class GraphAnalyzer  {
		private List<FstEdge[]> paths = new ArrayList<FstEdge[]>();
		private List<FstEdge> edges = new ArrayList<FstEdge>();
		
		public GraphAnalyzer() {}
		
		
		public List<FstEdge> getReachableEdges(int state) {
			for (FstEdge edge : fstEdgesPerState.get(state)) {
				if(!edges.contains(edge)) {
					edges.add(edge);
				}
				explorePath(new FstEdge[] { edge });
			}
			return edges;
		}
		
		public List<FstEdge[]> getAllCycles() {
			/* for all vertices in the graph */
			for (int i = 0; i < fstEdgesPerState.size(); i++) {
				/* for all edges from that vertex */
				for (FstEdge edge : fstEdgesPerState.get(i)) {
					findNewCycles(new FstEdge[] { edge });
				}
			}
			return paths;
		}
		
		public List<FstEdge[]> getCyclesForState(int state) {
			/* for all edges from that vertex */
			for (FstEdge edge : fstEdgesPerState.get(state)) {
				findNewCycles(new FstEdge[] { edge });
			}		
			return paths;
		}
		
		private void explorePath(FstEdge[] path) {
			FstEdge n = path[0];
			FstEdge[] sub = new FstEdge[path.length + 1];
			
			for(FstEdge edge: fstEdgesPerState.get(n.toState)) {
				if (!visited(edge, path)) {
					// neighbor node not on path yet
					sub[0] = edge;
					System.arraycopy(path, 0, sub, 1, path.length);
					
					// add edge to list of edges if it is new
					if(!edges.contains(edge)) {
						edges.add(edge);
					}
					
					// explore extended path
					explorePath(sub);
				} else {
					// cycle found -> stop processing
				}
			}
		}

		private void findNewCycles(FstEdge[] path) {
			FstEdge n = path[0];
			FstEdge[] sub = new FstEdge[path.length + 1];
			
			for(FstEdge edge: fstEdgesPerState.get(n.toState)) {
				if (!visited(edge, path)) {
						// neighbor node not on path yet
						sub[0] = edge;
						System.arraycopy(path, 0, sub, 1, path.length);
						
						// explore extended path
						findNewCycles(sub);
				} else {
					//if ( (edge.equals(path[path.length - 1]))) 
					// cycle found
					if (isNew(invert(path))) {
						paths.add(invert(path));
					}
				}
			}
		}

		// check of both arrays have same lengths and contents
		private Boolean equals(FstEdge[] a, FstEdge[] b) {
			Boolean ret = (a[0] == b[0]) && (a.length == b.length);

			for (int i = 1; ret && (i < a.length); i++) {
				if (a[i].equals(b[i])) {
					ret = false;
				}
			}

			return ret;
		}

		// create a path array with reversed order
		private FstEdge[] invert(FstEdge[] path) {
			FstEdge[] p = new FstEdge[path.length];

			for (int i = 0; i < path.length; i++) {
				p[i] = path[path.length - 1 - i];
			}

			return p;
		}
		
		// compare path against known cycles
		// return true, iff path is not a known cycle
		private Boolean isNew(FstEdge[] path) {
			Boolean ret = true;

			for (FstEdge[] p : paths) {
				if (equals(p, path)) {
					ret = false;
					break;
				}
			}

			return ret;
		}
		
		// compare path against known cycles
		// return true, iff path is not a known cycle
		private Boolean isNew(FstEdge edge) {
			Boolean ret = true;

			for (FstEdge e : edges) {
				if (e.equals(edge)) {
					ret = false;
					break;
				}
			}

			return ret;
		}
		
		// check if vertex n is contained in path
		private Boolean visited(FstEdge n, FstEdge[] path) {
			Boolean ret = false;

			for (FstEdge p : path) {
				if (p.equals(n)) {
					ret = true;
					break;
				}
			}

			return ret;
		}
		

	}

	
}
