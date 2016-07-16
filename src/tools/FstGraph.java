package tools;

import it.unimi.dsi.fastutil.ints.Int2IntArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import fst.OutputLabel.Type;

public class FstGraph {
	
	private boolean[] acceptedStates;
	private Int2ObjectArrayMap<ArrayList<FstEdge>> fstEdgesPerState;
	private Int2ObjectArrayMap<List<FstEdge>> reachableEdgesPerState;
	private Object[] maxTransitionsToFinalState;
	private ArrayList<ArrayList<FstEdge>> edgesToState = new ArrayList<ArrayList<FstEdge>>();
	private CyclePath[] primaryCyclePaths;

	public FstGraph(Int2ObjectArrayMap<ArrayList<FstEdge>> fstEdgesPerState, boolean[] acceptedStates) {
		this.fstEdgesPerState = fstEdgesPerState;
		this.acceptedStates = acceptedStates;
		
		reachableEdgesPerState = new Int2ObjectArrayMap<List<FstEdge>>();
		
		// an edge is defined by from state, to state and output label
		// the ID is added to more easily compare the edges
		
		List<FstEdge[]> cycles = getAllCycles(false);
		maxTransitionsToFinalState = new Object[acceptedStates.length];
		
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
			
			List<FstEdge[]> stateCycles = getCyclesPerState(i, true);
			if(stateCycles.size() != 0) {
				ArrayList<CyclePath> cyclePaths = new ArrayList<CyclePath>();
				for(int j=0; j<acceptedStates.length;j++) {
					if(acceptedStates[j]) {
						GraphAnalyzer analyzer = new GraphAnalyzer();
						// TODO: cycles over multiple edges are not considered correctly yet!
						List<FstEdge[]> primaryPaths = filterNonPrimaryPath(analyzer.getPathsToState(i, j));
						
						for (int k = 0; k < primaryPaths.size(); k++) {
							CyclePath cyclePath = new CyclePath(primaryPaths.get(k), cycles);
							cyclePaths.add(cyclePath);
						}
					}
				}
				maxTransitionsToFinalState[i] = cyclePaths;
//				maxTransitionsToFinalState[i] = Integer.MAX_VALUE;
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
				ArrayList<CyclePath> cyclePaths = new ArrayList<CyclePath>();
				CyclePath cyclePath = new CyclePath(maxTransitions);
				cyclePaths.add(cyclePath);
				maxTransitionsToFinalState[i] = cyclePaths;
			}
		}
//		System.out.println("Done");
	}
	
	public Int2ObjectArrayMap<ArrayList<FstEdge>> getFstEdges() {
		return fstEdgesPerState;
	}

	public List<FstEdge[]> getAllCycles(boolean findSubsequentCycles) {
		GraphAnalyzer cycleDetector = new GraphAnalyzer();
		return cycleDetector.getAllCycles(findSubsequentCycles);
	}
	
	public List<FstEdge[]> getCyclesPerState(int state, boolean findSubsequentCycles) {
		GraphAnalyzer cycleDetector = new GraphAnalyzer();
		return cycleDetector.getCyclesForState(state, findSubsequentCycles);
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
	
	public List<FstEdge[]> filterNonPrimaryPath(List<FstEdge[]> paths) {
		System.out.println("PrimaryPaths");
		List<FstEdge[]> primaryPaths = new ArrayList<FstEdge[]>();
		boolean[] primaryPathInfo = new boolean[paths.size()];
		
		// for all paths
		for(int subPathNumber = 0; subPathNumber < paths.size(); subPathNumber++) {
			FstEdge[] subPath = paths.get(subPathNumber);
			boolean primaryPath = true;
			// compare with all other paths
			for(int otherPathsNumber = 0; otherPathsNumber < paths.size(); otherPathsNumber++) {
				FstEdge[] path = paths.get(otherPathsNumber);
				if(!pathEquals(subPath, path)) {
					boolean[] edgeExists = new boolean[subPath.length];
					
					// compare each edge
					for(int i = 0; i < subPath.length; i++) {	
						// with each other edge from other path
						for(int j = 0; j<path.length; j++) {
							if(subPath[i].equals(path[j])) {
								edgeExists[i] = true;
							}
						}
					}
					boolean primary = false;
					for (int i = 0; i < edgeExists.length; i++) {
						if(edgeExists[i] == false) {
							primary = true;
						}
					}
					
					if(!primary) {
						if(primaryPathInfo[otherPathsNumber] == false && otherPathsNumber < subPathNumber) {
							// compared path is not a primary graph either... wait until it was compared with a primary graph
						} else {
							primaryPath = false;
						}
					} 
						
				}
			}
			
			if(primaryPath) {
				primaryPathInfo[subPathNumber] = true;
				for (int i = 0; i < subPath.length; i++) {
					if(i==0) System.out.print(subPath[i].fromState);
					System.out.print(" -> " +subPath[i].toState);
				}
				System.out.println("");
				primaryPaths.add(subPath);
			}
		}
		
		return primaryPaths;
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
	
	public int getMaxTransitionsToFinalState(int state, int cycleRepeats[]) {
		ArrayList<CyclePath> cyclePaths = (ArrayList<CyclePath>) maxTransitionsToFinalState[state];
		int maxLength = 0;
		for (CyclePath cyclePath : cyclePaths) {
			maxLength = Integer.max(maxLength, cyclePath.getMaxLength(cycleRepeats));
		}
		return maxLength;
	}

	private List<FstEdge> getReachableEdges(int state) {
		/* for all edges from that vertex */
		GraphAnalyzer graphAnalyzer = new GraphAnalyzer();
		
		return graphAnalyzer.getReachableEdges(state);
	}
	
	// check of both arrays have same lengths and contents
	private Boolean pathEquals(FstEdge[] a, FstEdge[] b) {
		Boolean ret = (a[0] == b[0]) && (a.length == b.length);

		for (int i = 1; ret && (i < a.length); i++) {
			if (!a[i].equals(b[i])) {
				ret = false;
			}
		}

		return ret;
	}
	
	// graph analyzing methods
	private class GraphAnalyzer  {
		private List<FstEdge[]> paths = new ArrayList<FstEdge[]>();
		private List<FstEdge> edges = new ArrayList<FstEdge>();
		
		public GraphAnalyzer() {}
		
		public List<FstEdge[]> getPathsToState(int fromState, int toState) {
			System.out.println("Path to final state from " + fromState + ":");
			for (FstEdge edge : fstEdgesPerState.get(fromState)) {
				getPathsToState(edge.toState, toState, new FstEdge[] { edge });
			}
			return paths;
		}
		
		private void getPathsToState(int fromState, int toState, FstEdge[] path) {
			FstEdge[] sub = new FstEdge[path.length + 1];
			
			for (FstEdge edge : fstEdgesPerState.get(fromState)) {
				
				if (!visited(edge, path)) {
					sub[0] = edge;
					System.arraycopy(path, 0, sub, 1, path.length);
					// go one step further
					getPathsToState(edge.toState, toState, sub);
					
					if(edge.getToState() == toState) {
//						System.out.println(Arrays.toString(sub));
						FstEdge[] finalPath = invert(sub);
						
						for (int i = 0; i < finalPath.length; i++) {
							if(i==0) System.out.print(finalPath[i].fromState);
							System.out.print(" -> " +finalPath[i].toState);
						}
						System.out.println("");
						paths.add(finalPath);
					}
				} else {
					// cycle that should not be followed!
				}
			}
		}
		
		
		public List<FstEdge> getReachableEdges(int state) {
			for (FstEdge edge : fstEdgesPerState.get(state)) {
				if(!edges.contains(edge)) {
					edges.add(edge);
				}
				explorePath(new FstEdge[] { edge });
			}
			return edges;
		}
		
		public List<FstEdge[]> getAllCycles(boolean findSubsequentCycles) {
			/* for all vertices in the graph */
			for (int i = 0; i < fstEdgesPerState.size(); i++) {
				/* for all edges from that vertex */
				for (FstEdge edge : fstEdgesPerState.get(i)) {
					findNewCycles(new FstEdge[] { edge }, findSubsequentCycles);
				}
			}
			return paths;
		}
		
		public List<FstEdge[]> getCyclesForState(int state, boolean findSubsequentCycles) {
			/* for all edges from that vertex */
			for (FstEdge edge : fstEdgesPerState.get(state)) {
				findNewCycles(new FstEdge[] { edge }, findSubsequentCycles);
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

		private void findNewCycles(FstEdge[] path, boolean findSubsequentCycles) {
			FstEdge n = path[0];
			FstEdge[] sub = new FstEdge[path.length + 1];
			
			for(FstEdge edge: fstEdgesPerState.get(n.toState)) {
				if (!visited(edge, path)) {
						// neighbor node not on path yet
						sub[0] = edge;
						System.arraycopy(path, 0, sub, 1, path.length);
						
						// explore extended path
						findNewCycles(sub, findSubsequentCycles);
				} else {
					if(findSubsequentCycles) {
						if (isNew(invert(path))) {
							paths.add(invert(path));
						}
					} else {
						// cycle found
						if (edge.equals(path[path.length - 1])) {
							// cycle found
							if (isNew(invert(path))) {
								paths.add(invert(path));
							}
						}	
					}
				}
			}
		}

		// check of both arrays have same lengths and contents
		private Boolean equals(FstEdge[] a, FstEdge[] b) {
			Boolean ret = (a[0] == b[0]) && (a.length == b.length);

			for (int i = 1; ret && (i < a.length); i++) {
				if (!a[i].equals(b[i])) {
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

	private class CyclePath {
		// for each state the constant length part
		int constantLength;
		
		// for each state and cycle the cycle lengths in the paths 
		int[] cyclesLengths;
		
		public CyclePath(int constantLength) {
			this.constantLength = constantLength;
			this.cyclesLengths = new int[0];
		}
		
		public CyclePath(FstEdge[] path, List<FstEdge[]> cycles) {
			
			cyclesLengths = new int[cycles.size()];
			constantLength = 0;
			
			for (int pathStart = 0; pathStart < path.length; pathStart++) {
				boolean cycleFound = false;
				
				FstEdge[] sub = new FstEdge[1];
				sub[0] = path[pathStart];
				
				for (int pathPos = pathStart + 1; pathPos < path.length; pathPos++) {
					for(int i = 0; i< cycles.size(); i++) {
						if(pathEquals(sub, cycles.get(i))) {
							cycleFound = true;
							for (int j = 0; j < cycles.get(i).length; j++) {
								if(cycles.get(i)[j].label.type != Type.EPSILON) {
									cyclesLengths[i]++;
								}
							}
						}
					}
					
					FstEdge[] oldPath = sub;
					
					sub = new FstEdge[oldPath.length+1];
					System.arraycopy(oldPath, 0, sub, 0, oldPath.length);
					sub[oldPath.length] = path[pathPos];
				}
				
				if(cycleFound == false) {
					if(path[pathStart].label.type != Type.EPSILON) {
						constantLength++;
					}
				}
			}
		}
		
		public int getMaxLength(int[] cycleRepeats) {
			int maxLength = 0;
			for (int i = 0; i < cyclesLengths.length; i++) {
				maxLength = maxLength + cycleRepeats[i] * cyclesLengths[i];
			}
			return maxLength + constantLength;
		}
		// save item information gain for each state and path?!
	}
	

}
