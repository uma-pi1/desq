package tools;

import fst.OutputLabel;
import it.unimi.dsi.fastutil.ints.Int2ByteOpenHashMap;

public class FstEdge  implements Comparable<FstEdge> {
	
	public static final byte VALID_OUTPUT = 2;
	public static final byte INVALID_OUTPUT = 1;
	public static final byte CACHE_MISS = 0;
	
	int id;
	int fromState;
	int toState;
	int transitionId;
	int ilabel;
	
	boolean isPartOfCycle;
	boolean isWildcardTransition;
	
	OutputLabel label;
	Int2ByteOpenHashMap outputCache;
	
	public FstEdge(int fromVertex, int toVertex, OutputLabel label) {
		this.fromState = fromVertex;
		this.toState = toVertex;
		this.label = label;
		this.outputCache = new Int2ByteOpenHashMap();
	}
	
	public FstEdge(int fromVertex, int toVertex, OutputLabel label, int ilabel, boolean isWildcardTransition, int transitionId) {
		this.fromState = fromVertex;
		this.toState = toVertex;
		this.label = label;
		this.ilabel = ilabel;
		this.isWildcardTransition = isWildcardTransition;
		this.transitionId = transitionId;
		this.outputCache = new Int2ByteOpenHashMap();
	}

	@Override
	public int compareTo(FstEdge other) {
		
	    int i = fromState - other.fromState;
	    if (i != 0) return i;
	    
	    i = toState - other.toState;
	    if (i != 0) return i;

	    return label.item - other.label.item;

	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof FstEdge) {
			FstEdge edge = (FstEdge) obj;
			return edge.fromState == this.fromState && edge.toState == this.toState && edge.label.item == this.label.item && edge.ilabel == this.ilabel;
		}
		
		return super.equals(obj);
	}

	public int getFromState() {
		return fromState;
	}

	public int getToState() {
		return toState;
	}
	
	public void setIsPartOfCylce(boolean isPartOfCycle) {
		this.isPartOfCycle = isPartOfCycle;
	}
	
	public boolean isPartOfCylce() {
		return isPartOfCycle;
	}

	public OutputLabel getLabel() {
		return label;
	}
	
	public boolean isWildcardTransition() {
		return isWildcardTransition;
	}

	public void addToOutputCache(int item, boolean validOutput) {
		if(validOutput) {
			outputCache.put(item, VALID_OUTPUT);
		} else {
			outputCache.put(item, INVALID_OUTPUT);
		}		
	}
	
	public byte getOutputCacheEntry(int item) {
		return outputCache.get(item);
	}

	public int getTransitionId() {
		return transitionId;
	}

	public void setTransitionId(int transitionId) {
		this.transitionId = transitionId;
	}
	
}
