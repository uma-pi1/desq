package de.uni_mannheim.desq.mining;

import java.util.BitSet;

/**
 * DesqDfsProjectedDatabase.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
final class DesqDfsProjectedDatabase {
	int itemFid;
	long prefixSupport;
	PostingList postingList;

	int currentInputId; // input sequence id of the current posting
	BitSet[] currentSnapshots; // buffers all the snapshots for the current input sequence
                               // (used to avoid storing duplicate snapshots)
	                           // index = state id; one bit per position in each Bitset
	DesqDfsProjectedDatabase(int numFstStates) {
		currentSnapshots = new BitSet[numFstStates];
		for(int i = 0; i < numFstStates; i++) {
			currentSnapshots[i] = new BitSet();
		}
		clear();
	}
	
	void clearSnapshots() {
		for (int i = 0; i < currentSnapshots.length; i++) {
			currentSnapshots[i].clear();
		}
	}
	
	void clear() {
		itemFid = -1;
		prefixSupport = 0;
		currentInputId = -1;
		postingList = new PostingList();
		for(int i = 0; i < currentSnapshots.length; i++) {
			currentSnapshots[i].clear();
		}
	}
	
}
