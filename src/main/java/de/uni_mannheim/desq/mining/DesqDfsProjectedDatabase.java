package de.uni_mannheim.desq.mining;

import java.util.BitSet;

/**
 * DesqDfsProjectedDatabase.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
final class DesqDfsProjectedDatabase {
	int itemFid;
	long prefixSupport;
	int lastInputId;
	NewPostingList postingList;
	BitSet[] snapshotSet;
	
	DesqDfsProjectedDatabase(int numFstStates) {
		snapshotSet = new BitSet[numFstStates];
		for(int i = 0; i < numFstStates; i++) {
			snapshotSet[i] = new BitSet();
		}
		clear();
	}
	
	void flushSnapshotSet() {
		for(int i = 0; i < snapshotSet.length; i++) {
			snapshotSet[i].clear();
		}
	}
	
	void clear() {
		itemFid = -1;
		prefixSupport = 0;
		lastInputId = -1;
		postingList = new NewPostingList();
		for(int i = 0; i < snapshotSet.length; i++) {
			snapshotSet[i].clear();
		}
	}
	
}
