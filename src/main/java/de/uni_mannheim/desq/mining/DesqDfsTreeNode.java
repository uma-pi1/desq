package de.uni_mannheim.desq.mining;

import java.util.ArrayList;
//import java.util.Collections;
import java.util.List;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * DesqDfsTreeNode.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class DesqDfsTreeNode {
	final DesqDfsProjectedDatabase projectedDatabase;
	Int2ObjectMap<DesqDfsProjectedDatabase>  expansionsByFid = new Int2ObjectOpenHashMap<>();
	final List<DesqDfsTreeNode> children = new ArrayList<>();
	int numFstStates; //TODO: not a clean way
	
	DesqDfsTreeNode(DesqDfsProjectedDatabase projectedDatabase) {
		this.projectedDatabase = projectedDatabase;
		this.numFstStates = projectedDatabase.snapshotSet.length;
	}
	
	void expandWithItem(int itemFid, int inputId, long inputSupport, int position, int stateId) {
		DesqDfsProjectedDatabase projectedDatabase = expansionsByFid.get(itemFid);
		if(projectedDatabase == null) {
			projectedDatabase = new DesqDfsProjectedDatabase(numFstStates);
			projectedDatabase.itemFid = itemFid;
			expansionsByFid.put(itemFid, projectedDatabase);
		}
		
		if(projectedDatabase.lastInputId != inputId) {
			// start a new posting
			projectedDatabase.flushSnapshotSet();
			projectedDatabase.postingList.newPosting();
			projectedDatabase.lastInputId = inputId;
			projectedDatabase.prefixSupport += inputSupport;
			projectedDatabase.snapshotSet[stateId].set(position);
			projectedDatabase.postingList.addNonNegativeInt(inputId);
			projectedDatabase.postingList.addNonNegativeInt(stateId);
            projectedDatabase.postingList.addNonNegativeInt(position);
		} else if(!projectedDatabase.snapshotSet[stateId].get(position)) {
			projectedDatabase.snapshotSet[stateId].set(position);
			projectedDatabase.postingList.addNonNegativeInt(stateId);
			projectedDatabase.postingList.addNonNegativeInt(position);
		}
	}
	
	void expansionsToChildren(long minSupport) {
		for(DesqDfsProjectedDatabase projectedDatabase : expansionsByFid.values()) {
			if(projectedDatabase.prefixSupport >= minSupport) {
				children.add(new DesqDfsTreeNode(projectedDatabase));
			}
		}
		// TODO: This probably does not work with DESQ DFS
		// See CompressedDesqDfs
		// Collections.sort(children, (c1, c2) -> c1.projectedDatabase.itemFid - c2.projectedDatabase.itemFid); // smallest fids first
	    expansionsByFid = null;
	}
	

    public void clear() {
        projectedDatabase.clear();
        if (expansionsByFid == null) {
            expansionsByFid = new Int2ObjectOpenHashMap<>();
        } else {
            expansionsByFid.clear();
        }
        children.clear();
    }

}
