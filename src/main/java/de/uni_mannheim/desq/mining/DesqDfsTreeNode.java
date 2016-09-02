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
	DesqDfsProjectedDatabase projectedDatabase;
	Int2ObjectMap<DesqDfsProjectedDatabase>  expansionsByFid = new Int2ObjectOpenHashMap<>();
	List<DesqDfsTreeNode> children = new ArrayList<>();
	final int numFstStates; //TODO: not a clean way
	
	DesqDfsTreeNode(DesqDfsProjectedDatabase projectedDatabase) {
		this.projectedDatabase = projectedDatabase;
		this.numFstStates = projectedDatabase.currentSnapshots.length;
	}
	
	void expandWithItem(int itemFid, int inputId, long inputSupport, int position, int stateId) {
		DesqDfsProjectedDatabase projectedDatabase = expansionsByFid.get(itemFid);
		if (projectedDatabase == null) {
			projectedDatabase = new DesqDfsProjectedDatabase(numFstStates);
			projectedDatabase.itemFid = itemFid;
			expansionsByFid.put(itemFid, projectedDatabase);
		}
		
		// TODO: add delta encoding as for prefix growth
		if (projectedDatabase.currentInputId != inputId) {
			// start a new posting
			projectedDatabase.postingList.newPosting();
			projectedDatabase.clearSnapshots();
			projectedDatabase.prefixSupport += inputSupport;
			projectedDatabase.currentSnapshots[stateId].set(position);
			projectedDatabase.postingList.addNonNegativeInt(inputId-projectedDatabase.currentInputId);
			projectedDatabase.currentInputId = inputId;
			projectedDatabase.postingList.addNonNegativeInt(stateId);
            projectedDatabase.postingList.addNonNegativeInt(position);
		} else if (!projectedDatabase.currentSnapshots[stateId].get(position)) {
			projectedDatabase.currentSnapshots[stateId].set(position);
			projectedDatabase.postingList.addNonNegativeInt(stateId);
			projectedDatabase.postingList.addNonNegativeInt(position);
		}
	}
	
	void expansionsToChildren(long minSupport) {
		for(DesqDfsProjectedDatabase projectedDatabase : expansionsByFid.values()) {
			if (projectedDatabase.prefixSupport >= minSupport) {
				children.add(new DesqDfsTreeNode(projectedDatabase));
			}
		}
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

	/** Call this when node not needed anymore. */
	public void invalidate() {
		projectedDatabase = null;
		expansionsByFid = null;
		children = null;
	}
}
