package de.uni_mannheim.desq.mining;

//import java.util.Collections;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.BitSet;

/**
 * DesqDfsTreeNode.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
final class DesqDfsTreeNode {

	// -- member variables --------------------------------------------------------------------------------------------

	/** The output item associated with this node */
	int itemFid;

	/** The prefix support of this node. Computed while expanding this node's parent. */
	long prefixSupport;

	/** The projected database associated with this node. Computed while expanding this node's parent,
	 * and cleared after this node has been expanded. */
	PostingList projectedDatabase;

	/** The children of this node by item fid. Computed while expanding this node's parent. */
	Int2ObjectOpenHashMap<DesqDfsTreeNode> childrenByFid = new Int2ObjectOpenHashMap<>();

	/** The input sequence id of the last entry added to the projected database. Used for incremental maintenance
	 * of the projected database. */
	int currentInputId;

	/** Buffers all snapshots of the current input sequence. Used to avoid adding duplicate snapshots to the
	 * projected database. The index is the state id of the FST, and each bitset contains one bit per position
	 * in the current input sequence.
 	 */
	BitSet[] currentSnapshots; // buffers all the snapshots for the current input sequence
	// note: when we send NFAs, we only use one BitSet, as we store only T[pos] and not T[q@pos]

	// -- construction and clearing -----------------------------------------------------------------------------------

	DesqDfsTreeNode(int noFstStates) {
		currentSnapshots = new BitSet[noFstStates];
		for(int i = 0; i < noFstStates; i++) {
			currentSnapshots[i] = new BitSet();
		}
		clear();
	}

	void clear() {
		// clear the posting list
		itemFid = -1;
		prefixSupport = 0;
		currentInputId = -1;
		projectedDatabase = new PostingList();
		clearSnapshots();

		// clear the children
		if (childrenByFid == null) {
			childrenByFid = new Int2ObjectOpenHashMap<>();
		} else {
			childrenByFid.clear();
		}
	}

	void clearSnapshots() {
		for (int i = 0; i < currentSnapshots.length; i++) {
			currentSnapshots[i].clear();
		}
	}

	/** Call this when node not needed anymore to free up memory. */
	public void invalidate() {
		projectedDatabase = null;
		childrenByFid = null;
	}


	// -- projected database maintenance ------------------------------------------------------------------------------

	/** Add a snapshot to the projected database of a child of this node. Ignores duplicate snapshots.
	 *
	 * @param itemFid the item fid of the child
	 * @param inputId input sequence id
	 * @param inputSupport support of the input sequence
	 * @param position position in the input sequence
	 * @param stateId state of the FST
	 */
	void expandWithItemTraditional(int itemFid, int inputId, long inputSupport, int position, int stateId) {
		DesqDfsTreeNode child = childrenByFid.get(itemFid);
		if (child == null) {
			child = new DesqDfsTreeNode(currentSnapshots.length);
			child.itemFid = itemFid;
			childrenByFid.put(itemFid, child);
		}

		if (child.currentInputId != inputId) {
			// start a new posting
			child.projectedDatabase.newPosting();
			child.clearSnapshots();
			child.prefixSupport += inputSupport;
			child.currentSnapshots[stateId].set(position);
			child.projectedDatabase.addNonNegativeInt(inputId-child.currentInputId);
			child.currentInputId = inputId;
			child.projectedDatabase.addNonNegativeInt(stateId);
			child.projectedDatabase.addNonNegativeInt(position);
		} else if (!child.currentSnapshots[stateId].get(position)) {
			child.currentSnapshots[stateId].set(position);
			child.projectedDatabase.addNonNegativeInt(stateId);
			child.projectedDatabase.addNonNegativeInt(position);
		}
	}

	/** Removes all children that have prefix support below the given value of minSupport */
	void pruneInfrequentChildren(long minSupport) {
		ObjectIterator<Int2ObjectMap.Entry<DesqDfsTreeNode>> childrenIt =
				childrenByFid.int2ObjectEntrySet().fastIterator();
		while (childrenIt.hasNext()) {
			Int2ObjectMap.Entry<DesqDfsTreeNode> entry = childrenIt.next();
			if (entry.getValue().prefixSupport < minSupport) {
				childrenIt.remove();
			}
		}
	}

	/** ---------- Distributed mode --------------------------------------- */


	/** Add a snapshot to the projected database of a child of this node.
	 *  Stores inputId,pos   (instead of inputId,pos,state)
	 *
	 * @param itemFid the item fid of the child
	 * @param inputId input sequence id
	 * @param inputSupport support of the input sequence
	 * @param position position in the input sequence
	 */
	void expandWithTransition(int itemFid, int inputId, long inputSupport, int position) {
		DesqDfsTreeNode child = childrenByFid.get(itemFid);
		if (child == null) {
			child = new DesqDfsTreeNode(currentSnapshots.length);
			child.itemFid = itemFid;
			childrenByFid.put(itemFid, child);
		}

		if (child.currentInputId != inputId) {
			// start a new posting
			child.projectedDatabase.newPosting();
			child.clearSnapshots();
			child.prefixSupport += inputSupport;
            child.currentSnapshots[0].set(position);
			child.projectedDatabase.addNonNegativeInt(inputId-child.currentInputId);
			child.currentInputId = inputId;
			child.projectedDatabase.addNonNegativeInt(position);
		} else if(!child.currentSnapshots[0].get(position)){
			child.currentSnapshots[0].set(position);
			child.projectedDatabase.addNonNegativeInt(position);
		}
	}
}
