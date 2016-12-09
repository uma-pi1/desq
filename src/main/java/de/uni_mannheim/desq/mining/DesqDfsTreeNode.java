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

	// number of states in FST
	final int numStates;

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
	 * projected database. Index of a snapshot is <code>position*fst.numStates() + stateId</code>. */
	BitSet currentSnapshots;


	// -- construction and clearing -----------------------------------------------------------------------------------

	DesqDfsTreeNode(int numStates) {
		this.numStates = numStates;
		currentSnapshots = new BitSet(numStates*16);
		clear();
	}

	void clear() {
		// clear the posting list
		itemFid = -1;
		prefixSupport = 0;
		currentInputId = -1;
		projectedDatabase = new PostingList();
		currentSnapshots.clear();

		// clear the children
		if (childrenByFid == null) {
			childrenByFid = new Int2ObjectOpenHashMap<>();
		} else {
			childrenByFid.clear();
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
	void expandWithItem(int itemFid, int inputId, long inputSupport, int position, int stateId) {
		DesqDfsTreeNode child = childrenByFid.get(itemFid);
		if (child == null) {
			child = new DesqDfsTreeNode(numStates);
			child.itemFid = itemFid;
			childrenByFid.put(itemFid, child);
		}

		assert stateId < numStates;
		final int spIndex = position*numStates + stateId;
		if (child.currentInputId != inputId) {
			// start a new posting
			child.projectedDatabase.newPosting();
			child.currentSnapshots.clear();
			child.prefixSupport += inputSupport;
			child.currentSnapshots.set(spIndex);
			child.projectedDatabase.addNonNegativeInt(inputId-child.currentInputId);
			child.currentInputId = inputId;
			child.projectedDatabase.addNonNegativeInt(stateId);
			child.projectedDatabase.addNonNegativeInt(position);
		} else if (!child.currentSnapshots.get(spIndex)) {
			child.currentSnapshots.set(spIndex);
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
}
