package de.uni_mannheim.desq.mining;

//import java.util.Collections;

import de.uni_mannheim.desq.fst.State;
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

	/** The prefix support of this node = number of distinct input sequences that can be expanded further. Equal
	 * to the number of input sequences (postings) in the projected database. Computed while expanding this
	 * node's parent. */
	long prefixSupport;

	/** The partial support of this node = number of distinct input sequences that hit a final complete state and
	 * for which there are no further expansions. Computed while expanding this node's parent. */
	long partialSupport;

	/** The projected database associated with this node. Computed while expanding this node's parent,
	 * and cleared after this node has been expanded. */
	PostingList projectedDatabase;

	/** The children of this node by item fid. Computed while expanding this node's parent. */
	Int2ObjectOpenHashMap<DesqDfsTreeNode> childrenByFid = new Int2ObjectOpenHashMap<>();

	/** The last input sequence being processed. Used for incremental maintenance of {@link #partialSupport}. */
	int currentInputId;

	/** The input sequence id of the last entry added to the projected database. Used for incremental maintenance
	 * of the projected database. */
	int projectedDatabaseCurrentInputId;

	/** Whether we saw a final complete state for the current input sequence */
	boolean reachedFinalCompleteState;

	/** Whether we saw a non final complete state for the current input sequence */
	boolean reachedNonFinalCompleteState;

	/** If we saw a final complete state for the current input sequence, then at this position */
	int finalCompletePosition;

	/** If we saw a final complete state for the current input sequence, then it was this state */
	int finalCompleteStateId;

	/** Buffers all snapshots of the current input sequence in the projected database. Used to avoid adding duplicate
	 * snapshots. Index of a snapshot is <code>position*fst.numStates() + stateId</code>. */
	BitSet currentSnapshots;

	/** Equivalents to projectedDatabase, currentSnapshots and projectedDatabaseCurrentInputID for hybrid mode
	 * In hybrid mode, we mine on NFAs and (trimmed) input sequences at the same time. So we have one posting list
	 * for the input sequences, which stores snapshots `pos@state`, and one for NFAs, which stores snapshots `state`.
	 */
	PostingList projectedNFADatabase;
	BitSet currentNFASnapshots;
	int projectedNFADatabaseCurrentInputId;

	boolean seenPivot = false;


	// -- construction and clearing -----------------------------------------------------------------------------------

	DesqDfsTreeNode(int numStates) {
		this(numStates, true, false);
	}

	DesqDfsTreeNode(int numStates, boolean useInputSequences, boolean useNFAs) {
		this.numStates = numStates;
		if(useInputSequences)
            currentSnapshots = new BitSet(numStates*16);
		if(useNFAs)
            currentNFASnapshots = new BitSet(16);
		clear();
	}

	void clear() {
		// clear the posting list
		itemFid = -1;
		prefixSupport = 0;
		partialSupport = 0;
		projectedDatabaseCurrentInputId = -1;
		projectedNFADatabaseCurrentInputId = -1;
		currentInputId = -1;
		reachedFinalCompleteState = false;
		reachedNonFinalCompleteState = false;
		projectedDatabase = new PostingList();
		projectedNFADatabase = new PostingList();
		if(currentSnapshots != null) currentSnapshots.clear();
		if(currentNFASnapshots != null) currentNFASnapshots.clear();

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
		projectedNFADatabase = null;
		childrenByFid = null;
		currentSnapshots = null;
	}


	// -- projected database maintenance ------------------------------------------------------------------------------

	/** Expands this node with an item.
	 *
	 * @param itemFid the item fid of the child
	 * @param inputId input sequence id
	 * @param inputSupport support of the input sequence
	 * @param position position in the input sequence
	 * @param state state of the FST
	 */
	void expandWithItem(final int itemFid, final int inputId, final long inputSupport,
						final int position, final State state, boolean isPivot) {
		DesqDfsTreeNode child = childrenByFid.get(itemFid);
		if (child == null) {
			child = new DesqDfsTreeNode(numStates, currentSnapshots != null, currentNFASnapshots != null);
			child.itemFid = itemFid;
			child.seenPivot = this.seenPivot || isPivot;
			childrenByFid.put(itemFid, child);
		}

		final int stateId = state.getId();
		if (child.currentInputId == inputId) {
			// we continue with the same input sequence
			if (state.isFinalComplete()) {
				// process final complete state
				if (child.reachedFinalCompleteState) {
					// we already recorded a final complete state for this input, so we don't need to record another one
					return;
				} else {
					// otherwise we remember it
					child.reachedFinalCompleteState = true;
					if (child.reachedNonFinalCompleteState) {
						// either in the projected database
						addToProjectedDatabase(child, inputId, inputSupport, position, stateId);
					} else {
						// or as a part of partial support (here we avoid writing unnecessary information to the
						// projected database)
						child.partialSupport += inputSupport;
						child.finalCompletePosition = position;
						child.finalCompleteStateId = stateId;
					}
				}
			} else {
				// process non-final complete state
				if (child.reachedFinalCompleteState & !child.reachedNonFinalCompleteState) {
					// we hit a non final complete state after a final complete one -> put one entry for the prior final
					// complete state in the projected database and remove it from the partial support
					child.partialSupport -= inputSupport;
					addToProjectedDatabase(child, inputId, inputSupport, child.finalCompletePosition,
							child.finalCompleteStateId);
				}
				child.reachedNonFinalCompleteState = true;
				addToProjectedDatabase(child, inputId, inputSupport, position, stateId);
			}
		} else {
			// process new input sequence
			child.currentInputId = inputId;
			if (state.isFinalComplete()) {
				child.reachedFinalCompleteState = true;
				child.reachedNonFinalCompleteState = false;
				child.partialSupport += inputSupport;
				child.finalCompleteStateId = stateId;
				child.finalCompletePosition = position;
			} else {
				child.reachedFinalCompleteState = false;
				child.reachedNonFinalCompleteState = true;
				addToProjectedDatabase(child, inputId, inputSupport, position, stateId);
			}
		}
	}

	/** Add a snapshot to the projected database of the given child. Ignores duplicate snapshots. */
	private void addToProjectedDatabase(final DesqDfsTreeNode child, final int inputId,
											  final long inputSupport, final int position, final int stateId) {
		assert stateId < numStates;
		final int spIndex = position*numStates + stateId;
		if (child.projectedDatabaseCurrentInputId != inputId) {
			// start a new posting
			child.projectedDatabase.newPosting();
			child.currentSnapshots.clear();
			child.prefixSupport += inputSupport;
			child.currentSnapshots.set(spIndex);
			child.projectedDatabase.addNonNegativeInt(inputId-child.projectedDatabaseCurrentInputId);
			child.projectedDatabaseCurrentInputId = inputId;
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
			final DesqDfsTreeNode child = entry.getValue();
			if (child.partialSupport + child.prefixSupport < minSupport) {
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

		// when we are working on NFAs, we store T[pos] instead of T[pos@q]

		DesqDfsTreeNode child = childrenByFid.get(itemFid);
		if (child == null) {
			child = new DesqDfsTreeNode(this.numStates, currentSnapshots != null, currentNFASnapshots != null);
			child.itemFid = itemFid;
			childrenByFid.put(itemFid, child);
		}

        // the following code is copied from addToProjectedDatabase. we don't write the stateId for NFAs
		//   this would be the call: addToProjectedDatabase(child, inputId, inputSupport, position, 0);
		final int spIndex = position;
		if (child.projectedNFADatabaseCurrentInputId != inputId) {
			// start a new posting
			child.projectedNFADatabase.newPosting();
			child.currentNFASnapshots.clear();
			child.prefixSupport += inputSupport;
			child.currentNFASnapshots.set(spIndex);
			child.projectedNFADatabase.addNonNegativeInt(inputId-child.projectedNFADatabaseCurrentInputId);
			child.projectedNFADatabaseCurrentInputId = inputId;
//			child.projectedDatabase.addNonNegativeInt(stateId);
			child.projectedNFADatabase.addNonNegativeInt(position);
		} else if (!child.currentNFASnapshots.get(spIndex)) {
			child.currentNFASnapshots.set(spIndex);
//			child.projectedDatabase.addNonNegativeInt(stateId);
			child.projectedNFADatabase.addNonNegativeInt(position);
		}
	}
}
