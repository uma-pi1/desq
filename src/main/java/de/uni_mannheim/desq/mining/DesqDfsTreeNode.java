package de.uni_mannheim.desq.mining;

//import java.util.Collections;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.State;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.BitSet;
import java.util.concurrent.atomic.LongAdder;

/**
 * DesqDfsTreeNode.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
final class DesqDfsTreeNode {
	public static LongAdder nodeCounter = new LongAdder();

	// -- member variables --------------------------------------------------------------------------------------------

	/** the FST */
	final Fst fst;

	/** possible FST states for this node */
	final BitSet possibleStates;

	/** If {@link #possibleStates} contains only one state, then the state, else -1 */
	final int possibleState;

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


	// -- construction and clearing -----------------------------------------------------------------------------------

	DesqDfsTreeNode(Fst fst, BitSet possibleStates) {
		nodeCounter.add(1);
		this.fst = fst;
		this.possibleStates = possibleStates;
		if (possibleStates.cardinality() == 1) {
			possibleState = possibleStates.nextSetBit(0);
		} else {
			possibleState = -1;
		}
		currentSnapshots = new BitSet(fst.numStates()*16);
		clear();
	}

	void clear() {
		// clear the posting list
		itemFid = -1;
		prefixSupport = 0;
		partialSupport = 0;
		projectedDatabaseCurrentInputId = -1;
		currentInputId = -1;
		reachedFinalCompleteState = false;
		reachedNonFinalCompleteState = false;
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
		currentSnapshots = null;
	}


	// -- projected database maintenance ------------------------------------------------------------------------------

	/** Expands this node with an item.
	 *
	 * @param outputFid the item fid of the child
	 * @param inputId input sequence id
	 * @param inputSupport support of the input sequence
	 * @param position position in the input sequence
	 * @param state state of the FST
	 */
	void expandWithItem(final int outputFid, final int inputId, final long inputSupport,
						final int position, final State state) {
		DesqDfsTreeNode child = childrenByFid.get(outputFid);
		if (child == null) {
			BitSet childPossibleStates = fst.reachableStates(possibleStates, outputFid);
			child = new DesqDfsTreeNode(fst, childPossibleStates);
			child.itemFid = outputFid;
			childrenByFid.put(outputFid, child);
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
		assert stateId < fst.numStates();
		assert child.possibleStates.get(stateId);
		final int spIndex = position*fst.numStates() + stateId;
		if (child.projectedDatabaseCurrentInputId != inputId) {
			// start a new posting
			child.projectedDatabase.newPosting();
			child.currentSnapshots.clear();
			child.prefixSupport += inputSupport;
			child.currentSnapshots.set(spIndex);
			child.projectedDatabase.addNonNegativeInt(inputId-child.projectedDatabaseCurrentInputId);
			child.projectedDatabaseCurrentInputId = inputId;
			if (child.possibleState < 0)
				child.projectedDatabase.addNonNegativeInt(stateId);
			child.projectedDatabase.addNonNegativeInt(position);
		} else if (!child.currentSnapshots.get(spIndex)) {
			child.currentSnapshots.set(spIndex);
			if (child.possibleState < 0)
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
}
