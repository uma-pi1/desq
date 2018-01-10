package de.uni_mannheim.desq.mining;

//import java.util.Collections;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.State;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import javafx.geometry.Pos;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * DesqDfsTreeNode.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
final class DesqDfsPatriciaTreeNode {

	// -- member variables --------------------------------------------------------------------------------------------

	/** the FST */
	final Fst fst;

	/** the input trie */
	//final PatriciaTrieBasic inputTrie;
	final int inputTrieSize;

	/** possible FST states for this node */
	final BitSet possibleStates;

	/** If {@link #possibleStates} contains only one state, then the state, else -1 */
	//final int possibleState;

	/** The output item associated with this node */
	int itemFid;

	/** The prefix support of this node = number of distinct input sequences that can be expanded further. Equal
	 * to the number of input sequences (postings) in the projected database. Computed while expanding this
	 * node's parent. */
	//long prefixSupport;

	/** The partial support of this node = number of distinct input sequences that hit a final complete state and
	 * for which there are no further expansions. Computed while expanding this node's parent.
	 *
	 * CHANGED: contains support of non-final states -> need to hit final state during next expansion!*/
	long partialSupport;

	/** The projected database associated with this node. Computed while expanding this node's parent,
	 * and cleared after this node has been expanded. */
	PostingList projectedDatabase;

	/** The children of this node by item fid. Computed while expanding this node's parent. */
	Int2ObjectOpenHashMap<DesqDfsPatriciaTreeNode> childrenByFid = new Int2ObjectOpenHashMap<>();

	/** The last input sequence being processed. Used for incremental maintenance of {@link #//partialSupport}. */
	int currentInputId;

	/** The input sequence id of the last entry added to the projected database. Used for incremental maintenance
	 * of the projected database. */
	int projectedDatabaseCurrentInputId;

	/** Whether we saw a final complete state for the current input sequence */
	//boolean reachedFinalCompleteState;

	/** Whether we saw a non final complete state for the current input sequence */
	//boolean reachedNonFinalCompleteState;

	/** If we saw a final complete state for the current input sequence, then at this position */
	//int finalCompletePosition;

	/** If we saw a final complete state for the current input sequence, then it was this state */
	//int finalCompleteStateId;

	/** Buffers all snapshots of the current input sequence in the projected database. Used to avoid adding duplicate
	 * snapshots. Index of a snapshot is <code>position*nodeId*fst.numStates() + stateId</code>. */
	BitSet currentSnapshots;


	/** Whether a final state was reached already for this input node **/
	BitSet reachedFinalStateAtInputId ;

	/** Whether a non final state was reached already for this input node (partial support already recorded)**/
	BitSet reachedNonFinalStateAtInputId;

	/** keep track of input node supports **/
	Int2LongOpenHashMap relevantNodeSupports;


	/** projection */
	//ArrayList<PostingList> projection;
	//PostingList[] projection;

	Int2ObjectMap<BitSet> currentSnapshotsByInput;

	BitSet nodesWithOutput; //keep track of nodes producing this output (itemFid)

	// -- Assumptions --
	// - only Itemsets -> sorted by fid
	// - no generalization -> no duplicate items
	// -> an item can only be produced once in a sequence
	// -> an item cannot re-occur in sub-trie of a child node! -> removing duplicates of a node is sufficient


	// -- construction and clearing -----------------------------------------------------------------------------------
	/*DesqDfsPatriciaTreeNode(Fst fst, BitSet possibleStates) {
		this(fst,possibleStates,0);
	}*/

	DesqDfsPatriciaTreeNode(Fst fst, BitSet possibleStates, int inputTrieSize) {
		this.inputTrieSize = inputTrieSize;
		this.fst = fst;
		this.possibleStates = possibleStates;
		/*if (possibleStates.cardinality() == 1) {
			possibleState = possibleStates.nextSetBit(0);
		} else {
			possibleState = -1;
		}*/
		//currentSnapshots = new BitSet(fst.numStates()*16);
		//currentSnapshots = new BitSet(inputTrieSize * fst.numStates() * 2);
		//currentSnapshotsByInput = new BitSet[inputTrieSize];
		reachedFinalStateAtInputId = new BitSet(inputTrieSize);
		reachedNonFinalStateAtInputId = new BitSet(inputTrieSize);
		//projection = new ArrayList<>(inputTrieSize);
		//projection = new PostingList[inputTrieSize];

		currentSnapshotsByInput = new Int2ObjectOpenHashMap<>();
		nodesWithOutput = new BitSet();

		relevantNodeSupports = new Int2LongOpenHashMap();
		clear();
	}

	void clear() {
		// clear the posting list
		itemFid = -1;
		//prefixSupport = 0;
		partialSupport = 0;
		projectedDatabaseCurrentInputId = -1;
		currentInputId = -1;
		//reachedFinalCompleteState = false;
		//reachedNonFinalCompleteState = false;
		projectedDatabase = new PostingList();
		reachedFinalStateAtInputId.clear();
		reachedNonFinalStateAtInputId.clear();
		//projection.clear();
		//projection = new PostingList[inputTrieSize];
		//currentSnapshotsByInput = new BitSet[inputTrieSize];
		//currentSnapshots.clear();
		currentSnapshotsByInput.clear();
		nodesWithOutput.clear();

		relevantNodeSupports.clear();
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
		//projection = null;
		currentSnapshotsByInput = null;
		nodesWithOutput = null;
		relevantNodeSupports = null;
	}


	// -- projected database maintenance ------------------------------------------------------------------------------

	/** Expands this node with an item.
	 *
	 * @param outputFid the item fid of the child
	 * @param inputNode input trie node
	 * @param position (to)position in the input trie node
	 * @param state (to)state of the FST
	 */
	//void expandWithItem(final int outputFid, final int inputId, final long inputSupport,
	void expandWithItem(final int outputFid, final PatriciaTrieBasic.TrieNode inputNode,
						final int position, final State state) {

		//Get child node with corresponding fid
		DesqDfsPatriciaTreeNode child = childrenByFid.get(outputFid);
		if (child == null) {
			//If not existing -> create child node with corresponding fid
			BitSet childPossibleStates = fst.reachableStates(possibleStates, outputFid);
			child = new DesqDfsPatriciaTreeNode(fst, childPossibleStates, inputTrieSize);
			child.itemFid = outputFid;
			childrenByFid.put(outputFid, child);
		}

		final int inputNodeId = inputNode.getId();

		//Handle supports:
		if(state.isFinal()){
			if(!child.reachedFinalStateAtInputId.get(inputNodeId)) {
				child.finalStateReached(inputNode);
			}
		}else{
			//Check if final or not final already reached
			if(!child.reachedFinalStateAtInputId.get(inputNodeId)
					&& !child.reachedNonFinalStateAtInputId.get(inputNodeId)) {
				//store potential support (not yet reached final state) as partial support
				child.reachedNonFinalStateAtInputId.set(inputNodeId);
				if(!child.reachedNonFinalStateAtInputId.intersects(inputNode.descendants))
					child.partialSupport += inputNode.getSupport();
				//relevantNodeSupports.put(inputNodeId, inputSupport);
			}

		}
		//Check if state can be expanded
		if(!state.isFinalComplete() &&
				!(state.isFinal() && inputNode.isLeaf() && inputNode.items.size() == position)){
			//Add all possible positions to projected database, which are worth to follow up
			addToProjectedDatabase(child, inputNodeId,position, state.getId());
		}



		/*
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
		}*/
	}

	public void finalStateReached(PatriciaTrieBasic.TrieNode inputNode){
		final int inputNodeId = inputNode.getId();
		final long inputSupport = inputNode.getSupport();
		//handle a valid final state in FST -> remember it to avoid multiple counts of support
		reachedFinalStateAtInputId.set(inputNodeId);

		//Check that no ancestor was counted already
		if(!reachedFinalStateAtInputId.intersects(inputNode.ancestors)) {
			//add support to prefix support
			//child.prefixSupport += inputSupport;
			//Remember support
			relevantNodeSupports.put(inputNodeId, inputSupport);
		}

		//check if descendants were counted already and remove the support
		//Flag in "reachedFinalStateAtInputId" can stay
		if(reachedFinalStateAtInputId.intersects(inputNode.descendants)){
			BitSet intersect = (BitSet) reachedFinalStateAtInputId.clone();
			intersect.and(inputNode.descendants);
			//correct
			int bit = 0;
			while(bit >= 0) {
				bit = intersect.nextSetBit(bit); //returns -1 if none
				//Remove support
				if(bit > 0 && relevantNodeSupports.containsKey(bit)) {
					relevantNodeSupports.remove(bit);
					//child.reachedFinalStateAtInputId.set(inputNodeId, false);
				}
			}
		}

		//potentially correct partial (unconfirmed) support
		if(reachedNonFinalStateAtInputId.get(inputNodeId)){
			partialSupport -= inputSupport;
			//But correct only once!
			reachedNonFinalStateAtInputId.set(inputNodeId, false);
		}
	}

	/** Add a snapshot to the projected database of the given child. Ignores duplicate snapshots. */
	private void addToProjectedDatabase(final DesqDfsPatriciaTreeNode child, final int inputId,
										// final long inputSupport,
										final int position, final int stateId) {
		assert stateId < fst.numStates();
		//assert child.possibleStates.get(stateId);
		final int spIndex = position*fst.numStates() + stateId;
			/*	position * inputTrieSize * fst.numStates()
						+ inputId * fst.numStates()
						+ stateId;*/
		//Ensure that node + pos + state combination is recorded only once
		//if(!child.currentSnapshots.get(spIndex)){
			//child.currentSnapshots.set(spIndex);
		BitSet b = child.currentSnapshotsByInput.get(inputId);
		if(b == null){
			b = new BitSet();
			child.currentSnapshotsByInput.put(inputId,b);
		}else if(b.get(spIndex)) {
			//already recorded
			return;
		}
		b.set(spIndex);

		/*BitSet b;
		if((b= child.currentSnapshotsByInput[inputId]) == null) {
			b = new BitSet(fst.numStates()  * 4);
			currentSnapshotsByInput[inputId] = b;
		}
		if(!b.get(spIndex)) {
			b.set(spIndex);*/
			//Check if first occurrence of this input node id
			//PostingList p = child.projection[inputId];
			//PostingList p;
			/*if(child.projection.size() <= inputId){
				//Initialize posting list of node
				p = new PostingList();
				child.projection.set(inputId, p);
			}else {*/
			//try to derive existing posting list
			/*PostingList p = child.projection[inputId];

			if (p == null) {
				//Initialize posting list of node
				p = new PostingList();
				child.projection[inputId] = p;
			}*/
			//}
			//Add stated id and position as new posting
			//Not the same approach as in DesqDfs, because input ids may reoccur unordered
		child.projectedDatabase.newPosting();
		child.projectedDatabase.addNonNegativeInt(inputId);
		child.projectedDatabase.addNonNegativeInt(stateId);
		child.projectedDatabase.addNonNegativeInt(position);



		/*
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
		//} else {
			child.currentSnapshots.set(spIndex);
			if (child.possibleState < 0)
				child.projectedDatabase.addNonNegativeInt(stateId);
			child.projectedDatabase.addNonNegativeInt(position);
		}*/
	}

	public long getSupport(){
		LongAdder adder = new LongAdder();
		relevantNodeSupports.values().parallelStream().forEach(adder::add);
		return adder.longValue();
	}

	/** Removes all children that have prefix support below the given value of minSupport */
	void pruneInfrequentChildren(long minSupport) {
		ObjectIterator<Int2ObjectMap.Entry<DesqDfsPatriciaTreeNode>> childrenIt =
				childrenByFid.int2ObjectEntrySet().fastIterator();
		while (childrenIt.hasNext()) {
			Int2ObjectMap.Entry<DesqDfsPatriciaTreeNode> entry = childrenIt.next();
			final DesqDfsPatriciaTreeNode child = entry.getValue();
			//if (child.partialSupport + child.prefixSupport < minSupport) {
			if (child.getSupport() + child.partialSupport < minSupport) {
				childrenIt.remove();
			}
		}
	}


}
