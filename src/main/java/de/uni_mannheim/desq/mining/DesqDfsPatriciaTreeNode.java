package de.uni_mannheim.desq.mining;

//import java.util.Collections;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.State;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.BitSet;
import java.util.concurrent.atomic.LongAdder;

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
	 */
	long partialSupport;

	/** the potential support tracks the sum of support nodes which did not reach a final state yet.
	 * They must be confirmed during next expansion. This value is just an estimation because sequences may be counted
	 * multiple times (if child node was already counted)
	 */
	long potentialSupport;

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
	//BitSet currentSnapshots;


	/** Whether a final state was reached already for this input node **/
	BitSet reachedFinalStateAtInputId ;

	/** Whether a non final state was reached already for this input node (partial support already recorded)**/
	BitSet reachedNonFinalStateAtInputId;

	/** keep track of input node supports **/
	Int2LongOpenHashMap relevantNodeSupports;

	//private Int2ObjectMap<BitSet> currentSnapshotsByInput;

	//BitSet nodesWithOutput; //keep track of nodes producing this output (itemFid)

	// -- construction and clearing -----------------------------------------------------------------------------------

	DesqDfsPatriciaTreeNode(Fst fst, BitSet possibleStates, int inputTrieSize) {
		this.inputTrieSize = inputTrieSize;
		this.fst = fst;
		this.possibleStates = possibleStates;
		/*if (possibleStates.cardinality() == 1) {
			possibleState = possibleStates.nextSetBit(0);
		} else {
			possibleState = -1;
		}*/
		projectedDatabase = new PostingList();
		//currentSnapshots = new BitSet(fst.numStates()*16);
		//currentSnapshots = new BitSet(inputTrieSize * fst.numStates() * 2);
		//currentSnapshotsByInput = new BitSet[inputTrieSize];
		reachedFinalStateAtInputId = new BitSet(inputTrieSize);
		reachedNonFinalStateAtInputId = new BitSet(inputTrieSize);
		//projection = new ArrayList<>(inputTrieSize);
		//projection = new PostingList[inputTrieSize];

		//currentSnapshotsByInput = new Int2ObjectOpenHashMap<>();
		//nodesWithOutput = new BitSet();

		relevantNodeSupports = new Int2LongOpenHashMap();
		clear();
	}

	void clear() {
		// clear the posting list
		itemFid = -1;
		//prefixSupport = 0;
		potentialSupport = 0;
		projectedDatabaseCurrentInputId = -1;
		currentInputId = -1;
		//reachedFinalCompleteState = false;
		//reachedNonFinalCompleteState = false;
		projectedDatabase.clear();
		reachedFinalStateAtInputId.clear();
		reachedNonFinalStateAtInputId.clear();
		//projection.clear();
		//projection = new PostingList[inputTrieSize];
		//currentSnapshotsByInput = new BitSet[inputTrieSize];
		//currentSnapshots.clear();
		//currentSnapshotsByInput.clear();
		//nodesWithOutput.clear();

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
		//currentSnapshots = null;
		//projection = null;
		//currentSnapshotsByInput = null;
		//nodesWithOutput = null;
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
			child.finalStateReached(inputNode);
		}else{
			//If not (non) final already reached...
			if(!child.reachedNonFinalStateAtInputId.get(inputNodeId)
					&& !child.reachedFinalStateAtInputId.get(inputNodeId)) {
				//..remember to avoid multiple processing...
				child.reachedNonFinalStateAtInputId.set(inputNodeId);
				//... check that not any ancestor/parent was recorded already...
				/*if(!child.reachedNonFinalStateAtInputId.intersects(inputNode.ancestors) ||
						!child.reachedFinalStateAtInputId.intersects(inputNode.ancestors))*/
					//... and increase the potential support
					child.potentialSupport += inputNode.getSupport();
			}

		}
		//Check if state can be expanded
		if(!state.isFinalComplete() &&
				!(state.isFinal() && inputNode.isLeaf() && inputNode.items.size() == position)){
			//Add all possible positions to projected database, which are worth to follow up
			addToProjectedDatabase(child, inputNodeId,position, state.getId());
		}


	}

	public void finalStateReached(PatriciaTrieBasic.TrieNode inputNode){
		final int inputNodeId = inputNode.getId();
		final long inputSupport = inputNode.getSupport();

		if(!reachedFinalStateAtInputId.get(inputNodeId)) {
			//handle a valid final state in FST -> remember it to avoid multiple processing of it
			reachedFinalStateAtInputId.set(inputNodeId);

			//Processing only necessary if no parent was processed already
			if (!reachedFinalStateAtInputId.intersects(inputNode.ancestors)) {
				//add support to prefix support
				//child.prefixSupport += inputSupport;
				//Remember support
				relevantNodeSupports.put(inputNodeId, inputSupport);


				//check if descendants were counted already and remove the support -> need to clean up
				//Flag in "reachedFinalStateAtInputId" can stay
				if (reachedFinalStateAtInputId.intersects(inputNode.descendants)) {
					BitSet intersect = (BitSet) reachedFinalStateAtInputId.clone();
					intersect.and(inputNode.descendants);
					//correct them (could be done after expand before recalculating support!)
					int nodeId = 0;
					while (nodeId >= 0) {
						nodeId = intersect.nextSetBit(nodeId); //returns -1 if none
						//Remove support
						if (nodeId > 0 && relevantNodeSupports.containsKey(nodeId)) {
							relevantNodeSupports.remove(nodeId);
							//child.reachedFinalStateAtInputId.set(inputNodeId, false);
						}
					}
				}

				//potentially correct partial (unconfirmed) support
				if (reachedNonFinalStateAtInputId.get(inputNodeId)) {
					potentialSupport -= inputSupport;
					//But correct only once! (since it is marked as final it will not be processed again)
					//reachedNonFinalStateAtInputId.set(inputNodeId, false);
				}

			}
		}
	}

	/** Add a snapshot to the projected database of the given child. Ignores duplicate snapshots. */
	private void addToProjectedDatabase(final DesqDfsPatriciaTreeNode child, final int inputId,
										// final long inputSupport,
										final int position, final int stateId) {
		assert stateId < fst.numStates();
		//assert child.possibleStates.get(stateId);
		//final int spIndex = position*fst.numStates() + stateId;
			/*	position * inputTrieSize * fst.numStates()
						+ inputId * fst.numStates()
						+ stateId;*/
		//Ensure that node + pos + state combination is recorded only once
		//if(!child.currentSnapshots.get(spIndex)){
			//child.currentSnapshots.set(spIndex);
		/*BitSet b = child.currentSnapshotsByInput.get(inputId);
		if(b == null){
			b = new BitSet();
			child.currentSnapshotsByInput.put(inputId,b);
		}else if(b.get(spIndex)) {
			//already recorded
			return;
		}
		b.set(spIndex);*/

		//Add stated id and position as new posting
		//Not the same approach as in DesqDfs, because input ids may reoccur unordered
		child.projectedDatabase.newPosting();
		child.projectedDatabase.addNonNegativeInt(inputId);
		child.projectedDatabase.addNonNegativeInt(stateId);
		child.projectedDatabase.addNonNegativeInt(position);

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
			if (child.getSupport() + child.potentialSupport < minSupport) {
				childrenIt.remove();
			}
		}
	}


}