package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.State;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.util.BitSet;
import java.util.Collections;
import java.util.concurrent.atomic.LongAdder;

/**
 * DesqDfsPatriciaTreeNode.java
 * @author Sascha Ulbrich (based on DesqDfsTreeNode by Kaustubh Beedkar)
 */
final class DesqDfsPatriciaTreeNode {

	public static LongAdder nodeCounter = new LongAdder();
	public static LongAdder pruneCounter = new LongAdder();

	// -- member variables --------------------------------------------------------------------------------------------

	/** the FST */
	final Fst fst;

	/** the input trie */
	final int inputTrieSize;


	/** The output item associated with this node */
	int itemFid;

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

	/** track if a final complete state was reached already **/
	BitSet reachedFCStateAtInputId ;

	/** Whether a non final state was reached already for this input node (partial support already recorded)**/
	BitSet reachedNonFCStateAtInputId;

	/** Collection of relevant input nodes, based on interval Ids */
	ObjectList<IntervalNode> relevantIntervalNodes;


	// -- construction and clearing -----------------------------------------------------------------------------------

	//DesqDfsPatriciaTreeNode(Fst fst, BitSet possibleStates, int inputTrieSize) {
	DesqDfsPatriciaTreeNode(Fst fst, int inputTrieSize) {
		nodeCounter.add(1);
		this.inputTrieSize = inputTrieSize;
		this.fst = fst;
		projectedDatabase = new PostingList();
		reachedFCStateAtInputId = new BitSet();//(inputTrieSize); //(inputTrieSize >> 6);
		reachedNonFCStateAtInputId = new BitSet();//(inputTrieSize);
		relevantIntervalNodes = new ObjectArrayList<>();
		clear();

	}

	void clear() {
		// clear the posting list
		itemFid = -1;
		potentialSupport = 0;
		projectedDatabaseCurrentInputId = -1;
		currentInputId = -1;
		projectedDatabase.clear();
		reachedFCStateAtInputId.clear();
		reachedNonFCStateAtInputId.clear();
		relevantIntervalNodes.clear();

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
		relevantIntervalNodes = null;
		reachedFCStateAtInputId = null;
		reachedNonFCStateAtInputId = null;
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
	void expandWithItem(final int outputFid, final PatriciaTrie.TrieNode inputNode,
						final int position, final State state) {

		//Get child node with corresponding fid
		DesqDfsPatriciaTreeNode child = childrenByFid.get(outputFid);
		if (child == null) {
			//If not existing -> create child node with corresponding fid
			child = new DesqDfsPatriciaTreeNode(fst, inputTrieSize);
			child.itemFid = outputFid;
			childrenByFid.put(outputFid, child);
		}

		final int inputNodeId = inputNode.getId();

		//Handle supports:
		if(!child.reachedFCStateAtInputId.get(inputNodeId)) { //FC -> full support
			if (state.isFinalComplete()) {
				child.recordRelevantNode(inputNode, true); //record it (instead of writing to projection)
				if (!child.reachedNonFCStateAtInputId.get(inputNodeId)) {
					//not counted in potential support yet -> add it
					child.potentialSupport += inputNode.getSupport();
				}
			} else if (!child.reachedNonFCStateAtInputId.get(inputNodeId)){
				//..remember to avoid multiple processing...
				child.reachedNonFCStateAtInputId.set(inputNodeId);
				//... and increase the potential support
				child.potentialSupport += inputNode.getSupport(); //(full support: all descendants might support it)
			}
		}
		//Check if state can be expanded
		if(!state.isFinalComplete()){
			//Add all possible positions to projected database, which are worth to follow up
			addToProjectedDatabase(child, inputNodeId,position, state.getId());
		}


	}

	//version for index based
	void expandWithItem(final int outputFid, final int inputNodeId,
						final int position, final State state, IndexPatriciaTrie trie) {

		//Get child node with corresponding fid
		DesqDfsPatriciaTreeNode child = childrenByFid.get(outputFid);
		if (child == null) {
			//If not existing -> create child node with corresponding fid
			child = new DesqDfsPatriciaTreeNode(fst, inputTrieSize);
			child.itemFid = outputFid;
			childrenByFid.put(outputFid, child);
		}

		//Handle support:
		if(!child.reachedFCStateAtInputId.get(inputNodeId)) {
			if (state.isFinalComplete()) {
				child.recordRelevantNode(inputNodeId, trie, true);
				if (!child.reachedNonFCStateAtInputId.get(inputNodeId)) {
					//not counted in potential support yet -> add it
					child.potentialSupport += trie.getSupport(inputNodeId);
				}
			} else if (!child.reachedNonFCStateAtInputId.get(inputNodeId)) {
				//..remember to avoid multiple processing...
				child.reachedNonFCStateAtInputId.set(inputNodeId);
				//... and increase the potential support
				child.potentialSupport += trie.getSupport(inputNodeId);
			}
		}
		//Check if state can be expanded
		if(!state.isFinalComplete()){
			//Add all possible positions to projected database, which are worth to follow up
			addToProjectedDatabase(child, inputNodeId,position, state.getId());
		}


	}

	public void recordRelevantNode(PatriciaTrie.TrieNode inputNode, boolean isFinalComplete){
		//handle a valid final state in FST -> remember it to avoid multiple processing of it
		if(isFinalComplete) reachedFCStateAtInputId.set(inputNode.getId());

		//Processing only necessary if no parent was processed already
		relevantIntervalNodes.add(new IntervalNode(
				inputNode.intervalStart,
				inputNode.intervalEnd,
				inputNode.support,
				inputNode.exclusiveSupport,
				isFinalComplete
		));
	}

	//Same for Index based
	public void recordRelevantNode(int nodeId, IndexPatriciaTrie trie, boolean isFinalComplete){
		//handle a valid final state in FST -> remember it to avoid multiple processing of it
		if(isFinalComplete) reachedFCStateAtInputId.set(nodeId);

		relevantIntervalNodes.add(
				trie.getIntervalNode(nodeId,isFinalComplete) //pre-calculated in index based version
		);
	}

	/** Add a snapshot to the projected database of the given child. Ignores duplicate snapshots. */
	private void addToProjectedDatabase(final DesqDfsPatriciaTreeNode child, final int inputId,
										// final long inputSupport,
										final int position, final int stateId) {
		//assert stateId < fst.numStates();
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

	/**
	 * Calculates the support of the prefix of this node
	 * Important: The support is only correct if this node was expanded already!
	 * Otherwise the non-final states which would reach a final state without output during expand are not considered
	 * @return support value
	 */
	public long getSupport() {
		LongAdder adder = new LongAdder();
		//Ensure that larger intervals (parent nodes) are processed before sub-intervals (child nodes)
		Collections.sort(relevantIntervalNodes);
		int skip = -1; //processed interval end
		IntervalNode last = null; //remember last processed interval (for de-duplication)
		for(IntervalNode node: relevantIntervalNodes){
			if(!node.equals(last) && node.start > skip){
				last = node;
				adder.add(
						(node.isFinalComplete) ? node.support : node.exclusiveSupport
				);
				if(node.isFinalComplete) skip = node.end ;
			}
		}
		return adder.longValue();
	}

	/** Removes all children that have a potential support below the given value of minSupport
	 * Note that the potential support is only an approximation which is >= the actual support.
	 * All input nodes are added without considering if a parent was added already
	 * AND it is not clear (before the expand) which non-final entries will actually reach a final state without output
	 * */
	void pruneInfrequentChildren(long minSupport) {
		ObjectIterator<Int2ObjectMap.Entry<DesqDfsPatriciaTreeNode>> childrenIt =
				childrenByFid.int2ObjectEntrySet().fastIterator();
		while (childrenIt.hasNext()) {
			Int2ObjectMap.Entry<DesqDfsPatriciaTreeNode> entry = childrenIt.next();
			final DesqDfsPatriciaTreeNode child = entry.getValue();
			if (child.potentialSupport < minSupport) {
				childrenIt.remove();
				pruneCounter.add(1);
			}
		}
	}


}
