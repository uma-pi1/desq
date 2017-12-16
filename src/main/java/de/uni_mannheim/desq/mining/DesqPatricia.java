package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.fst.*;
import de.uni_mannheim.desq.patex.PatExUtils;
import de.uni_mannheim.desq.util.DesqProperties;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.*;

public final class DesqPatricia extends DesqMiner {
	private static final Logger logger = Logger.getLogger(DesqPatricia.class);
	private static final boolean DEBUG = false;
	static {
		if (DEBUG) logger.setLevel(Level.TRACE);
	}

	// -- parameters for mining ---------------------------------------------------------------------------------------

	/** Minimum support */
    private final long sigma;

    /** The pattern expression used for mining */
	private final String patternExpression;

    /** If true, input sequences that do not match the pattern expression are pruned */
	private final boolean pruneIrrelevantInputs;

    /** If true, the two-pass algorithm for DesqDfs is used */
	private final boolean useTwoPass;


	// -- helper variables --------------------------------------------------------------------------------------------

    /** Stores the final state transducer for DesqDfs (one-pass) */
	private final Fst fst;

    /** Stores the largest fid of an item with frequency at least sigma. Zsed to quickly determine
     * whether an item is frequent (if fid <= largestFrequentFid, the item is frequent */
	private final int largestFrequentFid;


	// -- helper variables for pruning and twopass --------------------------------------------------------------------

	/** The DFA corresponding to the FST (pruning) or reverse FST (two-pass). */
	private final Dfa dfa;

  	/**Trie representing the data **/
	private PatriciaItemTrie inputTrie; //stores the input data as patricia trie
	private PatriciaItemTrie outputTrie; //stores the mined pattern of FST in patricia trie

	private PatriciaItemTrie.TrieNode currentOutputNode;
	private HashSet<PatriciaItemTrie.TrieNode> currentFinalNodesWithoutOutput;


	// -- construction/clearing ---------------------------------------------------------------------------------------

	public DesqPatricia(DesqMinerContext ctx) {
		super(ctx);
		sigma = ctx.conf.getLong("desq.mining.min.support");
		largestFrequentFid = ctx.dict.lastFidAbove(sigma);
		pruneIrrelevantInputs = ctx.conf.getBoolean("desq.mining.prune.irrelevant.inputs");
        useTwoPass = ctx.conf.getBoolean("desq.mining.use.two.pass");
		boolean useLazyDfa = ctx.conf.getBoolean("desq.mining.use.lazy.dfa");

		// create FST
		patternExpression = ctx.conf.getString("desq.mining.pattern.expression");
		this.fst = PatExUtils.toFst(ctx, patternExpression);

		if (pruneIrrelevantInputs) {
			// construct the DFA to prune irrelevant inputs
			// the DFA is constructed for the forward FST
			this.dfa = Dfa.createDfa(fst, ctx.dict, largestFrequentFid, false, useLazyDfa);
		} else {
			this.dfa = null;
		}

		// variables for patricia
		inputTrie = new PatriciaItemTrie();
		outputTrie = new PatriciaItemTrie();
		currentFinalNodesWithoutOutput = new HashSet<>();
	}

	public static DesqProperties createConf(String patternExpression, long sigma) {
		DesqProperties conf = new DesqProperties();
		conf.setProperty("desq.mining.miner.class", DesqPatricia.class.getCanonicalName());
		conf.setProperty("desq.mining.min.support", sigma);
		conf.setProperty("desq.mining.pattern.expression", patternExpression);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		conf.setProperty("desq.mining.use.lazy.dfa", false);
		conf.setProperty("desq.mining.use.two.pass", true);
		conf.setProperty("desq.mining.optimize.permutations",true);
		return conf;
	}


	// -- processing input sequences ----------------------------------------------------------------------------------

	@Override
	public void addInputSequence(IntList inputSequence, long inputSupport, boolean allowBuffering) {
        //Represent complete data structure as patricia trie, but prune already (irrelevant input)
		//Prune infrequent item (sorted by fid ascending!)
		// pruning input sequences is tricky because they could generate (potentially) frequent generalized items
		//this only works if sorted! -> one more reason to remove it
		/*for(int i = 0;i < inputSequence.size(); i++){
			if(inputSequence.getInt(i) > largestFrequentFid){
				if(i == 0) return;
				inputSequence = inputSequence.subList(0,i);
				break;
			}
		}*/

		// one-pass
		if (!pruneIrrelevantInputs || dfa.accepts(inputSequence)) {
			// if we reach this place, we either don't want to prune irrelevant inputs or the input is relevant
			// -> remember it
			inputTrie.addItems(inputSequence);
		}
	}


    // -- mining ------------------------------------------------------------------------------------------------------

	@Override
	public void mine() {
		//pruned input data is stored in patricia trie -> traverse the inputTrie and find frequent input sets
		if (DEBUG) {
			System.out.println("Trie size:" + inputTrie.size()
					+ "; Root Support: " + inputTrie.getRoot().getSupport()
					+ "; 1st Level Children: " + inputTrie.getRoot().childrenCount());
			inputTrie.exportGraphViz("inputTrie.pdf", ctx.dict, 5);
			fst.exportGraphViz("fst.pdf");
		}
		if((inputTrie.getRoot().getSupport() >= sigma) && !inputTrie.getRoot().isLeaf()) {
			//start pattern growth
			//init root node
			PatriciaItemTrie.TrieNode outputRoot = outputTrie.getRoot();
			outputRoot.addProducer(
					new PatriciaItemTrie.Producer(
							fst.getInitialState(),
							inputTrie.getRoot(),
							0, sigma, false, false
			));
			//init root support of output trie with root support of input trie (count of input sequences)
			outputRoot.incSupport(inputTrie.getRoot().getSupport());

			//Start recursive expansion
			expand(outputRoot);

			outputRoot.trim(sigma, true, false);

			//input trie not needed anymore -> free space
			inputTrie = null;

			//lookup the mines patterns from the output trie
			if((outputTrie.getRoot().getSupport() >= sigma) && !outputTrie.getRoot().isLeaf()) {
				//start recursive trie traversal
				traverseOutputTrie(new IntArrayList(), outputTrie.getRoot());
			}
		}
		if (DEBUG) {
			System.out.println("Trie size:" + outputTrie.size()
					+ "; Root Support: " + outputTrie.getRoot().getSupport()
					+ "; 1st Level Children: " + outputTrie.getRoot().childrenCount());
			outputTrie.exportGraphViz("outputTrie.pdf", ctx.dict, 3);
		}
	}

	/**
	*Expand the given output Trie node by doing next step iteration from all relevant states
	 */

	private void expand(PatriciaItemTrie.TrieNode node){
		//correct support by tracking what was removed in child nodes

		//check if node itself has sufficient support
		if(node.support < sigma){ return; }

		currentOutputNode = node;
		currentFinalNodesWithoutOutput.clear();
		long finalWithoutOutput = 0;
		//Extend all possible states from this node and generate next output
		for(PatriciaItemTrie.Producer p: node.producers){
			//TODO: dfa based checks if final state is reachable? - or in state iterator in step?
			if(!p.isComplete) {
				finalWithoutOutput += step(p.state, p.node, p.itemPos);
			}
		}
		//Node not yet final but reaches final state(s) (with sufficient support)?
		if(!currentOutputNode.isFinal() && finalWithoutOutput > sigma){
			currentOutputNode.setFinal(true);
			currentOutputNode.incSupport(finalWithoutOutput - currentOutputNode.getSupport());
		}

		//Trim this node:
		// - remove infrequent items
		// - merge with child (if there is only one left)
		// - adjust support
		// - Not possible: prune based on relevant output -> might not be reached yet
		currentOutputNode.trim(sigma, false, true);

		//Recursively extend all sufficient frequent child nodes (just created)
		IntList prune = new IntArrayList(node.childrenCount());
		for(PatriciaItemTrie.TrieNode child: node.collectChildren()) {

			if (child.support >= sigma) {
				expand(child);
				//After expansion of child (due to recursion child is fully expanded!)
				//Pruning:
				// - remove children which are not frequent anymore
				if(child.support < sigma){
					prune.add(child.firstItem());
				}
				// - remove children which are not relevant
				if(!child.isRelevant() && !child.isFinal()){
					prune.add(child.firstItem());
				}
			}
		}
		//execute pruning
		if(prune.size() > 0)
			node.removeChildrenByFirstItem(prune);

		//check relevant itself (never remove a isRelevant = true)
		if(!node.isRelevant
				&& (node.childrenCount() > 0 || node.isFinal())
				&& node.support >= sigma)
			node.setRelevant(true);
	}



	/**
	 * Add next output to currentOutputNode
	 * Start from given state in given node-item and expand till next output or finalCompleteState
	 *
	 * @param state start state of FST
	 * @param node start node of input trie to expand from
	 * @param pos item position in start node
	 */

	private long step(State state, PatriciaItemTrie.TrieNode node, int pos){

		//Check if Fst at final complete state (without valid output)
		if (state.isFinalComplete()){
			return handleSupportOfFinalWithoutOutput(node, state);
		}

		if(pos == node.getItems().size()) {
			//Helper: return the support of reached final states without output
			long finalWithoutOutput = handleSupportOfFinalWithoutOutput(node, state);
			//No more items in node -> proceed to next trie nodes (if no leaf already)
			if(!node.isLeaf()) {
				for (PatriciaItemTrie.TrieNode childNode : node.collectChildren()) {
					finalWithoutOutput += step(state, childNode, 0);
				}
			}
			return finalWithoutOutput;
		}

		// print debug information
		if (DEBUG) {
			logger.trace("Processing " + node.toString(ctx.dict) + ", itemPos=" + pos + ", nodeId="
					+ node.getId() + ", state=" + state.getId());
		}


		//Actual processing of items in node
		State.ItemStateIterator itemStateIt = new State.ItemStateIterator(ctx.dict.isForest());
		state.consume(node.getItems().getInt(pos), itemStateIt);
		long finalWithoutOutput = handleSupportOfFinalWithoutOutput(node, state);
		while(itemStateIt.hasNext()) {
			final ItemState itemState = itemStateIt.next();
			final State toState = itemState.state;
			final int outputItemFid = itemState.itemFid;
			final int nextItemPos = pos + 1;

			if(outputItemFid == 0) { // No output
				//proceed with next input item
				finalWithoutOutput += step(toState, node, nextItemPos);
			} else {
				// we got an output! -> check whether it is relevant
				if (largestFrequentFid >= outputItemFid) {
					// now append this to the current output node
					// handover producer only if further processing from this state makes sense
					// support is just intermediate (might be to high but not to low)
					boolean isFinal = toState.isFinal() && node.isFinal(); //valid final state AND end of a sequence
					boolean isComplete = toState.isFinalComplete() ||
							( node.isLeaf() && nextItemPos == node.items.size()); //no more processing
					outputTrie.appendItem(currentOutputNode,outputItemFid, node.support,
							new PatriciaItemTrie.Producer(toState, node, nextItemPos, node.support, isFinal, isComplete ),
							isFinal);

					if (DEBUG) {
						logger.trace("Output: " + ctx.dict.sidOfFid(outputItemFid) + "@" + node.support
								+ " -> appended to " + currentOutputNode.toString(ctx.dict));
					}
				}//else: infrequent -> no further processing of FST from this state, check next state (loop)
			}
		}
		return finalWithoutOutput;
	}

	private long handleSupportOfFinalWithoutOutput(PatriciaItemTrie.TrieNode node, State state){
		if(state.isFinal() && !currentFinalNodesWithoutOutput.contains(node)){
			currentFinalNodesWithoutOutput.add(node);
			return node.getExclusiveSupport();
		}
		else return 0;
	}

	private void traverseOutputTrie(IntList prefix, PatriciaItemTrie.TrieNode node){
		//no children -> store result
		if(node.isLeaf() && node.isFinal()){
			ctx.patternWriter.write(prefix, node.getSupport());

		}else if(!node.isLeaf()){
			//int sumSupport = 0;
			for(PatriciaItemTrie.TrieNode child: node.collectChildren()){
				//keep track of total child support
				//sumSupport += node.getSupport();
				//process child trie only if support is above or equal min support
				if(child.getSupport() >= sigma){
					//New recursion step -> copy item list per branch and add already child's items
					IntList newPrefix = new IntArrayList(prefix);
					newPrefix.addAll(child.getItems());
					traverseOutputTrie(newPrefix,child);
				}
			}
			//if(node.getSupport() - sumSupport) >= sigma{
			//Case: valid pattern within trie (not at leaf node)
			if(node.isFinal() && node.getSupport() >= sigma){
				//the node itself is not a leaf, but defines a valid pattern already!
				ctx.patternWriter.write(prefix, node.getSupport());
			}
		}
	}


	//-------------------- OLD

	/*
	@Override
	public void mine() {
		//pruned input data is stored in patricia trie -> traverse the inputTrie and find frequent input sets
		System.out.println("Node#:" + PatriciaItemTrie.nodeCounter + " - " + inputTrie.getRoot().collectChildren().size());
		if((inputTrie.getRoot().getSupport() >= sigma) && !inputTrie.getRoot().isLeaf()) {
			//start recursive trie traversal and write mined patterns in output trie
			currentPrefix = new IntArrayList();
			traverseTrieWithFST(0, inputTrie.getRoot(), fst.getInitialState());
			//input trie not needed anymore -> free space
			//inputTrie.exportGraphViz("inputTrie.pdf", ctx.dict, 1);
			inputTrie = null;

			//lookup the mines patterns from the output trie
			if((outputTrie.getRoot().getSupport() >= sigma) && !outputTrie.getRoot().isLeaf()) {
				//start recursive trie traversal
				traverseOutputTrie(new IntArrayList(), outputTrie.getRoot());
			}
		}
		System.out.println("Node#:" + PatriciaItemTrie.nodeCounter + " - " + outputTrie.getRoot().collectChildren().size());
		//outputTrie.exportGraphViz("outputTrie.pdf", ctx.dict, 1);
	}*/



	/**
	 * Traverse the patricia trie containing input data and run FST in parallel
	 * Start with FST after root node and hand it over to the children
	 * Stores each ouput of the FST in the output trie (with support of input trie node)
	 * Based on expand in DesqDFS
	 * //@param prefix current Prefix (mined pattern)
	 * //@param itemPos item position in current node
	 * //@param node current trie node
	 * //@param state current state in fst
	 *
	 *
	private void traverseTrieWithFST(int itemPos, PatriciaItemTrie.TrieNode node, State state){

		// print debug information
		if (DEBUG) {
			logger.trace("Processing " + node.toString(ctx.dict) + ", itemPos=" + itemPos + ", nodeSupport="
					+ node.getSupport() + ", state=" + state.getId());
		}


		//Check if Fst at final complete state OR consumed a sequence (of a node) completely
		if (state.isFinalComplete() || itemPos == node.getItems().size()) {
			//Check if valid output (not all nodes might represent an end of a sequence)
			if (!currentPrefix.isEmpty() && state.isFinal() && node.isFinal()) {
				outputTrie.addItems(currentPrefix, node.getSupport());
				if (DEBUG) {
					logger.trace("Output " + ctx.dict.sidsOfFids(currentPrefix) + " @ " + node.getSupport());
				}
			}
			if(!state.isFinalComplete() && !node.isLeaf()){
				//Just no more items -> proceed to next trie node
				for(PatriciaItemTrie.TrieNode childNode: node.collectChildren()) {
					traverseTrieWithFST(0, childNode, state);
				}
			}
			return;
		}


		//Actual processing of items in node
		State.ItemStateIterator itemStateIt = new State.ItemStateIterator(ctx.dict.isForest());
		state.consume(node.getItems().getInt(itemPos), itemStateIt);
		while(itemStateIt.hasNext()) {
			final ItemState itemState = itemStateIt.next();
			final State toState = itemState.state;
			final int outputItemFid = itemState.itemFid;
			final int nextItemPos = itemPos + 1;

			if(outputItemFid == 0) { // No output
				//proceed with next input
				traverseTrieWithFST(nextItemPos, node, toState);
			} else {
				// we got an output! -> check whether it is relevant
				if (largestFrequentFid >= outputItemFid) {
					// now append this item to the prefix, continue running the FST/Trie, and remove the item once done
					currentPrefix.add(outputItemFid);
					traverseTrieWithFST(nextItemPos, node, toState);
					currentPrefix.removeInt(currentPrefix.size() - 1);
				}//else: infrequent -> no further processing of FST from this state, check next state (loop)
			}
		}
	}

	private void traverseOutputTrie(IntList prefix, PatriciaItemTrie.TrieNode node){
		if(node.isLeaf()){
			//no children -> store result
			ctx.patternWriter.write(prefix, node.getSupport());
		}else{
			int sumSupport = 0;
			for(PatriciaItemTrie.TrieNode child: node.collectChildren()){
				//keep track of total child support
				sumSupport += node.getSupport();
				//process child trie only if support is above or equal min support
				if(child.getSupport() >= sigma){
					//New recursion step -> copy item list per branch and add already child's items
					IntList newPrefix = new IntArrayList(prefix);
					newPrefix.addAll(child.getItems());
					traverseOutputTrie(newPrefix,child);
				}
			}
			if((node.getSupport() - sumSupport) >= sigma){
				//the node itself is not a leaf, but defines a valid pattern already!
				ctx.patternWriter.write(prefix, node.getSupport());
			}
		}
	}*/

   	// -- accessors to internal data structures (use with care) -------------------------------------------------------

	public Fst getFst() {
		return fst;
	}

	public Dfa getDfa() {
		return dfa;
	}
}
