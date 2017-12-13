package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.fst.*;
import de.uni_mannheim.desq.patex.PatExUtils;
import de.uni_mannheim.desq.util.DesqProperties;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;

public final class DesqPatricia extends MemoryDesqMiner {
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

    /** Stores iterators over output item/next state pairs for reuse. */
	private final ArrayList<State.ItemStateIterator> itemStateIterators = new ArrayList<>();

    /** An iterator over a projected database (a posting list) for reuse */
	private final PostingList.Iterator projectedDatabaseIt = new PostingList.Iterator();

	/** The root node of the search tree. */
	private final DesqDfsTreeNode root;

	// -- helper variables for pruning and twopass --------------------------------------------------------------------

	/** The DFA corresponding to the FST (pruning) or reverse FST (two-pass). */
	private final Dfa dfa;

    /** For each relevant input sequence, the sequence of states taken by dfa (two-pass only) */
	private final ArrayList<DfaState[]> dfaStateSequences;

    /** A sequence of EDFA states for reuse (two-pass only) */
	private final ArrayList<DfaState> dfaStateSequence;

    /** A sequence of positions for reuse (two-pass only) */
	private final IntList dfaInitialPos;

	// -- implicit arguments for incStep() ----------------------------------------------------------------------------

	/** The ID of the input sequence we are processing */
	//int currentInputId;

	/** The items in the input sequence we are processing */
	//WeightedSequence currentInputSequence;

	/** The state sequence of the accepting DFA run for the current intput sequence (two-pass only). */
	//DfaState[] currentDfaStateSequence;

	/** The node in the search tree currently being processed */
	//DesqDfsTreeNode currentNode;

	/** For each state/position pair, whether we have reached this state and position without further output
	 * already. Index of a pair is <code>pos*fst.numStates() + toState.getId()</code>.
	 */
	//BitSet currentSpReachedWithoutOutput = new BitSet();


	/**Trie representing the data **/
	private PatriciaItemTrie inputTrie; //stores the input data as patricia trie
	private PatriciaItemTrie outputTrie; //stores the mined pattern of FST in patricia trie


	//private IntList currentInput;
	private IntList currentPrefix;
	//private Long currentSupport;
	private HashSet<IntList> outputSequenceCache; //caches the mined sequences for one input


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


		// create two pass auxiliary variables (if needed)
		if (useTwoPass) { // two-pass
            // two-pass will always prune irrelevant input sequences, so notify the user when the corresponding
            // property is not set
			if (!pruneIrrelevantInputs) {
				logger.warn("property desq.mining.prune.irrelevant.inputs=false will be ignored because " +
						"desq.mining.use.two.pass=true");
			}

            // initialize helper variables for two-pass
			dfaStateSequences = new ArrayList<>();
			dfaStateSequence = new ArrayList<>();
			dfaInitialPos = new IntArrayList();
		} else { // invalidate helper variables for two-pass
            dfaStateSequences = null;
			dfaStateSequence = null;
			dfaInitialPos = null;
		}

		// create DFA or reverse DFA (if needed)
		if(useTwoPass) {
			// construct the DFA for the FST (for the first pass)
			// the DFA is constructed for the reverse FST
			this.dfa = Dfa.createReverseDfa(fst, ctx.dict, largestFrequentFid, true, useLazyDfa);
		} else if (pruneIrrelevantInputs) {
			// construct the DFA to prune irrelevant inputs
			// the DFA is constructed for the forward FST
			this.dfa = Dfa.createDfa(fst, ctx.dict, largestFrequentFid, false, useLazyDfa);
		} else {
			this.dfa = null;
		}

		// other auxiliary variables
		BitSet initialState = new BitSet(fst.numStates());
		initialState.set(fst.getInitialState().getId());
		root = new DesqDfsTreeNode(fst, initialState);
		//currentNode = root;

		// variables for patricia
		inputTrie = new PatriciaItemTrie();
		outputTrie = new PatriciaItemTrie();
		outputSequenceCache = new HashSet<>();
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

	public void clear() {
		inputSequences.clear();
        inputSequences.trimToSize();
		if (useTwoPass) {
			dfaStateSequences.clear();
            dfaStateSequences.trimToSize();
		}
		root.clear();
		//currentNode = root;
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
	}

	/**
	 * Traverse the patricia trie containing input data and run FST in parallel
	 * Start with FST after root node and hand it over to the children
	 * Stores each ouput of the FST in the output trie (with support of input trie node)
	 * Based on expand in DesqDFS
	 * //@param prefix current Prefix (mined pattern)
	 * @param itemPos item position in current node
	 * @param node current trie node
	 * @param state current state in fst
	 *
	 */
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
	}

   	// -- accessors to internal data structures (use with care) -------------------------------------------------------

	public Fst getFst() {
		return fst;
	}

	public Dfa getDfa() {
		return dfa;
	}
}
