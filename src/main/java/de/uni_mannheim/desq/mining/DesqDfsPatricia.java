package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.experiments.MetricLogger;
import de.uni_mannheim.desq.fst.*;
import de.uni_mannheim.desq.patex.PatExUtils;
import de.uni_mannheim.desq.util.DesqProperties;
import de.uni_mannheim.desq.util.IntBitSet;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.AbstractObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.collections.iterators.ArrayListIterator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.*;

public final class DesqDfsPatricia extends DesqMiner {
	private static final Logger logger = Logger.getLogger(DesqDfsPatricia.class);
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
	//private final boolean useTwoPass;


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
	//private final PostingList.Iterator postingListIt = new PostingList.Iterator();
	//private ListIterator<PostingList> projectionIt;

	/** The root node of the search tree. */
	private DesqDfsPatriciaTreeNode root;

	// -- helper variables for pruning and twopass --------------------------------------------------------------------

	/** The DFA corresponding to the FST (pruning) or reverse FST (two-pass). */
	private final Dfa dfa;

    /** For each relevant input sequence, the sequence of states taken by dfa (two-pass only) */
	//private final ArrayList<DfaState[]> dfaStateSequences;

    /** A sequence of EDFA states for reuse (two-pass only) */
	//private final ArrayList<DfaState> dfaStateSequence;

    /** A sequence of positions for reuse (two-pass only) */
	//private final IntList dfaInitialPos;

	// -- implicit arguments for incStep() ----------------------------------------------------------------------------

	/** The ID of the input trie node we are processing */
	private int currentInputId;

	/** The items in the input trie we are processing */
	//private PatriciaTrieBasic.TrieNode currentInputNode;

	/** The state sequence of the accepting DFA run for the current intput sequence (two-pass only). */
	//DfaState[] currentDfaStateSequence;

	/** The node in the search tree currently being processed */
	private DesqDfsPatriciaTreeNode currentNode;

	/** For each state/position pair, whether we have reached this state and position without further output
	 * already. Index of a pair is <code>pos*fst.numStates() + toState.getId()</code>.
	 */
	//private BitSet currentSpReachedWithoutOutput = new BitSet();

	/**Trie representing the data **/
	private PatriciaTrieBasic inputTrie; //stores the input data as patricia trie

	//private BitSet nodeReachedAsFinalWithoutOutput;

	//private Int2ObjectMap<BitSet> reachedNodesWithoutOutput = new Int2ObjectOpenHashMap<>();
	private BitSet reachedNodesWithoutOutput = new BitSet();

	private IntSet startNodesReachedNodesWithoutOutput = new IntBitSet();

	//private final BitSet visitedIndices = new BitSet();
	private final Int2ObjectMap<BitSet> visitedIndices = new Int2ObjectOpenHashMap<>();

	// -- construction/clearing ---------------------------------------------------------------------------------------

	public DesqDfsPatricia(DesqMinerContext ctx) {
		super(ctx);
		sigma = ctx.conf.getLong("desq.mining.min.support");
		largestFrequentFid = ctx.dict.lastFidAbove(sigma);
		pruneIrrelevantInputs = ctx.conf.getBoolean("desq.mining.prune.irrelevant.inputs");
        //useTwoPass = ctx.conf.getBoolean("desq.mining.use.two.pass");
		boolean useLazyDfa = ctx.conf.getBoolean("desq.mining.use.lazy.dfa");

		// create FST
		patternExpression = ctx.conf.getString("desq.mining.pattern.expression");
		this.fst = PatExUtils.toFst(ctx, patternExpression);

		// create two pass auxiliary variables (if needed)
		/*if (useTwoPass) { // two-pass
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
		} else if (pruneIrrelevantInputs) {*/
		if (pruneIrrelevantInputs){
			// construct the DFA to prune irrelevant inputs
			// the DFA is constructed for the forward FST
			this.dfa = Dfa.createDfa(fst, ctx.dict, largestFrequentFid, false, useLazyDfa);
		} else {
			this.dfa = null;
		}

		// other auxiliary variables

		inputTrie = new PatriciaTrieBasic(false);

		/*//Init after trie is built!
		BitSet initialState = new BitSet(fst.numStates());
		initialState.set(fst.getInitialState().getId());
		root = new DesqDfsPatriciaTreeNode(fst, initialState);//,inputTrie.size()); size needs to be corrected after trie is built
		currentNode = root;*/



	}

	public static DesqProperties createConf(String patternExpression, long sigma) {
		DesqProperties conf = new DesqProperties();
		conf.setProperty("desq.mining.miner.class", DesqDfsPatricia.class.getCanonicalName());
		conf.setProperty("desq.mining.min.support", sigma);
		conf.setProperty("desq.mining.pattern.expression", patternExpression);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		conf.setProperty("desq.mining.use.lazy.dfa", false);
		conf.setProperty("desq.mining.use.two.pass", false);
		conf.setProperty("desq.mining.optimize.permutations",true);
		return conf;
	}

	public void clear() {
        inputTrie.clear();

		/*if (useTwoPass) {
			dfaStateSequences.clear();
            dfaStateSequences.trimToSize();
		}*/
		root.clear();
		currentNode = root;
		visitedIndices.clear();
		//currentSpReachedWithoutOutput.clear();
	}

	// -- processing input sequences ----------------------------------------------------------------------------------

	@Override
	public void addInputSequence(IntList inputSequence, long inputSupport, boolean allowBuffering) {
        // two-pass version of DesqDfs
        /*if (useTwoPass) {
            // run the input sequence through the DFA and compute the state sequences as well as the positions from
            // which a final FST state is reached
			if (dfa.acceptsReverse(inputSequence, dfaStateSequence, dfaInitialPos)) {
			    // we now know that the sequence is relevant; remember it
				//super.addInputSequence(inputSequence, inputSupport, allowBuffering);
				inputTrie.addItems(inputSequence,inputSupport);
				while (itemStateIterators.size() < inputSequence.size())
					itemStateIterators.add(new State.ItemStateIterator(ctx.dict.isForest()));
				dfaStateSequences.add(dfaStateSequence.toArray(new DfaState[dfaStateSequence.size()]));

				// clean up
				dfaInitialPos.clear();
			}
			dfaStateSequence.clear();
            return;
		}*/
		// one-pass version of DesqDfs
		if (!pruneIrrelevantInputs || dfa.accepts(inputSequence)) {
			// if we reach this place, we either don't want to prune irrelevant inputs or the input is relevant
            // -> remember it
			inputTrie.addItems(inputSequence,inputSupport);//,largestFrequentFid);
			while (itemStateIterators.size() < inputSequence.size())
				itemStateIterators.add(new State.ItemStateIterator(ctx.dict.isForest()));

		}
	}

    // -- mining ------------------------------------------------------------------------------------------------------

	@Override
	public void mine() {
		/*System.out.println("#Trie Nodes:" + inputTrie.size()
				+ "; Root Support: " + inputTrie.getRoot().getSupport()
				+ "; 1st Level Children: " + inputTrie.getRoot().childrenCount()
				+ "; #Fst States: " + fst.numStates()
				+ "; Avg child count in 1st Level: "
				+ inputTrie.getRoot().getChildren().stream().mapToInt(child -> child.getChildren().size()).sum() / inputTrie.getRoot().childrenCount()
		);*/

		//ensure intervals are present in trie
		inputTrie.getRoot().calculateIntervals(0);

		if (DEBUG) {
			inputTrie.exportGraphViz("inputTrie.pdf", ctx.dict, 5);
			fst.exportGraphViz("fst.pdf");
		}

		//Init Mining
		//BitSet initialState = new BitSet(fst.numStates());
		//initialState.set(fst.getInitialState().getId());
		//input trie size needs to be set after trie is built
		root = new DesqDfsPatriciaTreeNode(fst, inputTrie.size());
		currentNode = root;

		//First IncStep (only possible after complete input trie is built)
		// run the first incStep; start at all positions from which a final FST state can be reached
		//nodeReachedAsFinalWithoutOutput = new BitSet(inputTrie.size());

		//currentInputId = inputSequences.size()-1;
		//currentInputSequence = inputSequences.get(currentInputId);
		if((inputTrie.getRoot().getSupport() >= sigma) && !inputTrie.getRoot().isLeaf()) {
			reachedNodesWithoutOutput.clear();
			for(PatriciaTrieBasic.TrieNode node: inputTrie.getRoot().getChildren()) {
			/*if (useTwoPass) {
				currentDfaStateSequence = dfaStateSequences.get(currentInputId);
				currentSpReachedWithoutOutput.clear();
				for (int i = 0; i < dfaInitialPos.size(); i++) {
					// for those positions, start with the initial state
					incStep(dfaInitialPos.getInt(i), fst.getInitialState(), 0, true);
				}
			} else {*/
				// and run the first inc step
				//currentSpReachedWithoutOutput.clear();
				//nodeReachedAsFinalWithoutOutput.clear();

				incStep(0, fst.getInitialState(), 0, true, node, false);
				//determineSupport(inputTrie.getRoot());
				//}
			}

			//Proceed as in standard DFS

			if (inputTrie.getRoot().getSupport() >= sigma) {
				// the root has already been processed; now recursively grow the patterns
				root.pruneInfrequentChildren(sigma);
				expand(new IntArrayList(), root);
			}
		}
	}

    /** Updates the projected databases of the children of the current node corresponding
	 * to each possible next output item for the current input sequence.
     *
     * @param pos next item to read
     * @param state current FST state
     * @param level recursion level (used for reusing iterators without conflict)
	 * @param expand if an item is produced, whether to add it to the corresponding child node
     *
     * @return true if the FST can accept without further output
     */
	private void incStep(int pos, State state, final int level, final boolean expand, PatriciaTrieBasic.TrieNode node, boolean trackWithoutOutput) {
		//boolean reachedFinalStateWithoutOutput = false; //only changed by FST transitions -> refers to same input node!

pos: 	do { // loop over positions; used for tail recursion optimization -> on trie not linear anymore -> recursion needs to split

			//Check if final state reached and if this fact should be recorded
			if(state.isFinal() && trackWithoutOutput) {

				trackWithoutOutput = false; //this node is captured, ignore children
				//reachedFinalStateWithoutOutput |= true; //cannot be overwritten
				currentNode.finalStateReached(node);

			}

			//If Fst reached final complete state -> exit
			if (state.isFinalComplete()){
				return;// reachedFinalStateWithoutOutput;
			}

			//Handle end of input trie node (proceed to child nodes if possible)
			if(pos == node.getItems().size()) {
				//Check if input trie node is leaf (no children) -> end of processing
				if(node.isLeaf()) {
					return;// reachedFinalStateWithoutOutput;
				}else{
					//No more items in node -> proceed to child trie node(s)

					final Iterator<PatriciaTrieBasic.TrieNode> it = node.getChildren().iterator();
					//ObjectIterator<Int2ObjectMap.Entry<PatriciaTrieBasic.TrieNode>> it = node.getChildrenIterator();
					//AbstractObjectIterator<PatriciaTrieBasic.TrieNode> it = node.getChildrenIterator(largestFrequentFid);
					//if (!it.hasNext()) return state.isFinal();
					while (it.hasNext()) {

						final PatriciaTrieBasic.TrieNode child = it.next();//.getValue();
//						MetricLogger.getInstance().addToSum(MetricLogger.Metric.NumberNodeMoves,1);
						if(it.hasNext()) {
							//Summarize returned support, because each node can reach final state independently
							//reachedFinalStateWithoutOutput |=
									incStep(0, state, level, expand, child, trackWithoutOutput);
						}else{
							node = child;
							pos = 0;
							//Proceed ...
						}
					}
				}
			}


			// get iterator over next output item/state pairs; reuse existing ones if possible
			// in two-pass, only iterates over states that we saw in the first pass (the other ones can safely be skipped)
			final int itemFid = node.getItems().getInt(pos);
			/*final BitSet validToStates = useTwoPass
					? currentDfaStateSequence[currentInputSequence.size() - (pos + 1)].getFstStates() // only states from first pass
					: null; // all states
			final State.ItemStateIterator itemStateIt = state.consume(itemFid, itemStateIterators.get(level), validToStates);*/
			final State.ItemStateIterator itemStateIt = state.consume(itemFid, itemStateIterators.get(level), null);

			// iterate over output item/state pairs and remember whether we hit the final or finalComplete state without producing output
			// (i.e., no transitions or only transitions with epsilon output)
itemState:	while (itemStateIt.hasNext()) { // loop over elements of itemStateIt; invariant that itemStateIt.hasNext()
				final ItemState itemState = itemStateIt.next();
				final int outputItemFid = itemState.itemFid;
				final State toState = itemState.state;
//				MetricLogger.getInstance().addToSum(MetricLogger.Metric.NumberFstTransitions,1);

				if (outputItemFid == 0) { // EPS output
					// we did not get an output
					// in the two pass algorithm, we don't need to consider empty-output paths that reach the initial state
					// because we'll start from those positions later on anyway. Those paths are only possible
					// in DesqDfs when we expand the empty prefix (equiv. current node is root)
					// NOT NEEDED ANYMORE (covered by indexing below)
					// if (useTwoPass && current.node==root && toState == fst.getInitialState()) {
					//	continue;
					// }

					// if we saw this state at this position without output (for this input sequence and for the currently
					// expanded node) before, we do not need to process it again
					//CANNOT PRUNE HERE IF TRIE -> after this node many other sequences can follow
					//int spIndex =  pos * fst.numStates() + toState.getId();
					//int spIndex =  (pos * fst.numStates() * inputTrie.size()) + (node.getId()*fst.numStates())  + toState.getId();
					//if (!currentSpReachedWithoutOutput.get(spIndex)) {
						// haven't seen it, so process
						//currentSpReachedWithoutOutput.set(spIndex);
						if (itemStateIt.hasNext()) {
							// recurse over FST states -> stays within same input node (but might change in next step)
							//reachedFinalStateWithoutOutput |=
									incStep(pos + 1, toState, level + 1, expand, node,trackWithoutOutput);
							continue itemState;
						} else {
							// tail recurse
							state = toState;
							pos++;
							continue pos;
						}
					//}
				} else if (expand && largestFrequentFid >= outputItemFid) {
					// we have an output and its frequent, so update the corresponding projected database
					/*if(toState.isFinal()){
						currentNodeReachedNodesWithoutOutput.set(node.getId());
						reachedFinalStateWithoutOutput = true;
					}*/
					currentNode.expandWithItem(outputItemFid, node,pos + 1, toState);

				}
				continue itemState;
			}

			break; // skipped only by call to "continue pos" above (tail recursion optimization)
		} while (true);

		return;// reachedFinalStateWithoutOutput;
	}

    /** Expands all children of the given search tree node. The node itself must have been processed/output/expanded
     * already.
     *
     * @param prefix (partial) output sequence corresponding to the given node (must remain unmodified upon return)
     * @param node the node whose children to expand
     */

	private void expand(IntList prefix, DesqDfsPatriciaTreeNode node) {
//		MetricLogger.getInstance().addToSum(MetricLogger.Metric.NumberExpands,1);
		// add a placeholder to prefix for the output item of the child being expanded
		final int lastPrefixIndex = prefix.size();
		prefix.add(-1);

		// iterate over all children
		for (final DesqDfsPatriciaTreeNode childNode : node.childrenByFid.values() )  {
			//assert childNode.partialSupport + childNode.prefixSupport >= sigma;

			// set the current (partial) output sequence
			prefix.set(lastPrefixIndex, childNode.itemFid);

			// print debug information
			if (DEBUG) {
				logger.trace("Expanding " + prefix
						+ ", potential support=" + childNode.potentialSupport
						//+ ", prefix support=" + childNode.prefixSupport
						//+ ", possible states=" + childNode.possibleStates
				);
			}

			//boolean expand = childNode.getSupport() >= sigma; // no expand -> just find finals without output
			projectedDatabaseIt.reset(childNode.projectedDatabase);
			reachedNodesWithoutOutput.clear();
			startNodesReachedNodesWithoutOutput.clear();
			visitedIndices.clear();

			//projectionIt = Arrays.asList(childNode.projection).listIterator();
			//currentInputId = -1;
			currentNode = childNode;
			//boolean reachedFinalStateWithoutOutput = false;

			//currentSpReachedWithoutOutput.clear();

			if(projectedDatabaseIt.hasNext()) {

				do {
					currentInputId = projectedDatabaseIt.nextNonNegativeInt();


					final PatriciaTrieBasic.TrieNode currentInputNode = inputTrie.getNodeById(currentInputId);
					final int stateId = projectedDatabaseIt.nextNonNegativeInt();
					final int pos = projectedDatabaseIt.nextNonNegativeInt();
					//reachedFinalStateWithoutOutput |=
							incStep(pos, fst.getState(stateId), 0, true,
									currentInputNode,!currentNode.reachedFinalStateAtInputId.get(currentInputId));

				} while (projectedDatabaseIt.nextPosting());
			}



			// output the pattern for the current child node if it turns out to be frequent
			long support = currentNode.getSupport(); //calculateSupport();
			if (support >= sigma) {
				if (ctx.patternWriter != null) {
					ctx.patternWriter.write(prefix, support);
				}
			}

			// expand the child node
			childNode.pruneInfrequentChildren(sigma);
			childNode.projectedDatabase = null; // not needed anymore

			expand(prefix, childNode);
			childNode.invalidate(); // not needed anymore
		}

		// we are done processing the node, so remove its item from the prefix
		prefix.removeInt(lastPrefixIndex);
	}

	// -- Helper for support determination
/*
	private long calculateSupport(){
		//Consider support values except non-final
		long support = 0;
		for(int key: currentNode.relevantNodeSupports.keySet()){
			if(!currentNode.reachedNonFinalStateAtInputId.get(key)){
				support += currentNode.relevantNodeSupports.get(key);
			}
		}
		return support;
	}

	private void checkTrie(Int2LongOpenHashMap relevantNodeSupports, PatriciaTrieBasic.TrieNode node){
		if(relevantNodeSupports.containsKey(node.getId())){
			relevantNodeSupports.remove(node.getId());
		}else{
			for(PatriciaTrieBasic.TrieNode child: node.getChildren()){
				checkTrie(relevantNodeSupports, child);
			}
		}
	}
	*/

	// -- accessors to internal data structures (use with care) -------------------------------------------------------

	public Fst getFst() {
		return fst;
	}

	public Dfa getDfa() {
		return dfa;
	}
}
