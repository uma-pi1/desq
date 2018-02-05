package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.experiments.MetricLogger;
import de.uni_mannheim.desq.fst.*;
import de.uni_mannheim.desq.patex.PatExUtils;
import de.uni_mannheim.desq.util.DesqProperties;
import de.uni_mannheim.desq.util.IntBitSet;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.*;

public final class DesqDfsPatricia extends DesqMiner {
	private static final Logger logger = Logger.getLogger(DesqDfsPatricia.class);
	private static final boolean DEBUG = false;
	private static final boolean logRuntime = true; //not performance critical
	private static final boolean logMetrics = false; //performance impact!
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

	/** The root node of the search tree. */
	private DesqDfsPatriciaTreeNode root;

	// -- helper variables for pruning --------------------------------------------------------------------

	/** The DFA corresponding to the FST (pruning) or reverse FST (two-pass). */
	private final Dfa dfa;

 	// -- implicit arguments for incStep() ----------------------------------------------------------------------------

		/** The node in the search tree currently being processed */
	private DesqDfsPatriciaTreeNode currentNode;

	/** For each state/position pair, whether we have reached this state and position without further output
	 * already. Index of a pair is <code>pos*fst.numStates() + toState.getId()</code>.
	 */
	//private BitSet currentSpReachedWithoutOutput = new BitSet();

	// -- patricia trie
	/**Trie representing the data **/
	private PatriciaTrie inputTrie; //stores the input data as patricia trie

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

		if (pruneIrrelevantInputs){
			// construct the DFA to prune irrelevant inputs
			// the DFA is constructed for the forward FST
			this.dfa = Dfa.createDfa(fst, ctx.dict, largestFrequentFid, false, useLazyDfa);
		} else {
			this.dfa = null;
		}

		// other variables
		inputTrie = new PatriciaTrie();
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
		root.clear();
		currentNode = root;
		//currentSpReachedWithoutOutput.clear();
	}

	// -- processing input sequences ----------------------------------------------------------------------------------

	@Override
	public void addInputSequence(IntList inputSequence, long inputSupport, boolean allowBuffering) {
		//System.out.println(++cnt);

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
		if(logRuntime) MetricLogger.getInstance().start(MetricLogger.Metric.MiningMinePreprocessingRuntime);
		//ensure intervals are present in trie
		inputTrie.getRoot().calculateIntervals(0);
		if(logRuntime) MetricLogger.getInstance().stop(MetricLogger.Metric.MiningMinePreprocessingRuntime);

		if(logMetrics) inputTrie.calcMetrics();

		if (DEBUG) {
			fst.exportGraphViz("fst.pdf");
			inputTrie.exportGraphViz("inputTrie.pdf", ctx.dict, 5);

		}

		//Init Mining
		//input trie size needs to be set after trie is built
		DesqDfsPatriciaTreeNode.nodeCounter.reset();
		DesqDfsPatriciaTreeNode.pruneCounter.reset();
		root = new DesqDfsPatriciaTreeNode(fst, inputTrie.size());
		currentNode = root;

		//First IncStep (only possible after complete input trie is built)
		// run the first incStep; start at all positions from which a final FST state can be reached
		//nodeReachedAsFinalWithoutOutput = new BitSet(inputTrie.size());

		//currentInputId = inputSequences.size()-1;
		//currentInputSequence = inputSequences.get(currentInputId);
		if((inputTrie.getRoot().getSupport() >= sigma) && !inputTrie.getRoot().isLeaf()) {
			if(logRuntime) MetricLogger.getInstance().start(MetricLogger.Metric.MiningMineFirstExpandRuntime);
			for(PatriciaTrie.TrieNode node: inputTrie.getRoot().getChildren()) {
				// and run the first inc step
				//currentSpReachedWithoutOutput.clear();
				incStep(0, fst.getInitialState(), 0, true, node, false);
			}
			if(logRuntime) MetricLogger.getInstance().stop(MetricLogger.Metric.MiningMineFirstExpandRuntime);

			//Proceed as in standard DFS
			if (inputTrie.getRoot().getSupport() >= sigma) {
				// the root has already been processed; now recursively grow the patterns
				root.pruneInfrequentChildren(sigma);
				expand(new IntArrayList(), root);
			}
		}
		if(logMetrics) {
			MetricLogger.getInstance().add(
					MetricLogger.Metric.NumberSearchTreeNodes,
					DesqDfsPatriciaTreeNode.nodeCounter.longValue());
			MetricLogger.getInstance().add(
					MetricLogger.Metric.NumberPrunedSearchTreeNodes,
					DesqDfsPatriciaTreeNode.pruneCounter.longValue());
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
	private void incStep(int pos, State state, final int level, final boolean expand, PatriciaTrie.TrieNode node, boolean trackWithoutOutput) {
		//boolean reachedFinalStateWithoutOutput = false; //only changed by FST transitions -> refers to same input node!

pos: 	do { // loop over positions; used for tail recursion optimization -> on trie not linear anymore -> recursion needs to split

			//If Fst reached final complete state -> exit (no items will be produced and ending in an final state is clear)
			if (state.isFinalComplete()){
				if(trackWithoutOutput && !currentNode.reachedFCStateAtInputId.get(node.getId()))
					currentNode.finalStateReached(node, true);
				return;// reachedFinalStateWithoutOutput;
			}

			//Handle end of input trie node (proceed to child nodes if possible)
			if(pos == node.getItems().size()) {
				//Check if end of final node
				if(trackWithoutOutput
						&& state.isFinal() && node.isFinal()
						&& !currentNode.reachedFCStateAtInputId.get(node.getId())){
					//Case: end of sequence and a final state -> track it (only exclusive support)
					//But keep tracking relevant descendants nodes
					currentNode.finalStateReached(node, false);
				}
				//Check if input trie node is leaf (no children) -> end of processing
				if(node.isLeaf()) {
					return;// reachedFinalStateWithoutOutput;
				}else{
					//No more items in node -> proceed to child trie node(s)
					final Iterator<PatriciaTrie.TrieNode> it = node.getChildren().iterator();
					while (it.hasNext()) {
						final PatriciaTrie.TrieNode child = it.next();
						if(logMetrics) MetricLogger.getInstance().addToSum(MetricLogger.Metric.NumberNodeMoves,1);
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
			final State.ItemStateIterator itemStateIt = state.consume(itemFid, itemStateIterators.get(level), null);

			// iterate over output item/state pairs and remember whether we hit the final or finalComplete state without producing output
			// (i.e., no transitions or only transitions with epsilon output)
itemState:	while (itemStateIt.hasNext()) { // loop over elements of itemStateIt; invariant that itemStateIt.hasNext()
				final ItemState itemState = itemStateIt.next();
				final int outputItemFid = itemState.itemFid;
				final State toState = itemState.state;
				if(logMetrics) MetricLogger.getInstance().addToSum(MetricLogger.Metric.NumberFstTransitions,1);

				if (outputItemFid == 0) { // EPS output
					// we did not get an output
					// if we saw this state at this position without output (for this input sequence and for the currently
					// expanded node) before, we do not need to process it again
					//CANNOT PRUNE HERE IF TRIE -> after this node many other sequences can follow -> node id must be included in index
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
		if(logMetrics) MetricLogger.getInstance().addToSum(MetricLogger.Metric.NumberExpands,1);
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
			currentNode = childNode;
			//boolean reachedFinalStateWithoutOutput = false;

			//currentSpReachedWithoutOutput.clear();

			if(projectedDatabaseIt.hasNext()) {

				do {
					final int currentInputId = projectedDatabaseIt.nextNonNegativeInt();
					final PatriciaTrie.TrieNode currentInputNode = inputTrie.getNodeById(currentInputId);
					final int stateId = projectedDatabaseIt.nextNonNegativeInt();
					final int pos = projectedDatabaseIt.nextNonNegativeInt();
					//reachedFinalStateWithoutOutput |=
							incStep(pos, fst.getState(stateId), 0, true,
									currentInputNode,!currentNode.reachedFCStateAtInputId.get(currentInputId));

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

	// -- accessors to internal data structures (use with care) -------------------------------------------------------

	public Fst getFst() {
		return fst;
	}

	public Dfa getDfa() {
		return dfa;
	}
}
