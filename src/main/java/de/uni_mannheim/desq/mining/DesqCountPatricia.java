package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.fst.*;
import de.uni_mannheim.desq.patex.PatExUtils;
import de.uni_mannheim.desq.util.DesqProperties;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.log4j.Logger;

import java.util.*;

public final class DesqCountPatricia extends DesqMiner {
	private static final Logger logger = Logger.getLogger(DesqCountPatricia.class);

	// -- parameters for mining ---------------------------------------------------------------------------------------

	/** Minimum support */
	final long sigma;

	/** The pattern expression used for mining */
	private final String patternExpression;

	/** Whether or not to use the f-list for pruning output sequences that are guaranteed to be infrequent. */
	private final boolean useFlist;

	/** If true, input sequences that do not match the pattern expression are pruned */
	private final boolean pruneIrrelevantInputs;

	/** If true, the two-pass algorithm is used */
	private final boolean useTwoPass;


	// -- helper variables --------------------------------------------------------------------------------------------

	/** Stores the final state transducer  */
	private final Fst fst;

	/** Stores the largest fid of an item with frequency at least sigma. Used to quickly determine
	 * whether an item is frequent (if fid <= largestFrequentFid, the item is frequent */
	private final int largestFrequentFid;

	/** Sequence id of the next input sequence */
	private int inputId;

	// -- helper variables --------------------------------------------------------------------------------------------
	/** The input sequence currenlty processed */
	IntList inputSequence;

	/** The support of the current input sequence */
	long inputSupport;

	/** Stores all mined sequences along with their frequency. Each long is composed of a 32-bit integer storing the
	 * actual count and a 32-bit integer storing the input id of the last input sequence that produced this output.  */
	//final Object2LongOpenHashMap<Sequence> outputSequences = new Object2LongOpenHashMap<>();

	/** Stores iterators over output item/next state pairs for reuse. Indexed by input position. */
	final ArrayList<State.ItemStateIterator> itemStateIterators = new ArrayList<>();

	/** Stores the part of the output sequence produced so far. */
	final Sequence prefix;

	// -- helper variables for pruning and twopass --------------------------------------------------------------------

	/** The DFA corresponding to the FST (pruning) or reverse FST (two-pass). */
	private final Dfa dfa;

	/** Sequence of EDFA states for current input (two-pass only) */
	private final ArrayList<DfaState> dfaStateSequence;

	/** Positions for which dfa reached an initial FST state (two-pass only) */
	private final IntList dfaInitalPos;

	/**Trie representing the data **/
	private PatriciaItemTrie trie;

	private HashSet<IntList> outputSequenceCache;


	// -- construction/clearing ---------------------------------------------------------------------------------------

	public DesqCountPatricia(DesqMinerContext ctx) {
		super(ctx);
		this.sigma = ctx.conf.getLong("desq.mining.min.support");
		this.useFlist = ctx.conf.getBoolean("desq.mining.use.flist");
		this.pruneIrrelevantInputs = ctx.conf.getBoolean("desq.mining.prune.irrelevant.inputs");
		this.useTwoPass = ctx.conf.getBoolean("desq.mining.use.two.pass");
		boolean useLazyDfa = ctx.conf.getBoolean("desq.mining.use.lazy.dfa");

		// initalize helper variable for FST simulation
		this.largestFrequentFid = ctx.dict.lastFidAbove(sigma);
		this.inputId = 0;
		prefix = new Sequence();
		//outputSequences.defaultReturnValue(-1L);

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
			dfaStateSequence = new ArrayList<>();
			dfaInitalPos = new IntArrayList();
		} else {
			dfaStateSequence = null;
			dfaInitalPos = null;
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

		// variables for patricia
		trie = new PatriciaItemTrie();
	}

	public static DesqProperties createConf(String patternExpression, long sigma) {
		DesqProperties conf = new DesqProperties();
		conf.setProperty("desq.mining.miner.class", DesqCountPatricia.class.getCanonicalName());
		conf.setProperty("desq.mining.min.support", sigma);
		conf.setProperty("desq.mining.pattern.expression", patternExpression);
		conf.setProperty("desq.mining.use.flist", true); /* use frequency by default -> sorted by Fid if itemset*/
		conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		conf.setProperty("desq.mining.use.lazy.dfa", false);
		conf.setProperty("desq.mining.use.two.pass", true);
		conf.setProperty("desq.mining.optimize.permutations",true);
		return conf;
	}


	// -- processing input sequences ---------------------------------------------------------------------------------

	@Override
	protected void addInputSequence(IntList sequence, long support, boolean allowBuffering) {
		assert prefix.isEmpty(); // will be maintained by stepOnePass()
		this.inputSequence = sequence;
		this.inputSupport = support;
		this.outputSequenceCache = new HashSet<>();

		// two-pass version of DesqCount
		if (useTwoPass) {
			// run the input sequence through the EDFA and compute the state sequences as well as the positions
			// at which, we start the FST simulation
			if (dfa.acceptsReverse(sequence, dfaStateSequence, dfaInitalPos)) {
				// we now know that the sequence is relevant; process it
				// look at all initial positions from which a final FST state can be reached
				for (final int pos : dfaInitalPos) {
					// for those positions, start with initial state
					step(pos, fst.getInitialState(), 0);
				}
				inputId++;
			}
			dfaStateSequence.clear();
			dfaInitalPos.clear();
			return;
		}

		// one-pass version of DesqCount
		if (!pruneIrrelevantInputs /*without pruning*/ || dfa.accepts(sequence) /*with pruning*/) {
			step(0, fst.getInitialState(), 0);
			inputId++;
		}
	}

	// -- mining ------------------------------------------------------------------------------------------------------

	@Override
	public void mine() {
		// patricia trie already built -> traverse it to generate relevant patterns

		if((trie.getRoot().getSupport() >= sigma) && !trie.getRoot().isLeaf()) {
			//start recursive trie traversal
			traverseTrie(new IntArrayList(),trie.getRoot());
		}
		//trie.exportGraphViz("trie.pdf");
	}

	private void traverseTrie(IntList prefix, PatriciaItemTrie.TrieNode node){
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
					traverseTrie(newPrefix,child);
				}
			}
			if((node.getSupport() - sumSupport) >= sigma){
				//the node itself is not a leaf, but defines a valid pattern already!
				ctx.patternWriter.write(prefix, node.getSupport());
			}
		}
	}



	/** Simulates the FST starting from the given position and state. Maintains the invariant that the current
	 * output is stored in {@link #prefix}. Recursive version
	 *
	 * @param pos position of next input item
	 * @param state current state of FST
	 * @param level recursion level (used for reusing iterators without conflict)
	 */
	private void step(int pos, State state, int level) {
		// stop recursing if we reached a final-complete state or consumed the entire input
		// and output if we stop at a final state
		if (state.isFinalComplete() || pos == inputSequence.size()) {
			if (!prefix.isEmpty() && state.isFinal() && !outputSequenceCache.contains(prefix)) {
				//add only once per input sequence
				outputSequenceCache.add(prefix);
				trie.addItems(prefix); //add relevant itemset to trie
			}
			return;
		}

		// get iterator over next output item/state pairs; reuse existing ones if possible
		// in two-pass, only iterates over states that we saw in the first pass (the other ones can safely be skipped)
		final int itemFid = inputSequence.getInt(pos);
		final BitSet validToStates = useTwoPass
				? dfaStateSequence.get( inputSequence.size()-(pos+1) ).getFstStates() // only states from first pass
				: null; // all states
		State.ItemStateIterator itemStateIt;
		if (level>=itemStateIterators.size()) {
			itemStateIt = state.consume(itemFid, new State.ItemStateIterator(ctx.dict.isForest()), validToStates);
			itemStateIterators.add(itemStateIt);
		} else {
			itemStateIt = state.consume(itemFid, itemStateIterators.get(level), validToStates);
		}


		// iterate over output item/state pairs
		while(itemStateIt.hasNext()) {
			final ItemState itemState = itemStateIt.next();
			final State toState = itemState.state;
			final int outputItemFid = itemState.itemFid;

			if(outputItemFid == 0) { // EPS output
				// we did not get an output
				// in the two pass algorithm, we don't need to consider empty-output paths that reach the initial state
				// because we'll start from those positions later on anyway.
				if (useTwoPass && prefix.isEmpty() && toState == fst.getInitialState()) {
					continue;
				}

				// otherwise, continue with the current prefix
				final int newLevel = level + (itemStateIt.hasNext() ? 1 : 0); // no need to create new iterator if we are done on this level
				step(pos + 1, toState, newLevel);
			} else {
				// we got an output; check whether it is relevant
				if (!useFlist || largestFrequentFid >= outputItemFid) {
					prefix.add(outputItemFid);
					final int newLevel = level + (itemStateIt.hasNext() ? 1 : 0); // no need to create new iterator if we are done on this level
					step(pos + 1, toState, newLevel);
					prefix.removeInt(prefix.size() - 1);
				}
			}
		}
	}
}
