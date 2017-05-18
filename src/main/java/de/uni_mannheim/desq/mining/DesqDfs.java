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

public final class DesqDfs extends MemoryDesqMiner {
	private static final Logger logger = Logger.getLogger(DesqDfs.class);
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
	int currentInputId;

	/** The items in the input sequence we are processing */
	WeightedSequence currentInputSequence;

	/** The state sequence of the accepting DFA run for the current intput sequence (two-pass only). */
	DfaState[] currentDfaStateSequence;

	/** The node in the search tree currently being processed */
	DesqDfsTreeNode currentNode;

	/** For each state/position pair, whether we have reached this state and position without further output
	 * already. Index of a pair is <code>pos*fst.numStates() + toState.getId()</code>.
	 */
	BitSet currentSpReachedWithoutOutput = new BitSet();

	// -- construction/clearing ---------------------------------------------------------------------------------------

	public DesqDfs(DesqMinerContext ctx) {
		super(ctx);
		sigma = ctx.conf.getLong("desq.mining.min.support");
		largestFrequentFid = ctx.dict.lastFidAbove(sigma);
		pruneIrrelevantInputs = ctx.conf.getBoolean("desq.mining.prune.irrelevant.inputs");
        useTwoPass = ctx.conf.getBoolean("desq.mining.use.two.pass");
		boolean useLazyDfa = ctx.conf.getBoolean("desq.mining.use.lazy.dfa");

		// create FST
		patternExpression = ctx.conf.getString("desq.mining.pattern.expression");
		this.fst = PatExUtils.toFst(ctx.dict, patternExpression);

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
		currentNode = root;
	}

	public static DesqProperties createConf(String patternExpression, long sigma) {
		DesqProperties conf = new DesqProperties();
		conf.setProperty("desq.mining.miner.class", DesqDfs.class.getCanonicalName());
		conf.setProperty("desq.mining.min.support", sigma);
		conf.setProperty("desq.mining.pattern.expression", patternExpression);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		conf.setProperty("desq.mining.use.lazy.dfa", false);
		conf.setProperty("desq.mining.use.two.pass", true);
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
		currentNode = root;
	}

	// -- processing input sequences ----------------------------------------------------------------------------------

	@Override
	public void addInputSequence(IntList inputSequence, long inputSupport, boolean allowBuffering) {
        // two-pass version of DesqDfs
        if (useTwoPass) {
            // run the input sequence through the DFA and compute the state sequences as well as the positions from
            // which a final FST state is reached
			if (dfa.acceptsReverse(inputSequence, dfaStateSequence, dfaInitialPos)) {
			    // we now know that the sequence is relevant; remember it
				super.addInputSequence(inputSequence, inputSupport, allowBuffering);
				while (itemStateIterators.size() < inputSequence.size())
					itemStateIterators.add(new State.ItemStateIterator(ctx.dict.isForest()));
				dfaStateSequences.add(dfaStateSequence.toArray(new DfaState[dfaStateSequence.size()]));

				// run the first incStep; start at all positions from which a final FST state can be reached
				assert currentNode == root;
				currentInputId = inputSequences.size()-1;
				currentInputSequence = inputSequences.get(currentInputId);
				currentDfaStateSequence = dfaStateSequences.get(currentInputId);
				currentSpReachedWithoutOutput.clear();
				for (int i = 0; i< dfaInitialPos.size(); i++) {
					// for those positions, start with the initial state
					incStep(dfaInitialPos.getInt(i), fst.getInitialState(), 0, true);
				}

				// clean up
				dfaInitialPos.clear();
			}
			dfaStateSequence.clear();
            return;
		}

		// one-pass version of DesqDfs
		if (!pruneIrrelevantInputs || dfa.accepts(inputSequence)) {
			// if we reach this place, we either don't want to prune irrelevant inputs or the input is relevant
            // -> remember it
		    super.addInputSequence(inputSequence, inputSupport, allowBuffering);
			while (itemStateIterators.size() < inputSequence.size())
				itemStateIterators.add(new State.ItemStateIterator(ctx.dict.isForest()));

		    // and run the first inc step
			assert currentNode == root;
			currentInputId = inputSequences.size()-1;
			currentInputSequence = inputSequences.get(currentInputId);
			currentSpReachedWithoutOutput.clear();
			incStep(0, fst.getInitialState(), 0, true);
		}
	}


    // -- mining ------------------------------------------------------------------------------------------------------

	@Override
	public void mine() {
		if (sumInputSupports >= sigma) {
			// the root has already been processed; now recursively grow the patterns
			root.pruneInfrequentChildren(sigma);
			expand(new IntArrayList(), root);
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
	private boolean incStep(int pos, State state, final int level, final boolean expand) {
		boolean reachedFinalStateWithoutOutput = false;

pos: 	do { // loop over positions; used for tail recursion optimization
			// check if we reached a final complete state or consumed entire input and reached a final state
			if (state.isFinalComplete() | pos == currentInputSequence.size())
				return state.isFinal();

			// get iterator over next output item/state pairs; reuse existing ones if possible
			// in two-pass, only iterates over states that we saw in the first pass (the other ones can safely be skipped)
			final int itemFid = currentInputSequence.getInt(pos);
			final BitSet validToStates = useTwoPass
					? currentDfaStateSequence[currentInputSequence.size() - (pos + 1)].getFstStates() // only states from first pass
					: null; // all states
			final State.ItemStateIterator itemStateIt = state.consume(itemFid, itemStateIterators.get(level), validToStates);

			// iterate over output item/state pairs and remember whether we hit the final or finalComplete state without producing output
			// (i.e., no transitions or only transitions with epsilon output)
itemState:	while (itemStateIt.hasNext()) { // loop over elements of itemStateIt; invariant that itemStateIt.hasNext()
				final ItemState itemState = itemStateIt.next();
				final int outputItemFid = itemState.itemFid;
				final State toState = itemState.state;

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
					int spIndex = pos * fst.numStates() + toState.getId();
					if (!currentSpReachedWithoutOutput.get(spIndex)) {
						// haven't seen it, so process
						currentSpReachedWithoutOutput.set(spIndex);
						if (itemStateIt.hasNext()) {
							// recurse
							reachedFinalStateWithoutOutput |= incStep(pos + 1, toState, level + 1, expand);
							continue itemState;
						} else {
							// tail recurse
							state = toState;
							pos++;
							continue pos;
						}
					}
				} else if (expand & largestFrequentFid >= outputItemFid) {
					// we have an output and its frequent, so update the corresponding projected database
					currentNode.expandWithItem(outputItemFid, currentInputId, currentInputSequence.weight,
							pos + 1, toState);
				}
				continue itemState;
			}

			break; // skipped only by call to "continue pos" above (tail recursion optimization)
		} while (true);
		return reachedFinalStateWithoutOutput;
	}

    /** Expands all children of the given search tree node. The node itself must have been processed/output/expanded
     * already.
     *
     * @param prefix (partial) output sequence corresponding to the given node (must remain unmodified upon return)
     * @param node the node whose children to expand
     */

	private void expand(IntList prefix, DesqDfsTreeNode node) {
		// add a placeholder to prefix for the output item of the child being expanded
		final int lastPrefixIndex = prefix.size();
		prefix.add(-1);

		// iterate over all children
		for (final DesqDfsTreeNode childNode : node.childrenByFid.values() )  {
			assert childNode.partialSupport + childNode.prefixSupport >= sigma;

			// while we expand the child node, we also compute its actual support to determine whether or not
			// to output it (and then output it if the support is large enough)
			// we start with the partial support; may be increased when processing the projected database
			long support = childNode.partialSupport;

			// set the current (parial) output sequence
			prefix.set(lastPrefixIndex, childNode.itemFid);

			// print debug information
			if (DEBUG) {
				logger.trace("Expanding " + prefix + ", partial support=" + support + ", prefix support="
						+ childNode.prefixSupport + ", #bytes=" + childNode.projectedDatabase.noBytes()
						+ ", possible states=" + childNode.possibleStates);
			}

			if (childNode.prefixSupport > 0) { // otherwise projected DB is empty and support = partial support
				// set up the expansion
				boolean expand = childNode.prefixSupport >= sigma; // otherwise expansions will be infrequent anyway
				projectedDatabaseIt.reset(childNode.projectedDatabase);
				currentInputId = -1;
				currentNode = childNode;

				do {
					// process next input sequence
					currentInputId += projectedDatabaseIt.nextNonNegativeInt();
					currentInputSequence = inputSequences.get(currentInputId);
					if (useTwoPass) {
						currentDfaStateSequence = dfaStateSequences.get(currentInputId);
					}
					currentSpReachedWithoutOutput.clear();

					// iterate over state@pos snapshots for this input sequence
					boolean reachedFinalStateWithoutOutput = false;
					do {
						int stateId = childNode.possibleState;
						if (stateId < 0) // if >= 0, then there is only one possible FST state and it's not recorded in the posting list
							stateId = projectedDatabaseIt.nextNonNegativeInt();
						final int pos = projectedDatabaseIt.nextNonNegativeInt(); // position of next input item
						reachedFinalStateWithoutOutput |= incStep(pos, fst.getState(stateId), 0, expand);
					} while (projectedDatabaseIt.hasNext());

					// if we reached a final state without output, increment the support of this child node
					if (reachedFinalStateWithoutOutput) {
						support += currentInputSequence.weight;
					}

					// now go to next posting (next input sequence)
				} while (projectedDatabaseIt.nextPosting());
			}

			// output the pattern for the current child node if it turns out to be frequent
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
