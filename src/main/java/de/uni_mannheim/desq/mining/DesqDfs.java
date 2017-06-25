package de.uni_mannheim.desq.mining;

import com.google.common.base.Stopwatch;
import de.uni_mannheim.desq.dictionary.MiningDictionary;
import de.uni_mannheim.desq.examples.DesqDfsRunDistributedMiningLocally;
import de.uni_mannheim.desq.fst.*;
import de.uni_mannheim.desq.util.CloneableIntHeapPriorityQueue;
import de.uni_mannheim.desq.patex.PatExUtils;
import de.uni_mannheim.desq.util.DesqProperties;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.log4j.Level;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.*;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;


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
	private static Fst fst;

	/** Flags to help construct and number the FST only once per executor JVM */
	private static String fstConstructedFor = "";
	private static String fstNumberedFor = "";

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
	private static Dfa dfa;

	/** Flag to construct the DFA only once per executor JVM */
	private static String dfaConstructedFor = "";

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

	// -- helper variables for distributing --------------------------------------------------------------------------
	/** Set of output items created by one input sequence */
	final IntSet pivotItems = new IntOpenHashSet();

	/** Storing the current input sequence */
	protected IntList inputSequence;

	/** pivot item of the currently mined partition */
	private int pivotItem = 0;

	/** Current prefix (used for finding the potential output items of one sequence) */
	final Sequence prefix = new Sequence();

	/** length of last processed prefix */
	int processedPathSize = 0;

	/** If true, DesqDfs Distributed sends Output NFAs instead of input sequences */
	final boolean sendNFAs;

	/** If true, DesqDfs Distributed merges shared suffixes in the tree representation */
	final boolean mergeSuffixes;

	/** If true, DesqDfs Distributed merges shared suffixes in the tree representation */
	final boolean trimInputSequences;

	/** If true, we send either NFAs or input sequence */
	final boolean useHybrid;

	/** If true, we construct one NFA to find pivots */
	final boolean useOneNFA;

	/** Stores the one NFA from which we extract pivot NFAs later on */
	OneNFA oneNFA = new OneNFA();

	/** Stores one Transition iterator per recursion level for reuse */
	final ArrayList<Iterator<Transition>> transitionIterators = new ArrayList<>();

	/** Current transition */
	Transition tr;

	/** Current to-state */
	State toState;

	/** A set to collect the output items of the current transition */
	IntArrayList outputItems = new IntArrayList();

	/** One itCache per level, in order to reuse the Iterator objects */
	private final ArrayList<Transition.ItemStateIteratorCache> itCaches = new ArrayList<>();

	/** Stores the current path through the FST */
	private ObjectList<OutputLabel> path = new ObjectArrayList<>();

	/** Stores input NFAs for current partition */
	protected final ObjectArrayList<WeightedSequence> inputNFAs = new ObjectArrayList<>();

	/** The items of the NFA we are processing */
	WeightedSequence currentNFA;

	/** Stores number of pivot-relevant items in the path (we use this for efficiently finding pivot elements for input sequences) */
	private IntArrayList outputItemLimits = new IntArrayList();

	/** For each pivot item (and input sequence), we store one NFA producing the output sequences for that pivot item from the current input sequence */
	private NFAList nfas = new NFAList();

	/** An nfaDecoder to decode nfas from a representation by path to one by state. Reuses internal data structures for all input nfas */
	private NFADecoder nfaDecoder = null;

	/** Stores one pivot element heap per level for reuse */
	final ArrayList<CloneableIntHeapPriorityQueue> pivotItemHeaps = new ArrayList<>();

	/** Stats about pivot element search */
	public long counterTotalRecursions = 0;
	private boolean verbose;
	private boolean drawGraphs = DesqDfsRunDistributedMiningLocally.drawGraphs;



	// -- construction/clearing ---------------------------------------------------------------------------------------

	public DesqDfs(DesqMinerContext ctx) {
		this(ctx, false);
	}

	public DesqDfs(DesqMinerContext ctx, boolean skipDfaBuild) {
		super(ctx);
		sigma = ctx.conf.getLong("desq.mining.min.support");
		Stopwatch swFid = new Stopwatch().start();
		if(ctx.dict instanceof MiningDictionary)
			largestFrequentFid = ((MiningDictionary)ctx.dict).lastFrequentFid();
		else
			largestFrequentFid = ctx.dict.lastFidAbove(sigma);

		pruneIrrelevantInputs = ctx.conf.getBoolean("desq.mining.prune.irrelevant.inputs");
        useTwoPass = ctx.conf.getBoolean("desq.mining.use.two.pass");
		boolean useLazyDfa = ctx.conf.getBoolean("desq.mining.use.lazy.dfa");

		sendNFAs = ctx.conf.getBoolean("desq.mining.send.nfas", false);
		mergeSuffixes = ctx.conf.getBoolean("desq.mining.merge.suffixes", false);
		trimInputSequences = ctx.conf.getBoolean("desq.mining.trim.input.sequences", false);
		useHybrid = ctx.conf.getBoolean("desq.mining.use.hybrid", false);
		useOneNFA = ctx.conf.getBoolean("desq.mining.use.one.nfa", false);


		// create FST once per JVM
		Stopwatch swFst = new Stopwatch().start();
		patternExpression = ctx.conf.getString("desq.mining.pattern.expression");
		synchronized (fstConstructedFor) {
			if(!fstConstructedFor.equals(patternExpression)) {
				this.fst = PatExUtils.toFst(ctx.dict, patternExpression);
				fstConstructedFor = patternExpression;
			}
		}


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

		// create DFA or reverse DFA (if needed) (once per JVM)
		Stopwatch swDfa = new Stopwatch().start();
        synchronized (dfaConstructedFor) {
        	if(!dfaConstructedFor.equals(patternExpression)) {
				if (useTwoPass && (!skipDfaBuild || !sendNFAs)) {
					// construct the DFA for the FST (for the first pass)
					// the DFA is constructed for the reverse FST
					this.dfa = Dfa.createReverseDfa(fst, ctx.dict, largestFrequentFid, true, useLazyDfa);
				} else if (pruneIrrelevantInputs && !skipDfaBuild) {
					// construct the DFA to prune irrelevant inputs
					// the DFA is constructed for the forward FST
					this.dfa = Dfa.createDfa(fst, ctx.dict, largestFrequentFid, false, useLazyDfa);
				} else {
					this.dfa = null;
				}
				dfaConstructedFor = patternExpression;
			}
		}

		if(drawGraphs) fst.exportGraphViz(DesqDfsRunDistributedMiningLocally.useCase + "-fst.pdf");

		// other auxiliary variables
		root = new DesqDfsTreeNode(fst.numStates(), !sendNFAs || useHybrid, sendNFAs); // if we use NFAs, we need only one BitSet per node
		currentNode = root;
		verbose = DesqDfsRunDistributedMiningLocally.verbose;


		synchronized (fstNumberedFor) {
			if(!fstNumberedFor.equals(patternExpression)) {
				fst.numberTransitions();
				fstNumberedFor = patternExpression;
			}
		}


        // we need an itCache in deserialization of the NFAs, to produce the output items of transitions
        if(itCaches.isEmpty()) {
            Transition.ItemStateIteratorCache itCache = new Transition.ItemStateIteratorCache(ctx.dict.isForest());
            itCaches.add(itCache);
        }
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
		clear(false);
	}

	public void clear(boolean trimInputSequences) {
		inputSequences.clear();
		inputNFAs.clear();
		if(trimInputSequences) {
			inputSequences.trimToSize();
			inputNFAs.trim();
		}
		if (useTwoPass && dfaStateSequences != null) {
			dfaStateSequences.clear();
            dfaStateSequences.trimToSize();
		}
		root.clear();
		currentNode = root;
		sumInputSupports = 0;
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


		sumInputSupports += inputSupport;
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
					// not needed anymore (covered by indexing below)
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
				} else if (expand && largestFrequentFid >= outputItemFid && (pivotItem == 0 || outputItemFid <= pivotItem)) {
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
	 * @param node the node whose children to expandOnNFA
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
						+ childNode.prefixSupport + ", #bytes=" + childNode.projectedDatabase.noBytes());
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
						final int stateId = projectedDatabaseIt.nextNonNegativeInt();
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
				// if we are mining a specific pivot partition (meaning, pivotItem>0), then we output only sequences with that pivot
				if (ctx.patternWriter != null && (pivotItem==0 || pivot(prefix) == pivotItem)) {
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




	// ---------------- Distributed ---------------------------------------------------------


	/**
	 * Returns the set of pivot items for the given input sequence
	 *
	 * @param inputSequence
	 * @return pivotItems set of frequent output items of input sequence inputSequence
	 */
	public IntSet generatePivotItems(IntList inputSequence, RelevantPositions relevantPositions) {
		pivotItems.clear();
		this.inputSequence = inputSequence;


		if(useOneNFA) {
			constructNFA(inputSequence);
			// export
//			oneNFA.exportGraphViz("oneNFA-" + inputSequence.toString() + ".pdf");

			if(oneNFA.numStates() > 0) {
				oneNFA.determinizeBackwards();
//				oneNFA.exportBdWithGraphViz("oneNFA-" + inputSequence.toString() + "-BRZ.pdf");
				IntList pivots = oneNFA.getPivotsBz(inputSequence);
				pivotItems.addAll(pivots);
			}
		} else {
  			// check whether sequence produces output at all. if yes, produce output items
			if (useTwoPass) {
				// Run the DFA to find to determine whether the input has outputs and in which states
				// Then walk backwards and collect the output items
				if (dfa.acceptsReverse(inputSequence, dfaStateSequence, dfaInitialPos)) {
					//dfaStateSequences.add(dfaStateSequence.toArray(new DfaState[dfaStateSequence.size()]));

					// run the first incStep; start at all positions from which a final FST state can be reached
					for (int i = 0; i < dfaInitialPos.size(); i++) {
						// for those positions, start with the initial state
						piStep(-1, dfaInitialPos.getInt(i), fst.getInitialState(), 0, relevantPositions, -1);
					}

					// clean up
					dfaInitialPos.clear();
				}
				dfaStateSequence.clear();

			} else { // one pass

				if (!pruneIrrelevantInputs || dfa.accepts(inputSequence)) {
					piStep(-1, 0, fst.getInitialState(), 0, relevantPositions, -1);
				}
			}
		}
		return pivotItems;
	}


	public OneNFA constructNFA(IntList inputSequence) {

		// get the pivot elements with the corresponding paths through the FST
		this.inputSequence = inputSequence;
		oneNFA.clear();

		// we always use two-pass
		dfaStateSequence.clear();
		if (dfa.acceptsReverse(inputSequence, dfaStateSequence, dfaInitialPos)) {

			// run the first incStep; start at all positions from which a final FST state can be reached
			for (int i = 0; i < dfaInitialPos.size(); i++) {
				findPathsStep(fst.getInitialState(), dfaInitialPos.getInt(i), 0, null, -1);
			}

			// clean up
			dfaInitialPos.clear();
		}

		return oneNFA;
	}


	/** Generates all Output NFAs of the given input sequence and returns them in a Map: pivItem -> NFA
	 *
	 * @param inputSequence
	 * @return pivotItems set of frequent output items of input sequence inputSequence
	 */
	public NFAList generateOutputNFAs(IntList inputSequence) {

		// get the pivot elements with the corresponding paths through the FST
		this.inputSequence = inputSequence;
		path.clear();
		nfas.clear();

		if(useTwoPass) {
			dfaStateSequence.clear();
			if (dfa.acceptsReverse(inputSequence, dfaStateSequence, dfaInitialPos)) {

				// run the first incStep; start at all positions from which a final FST state can be reached
				for (int i = 0; i < dfaInitialPos.size(); i++) {
					piStep(-1, dfaInitialPos.getInt(i), fst.getInitialState(), 0, null, -1);
				}

				// clean up
				dfaInitialPos.clear();
			}
		} else {
			piStep(-1, 0, fst.getInitialState(), 0, null, -1);
		}

		return nfas;
	}


	/** Runs one step (along compressed transition) in the FST in order to produce the set of frequent output items
	 *  and directly creates the partitions with the transition representation
	 *
	 * @param largestFidSeenIncoming largest seen fid so far. this will become the first pivot item later on
	 * @param pos   current position
	 * @param state current state
	 * @param level current level
	 */
	private void piStep(final int largestFidSeenIncoming, int pos, State state, int level,
						   RelevantPositions relevantPositions, int currentLastIrrelevant) {
		counterTotalRecursions++;

		if(state.getId() == 0 && path.size() == 0) {
			currentLastIrrelevant = pos;
		}

		// if we reached a final state, we add the current set of pivot items at this state to the global set of pivot items for this sequences
		if(state.isFinal() && path.size() > processedPathSize) {
			processedPathSize = path.size();

			if(sendNFAs) {
				// make a deep copy of the path, as we will kick items out of the sets
				OutputLabel[] pathCopy = new OutputLabel[path.size()];
				for(int i=0; i<pathCopy.length; i++) {
					pathCopy[i] = path.get(i).clone();
				}

				// add this path to the different pivot NFAs
				//    we start with the largest pivot, and drop elements from the output sets one by one
				int pivotItem = largestFidSeenIncoming;
				int numPivotsEmitted = 0;
				while(pivotItem != -1) {
					numPivotsEmitted++;
					OutputNFA nfa = nfas.getNFAForPivot(pivotItem, fst, useHybrid);

					// if the path doesn't contain the pivot, something went wrong here.
					assert pivot(pathCopy) >= pivotItem: "Pivot in path is " + pivot(pathCopy) + ", but pivot item is " + pivotItem;

					// add the path to the pivot (incl. kicking out items>pivot) and retrieve next pivot item
					pivotItem = nfa.addPathAndReturnNextPivot(pathCopy, currentLastIrrelevant, pos);
				}
			} else {
			    // If we are not sending NFAs, we are only interested in the pivot items for this sequence.
				// We extract these pivot items from the output items in the path.

				// we work on subset of the output item sets in the path. In outputItemLimits, we store the index of
				// the rightmost item of our subset.
			   	for(int i=0; i<path.size(); i++) {
			   		if(i >= outputItemLimits.size()) {
						outputItemLimits.add(path.get(i).outputItems.size() - 1);
					} else {
			   			outputItemLimits.set(i, path.get(i).outputItems.size() - 1);
					}
				}

			    int nextPivot = largestFidSeenIncoming;
			    int currentPivot;
			    int currentLimit;

			    pivotloop:
			    while(true) {
			    	// for each pivot item we find, we drop items >= pivot from the output item sets and look for the
					// next-largest output item, which then will be the next pivot item
					// we stop when one of the sets in the path is empty

					currentPivot = nextPivot;
					nextPivot = -1;
					pivotItems.add(currentPivot);
					if(trimInputSequences)
						relevantPositions.addLimitsForPivot(currentPivot, currentLastIrrelevant, pos);

					// go through each output set in the path
					for (int i = 0; i < path.size(); i++) {
						currentLimit = outputItemLimits.getInt(i);

						// drop items >= the current pivot
						while (path.get(i).outputItems.getInt(currentLimit) >= currentPivot) {
							currentLimit--;

							// if this set is empty, we have found all pivot items and can stop here
							if(currentLimit < 0) {
								break pivotloop;
							}
						}

						outputItemLimits.set(i, currentLimit);
						nextPivot = Math.max(nextPivot, path.get(i).outputItems.getInt(currentLimit));
					}
				}
			}
		}

		// check if we already read the entire input
		if (state.isFinalComplete() || pos == inputSequence.size()) {
			return;
		}

		// get the next input item
		final int itemFid = inputSequence.getInt(pos);

		// in two pass, we only go to states we saw in the first pass
		final BitSet validToStates = useTwoPass
				? dfaStateSequence.get(inputSequence.size()-(pos+1)).getFstStates() // only states from first pass
				: null; // all states

		// get an iterator over all relevant transitions from here (relevant=starts from this state + matches the input item)
		Iterator<Transition> transitionIt;
		if(level >= transitionIterators.size()) {
			transitionIt = state.consumeCompressed(itemFid, null, validToStates);
			transitionIterators.add(transitionIt);
		} else {
			transitionIt = state.consumeCompressed(itemFid, transitionIterators.get(level), validToStates);
		}

		Transition.ItemStateIteratorCache itCache;
		if(level >= itCaches.size()) {
			itCache = new Transition.ItemStateIteratorCache(ctx.dict.isForest());
			itCaches.add(itCache);
		} else {
		    itCache = itCaches.get(level);
		}

		OutputLabel ol;
		int outputItem;
		int largestFidSeen;
		boolean needToSort;
		int lastOutputItem;

		// follow each relevant transition
		while(transitionIt.hasNext()) {
			tr = transitionIt.next();
			toState = tr.getToState();

			largestFidSeen = largestFidSeenIncoming;

			// We handle the different output label types differently
			if(!tr.hasOutput()) { // this transition doesn't produce output
                // an eps transition does not introduce potential pivot elements, so we simply continue recursion
                piStep(largestFidSeen, pos + 1, toState, level + 1, relevantPositions, currentLastIrrelevant);

			} else { // this transition produces output
				// collect all output elements into the outputItems set
				Iterator<ItemState> outIt = tr.consume(itemFid, itCache);

				// we assume that we get the items sorted, in decreasing order
				// if that is not the case, we need to sort
				needToSort = false;
				lastOutputItem = Integer.MAX_VALUE;

				while(outIt.hasNext()) {
					outputItem = outIt.next().itemFid;

					assert outputItem != lastOutputItem; // We assume we get each item only once
					if(outputItem > lastOutputItem)
						needToSort = true;
					lastOutputItem = outputItem;

					// we are only interested in frequent output items
				    if(largestFrequentFid >= outputItem) {
						outputItems.add(outputItem);

						// we look out for the maximum item
						if(outputItem > largestFidSeen) {
							largestFidSeen = outputItem;
						}
					}
				}

				// if we have frequent output items, build the output label for this transition and follow it
                if(outputItems.size() > 0) {

					// if we need to sort, we sort. Otherwise we just reverse the elements to have them in increasing order
					if(needToSort)
						IntArrays.quickSort(outputItems.elements(), 0, outputItems.size());
					else
						IntArrays.reverse(outputItems.elements(), 0, outputItems.size());

                    ol = new OutputLabel(tr, itemFid, outputItems);

                    // now that we use the previous set in the output label, we start a new one for the next transition
                    outputItems = new IntArrayList();

                    // add this output label to the path and follow the path further. afterwards, remove the
					//   output label from the path and continue with the next transition
					path.add(ol);
					piStep(largestFidSeen, pos + 1, toState, level + 1, relevantPositions, currentLastIrrelevant);
					path.remove(path.size() - 1);
					processedPathSize = path.size();
				}
			}
		}
	}



	/** Run one FST step to find all paths through the FST for the current input sequence.
	 * Returns true if it found an accepting path starting at given (state,pos)
	 */
	private boolean findPathsStep(State state, int pos, int level,
								  RelevantPositions relevantPositions, int currentLastIrrelevant) {
	    				// current nfaState is implicit through (fstState.id, pos)
		counterTotalRecursions++;
		int qCurrent = state.getId();

		if(qCurrent == 0 && path.size() == 0) {
			currentLastIrrelevant = pos;
		}

		// mark this state as final in the NFA. will be done only once, as we visit this state only once
		if(state.isFinal()) {
			oneNFA.markFinal(qCurrent, pos); // creates the state if it doesn't exist yet
		}

		// check if we already read the entire input
		if (state.isFinalComplete() || pos == inputSequence.size()) {
			// mark this state as final
			return true;
		}

		// get the next input item
		final int itemFid = inputSequence.getInt(pos);

		// in two pass, we only go to states we saw in the first pass
		final BitSet validToStates = useTwoPass
				? dfaStateSequence.get(inputSequence.size()-(pos+1)).getFstStates() // only states from first
				: null; // all states

		// get an iterator over all relevant transitions from here (relevant=starts from this state + matches the input item)
		Iterator<Transition> transitionIt;
		if(level >= transitionIterators.size()) {
			transitionIt = state.consumeCompressed(itemFid, null, validToStates);
			transitionIterators.add(transitionIt);
		} else {
			transitionIt = state.consumeCompressed(itemFid, transitionIterators.get(level), validToStates);
		}

		Transition.ItemStateIteratorCache itCache;
		if(level >= itCaches.size()) {
			itCache = new Transition.ItemStateIteratorCache(ctx.dict.isForest());
			itCaches.add(itCache);
		} else {
			itCache = itCaches.get(level);
		}

		OutputLabel ol;
		int outputItem;
		boolean needToSort;
		int lastOutputItem;
		int qTo;
		boolean foundAcceptingPath = false;

		// follow each relevant transition
		while(transitionIt.hasNext()) {
			tr = transitionIt.next();
			toState = tr.getToState();
			ol = null;

			// We handle the different output label types differently
			if(!tr.hasOutput()) { // this transition doesn't produce output
                // this is an eps-edge. we use null as a marker for eps-edges in the NFA
				ol = null;
			} else { // this transition produces output
				// collect all output elements into the outputItems set
				Iterator<ItemState> outIt = tr.consume(itemFid, itCache);

				// we assume that we get the items sorted, in decreasing order
				// if that is not the case, we need to sort
				needToSort = false;
				lastOutputItem = Integer.MAX_VALUE;

				while(outIt.hasNext()) {
					outputItem = outIt.next().itemFid;

					assert outputItem != lastOutputItem; // We assume we get each item only once
					if(outputItem > lastOutputItem)
						needToSort = true;
					lastOutputItem = outputItem;

					// we are only interested in frequent output items
					if(largestFrequentFid >= outputItem) {
						outputItems.add(outputItem);
					}
				}

				// if we have frequent output items, build the output label for this transition and follow it
				if(outputItems.size() > 0) {

					// if we need to sort, we sort. Otherwise we just reverse the elements to have them in increasing order
					if(needToSort)
						IntArrays.quickSort(outputItems.elements(), 0, outputItems.size());
					else
						IntArrays.reverse(outputItems.elements(), 0, outputItems.size());

					ol = oneNFA.getOrCreateLabel(tr, itemFid, outputItems);

					// if we are using this output items object in the new ol (and not reusing an old object), we create a new one
                    // otherwise, we can reuse the object
					if(ol.outputItems == outputItems) {
						outputItems = new IntArrayList();
					} else {
						outputItems.clear();
					}
				}
			}

			qTo = toState.getId();
			// so we have a new connection from (qCurrent, pos) to (qTo, pos+1) with label ol. what to do now?

			// easiest option: we know the to-state (qTo, pos+1) is a dead end. then we do nothing
			if(oneNFA.isDeadEnd(qTo, pos+1)) {
				// do nothing
			}

			// next: qCurrent already has such an forwardEdges edge. then we have to do basically nothing
			else if(oneNFA.checkForEdge(qCurrent, pos, qTo, ol)) {
				// do nothing
			}

			// next: the to-state (qTo, pos+1) already exists. then we don't have to recurse, we just add a link
			else if(oneNFA.checkForState(qTo, pos+1) != -1) {
                foundAcceptingPath = true;
                oneNFA.addEdge(qCurrent, pos, qTo, ol); // add edge to NFA and maintain maxPivot entries
			}

			// otherwise, we need to recurse down this path and update our information when we come back up
			else {
				boolean isAccepting = findPathsStep(toState, pos + 1, level + 1, relevantPositions, currentLastIrrelevant);
				foundAcceptingPath |= isAccepting;

				// if we found and accpeting path with this transition, add an edge
                if(isAccepting) {
					oneNFA.addEdge(qCurrent, pos, qTo, ol); // this creates the current state if it doesn't exist yet
				}
			}
		}

		// if we didnt' find any accepting path starting from here, we mark this (q,pos) pair as dead end
		if(!foundAcceptingPath) {
			oneNFA.markDeadEnd(qCurrent, pos);
		}
		return foundAcceptingPath;
	}


	/**
	 * Mines for frequent sequences in the given HashMap of aggregated NFAs
	 * @param pivotItem
	 * @param serializedInputNFAs
	 */
	public void mineNFAs(int pivotItem, Object2LongOpenHashMap<Sequence> serializedInputNFAs) {
		this.pivotItem = pivotItem;

		// if we don't have an nfaDecoder from a previous partition, create one
		if(nfaDecoder == null)
			nfaDecoder = new NFADecoder(fst, itCaches.get(0));

		sumInputSupports = 0;
		// transform aggregated nfas to a by-state representation
		for (Object2LongMap.Entry<Sequence> nfaWithWeight : serializedInputNFAs.object2LongEntrySet()) {
			Sequence nfa = nfaWithWeight.getKey();
			long weight = nfaWithWeight.getLongValue();
			if(useHybrid && nfa.getInt(0) == 0) {
                // this is an input sequence
                nfa.removeInt(0); // remove the first int (the first int signals that this is an input sequence rather than an NFA)
                addInputSequence(nfa, weight, true);
			}
			else {
				inputNFAs.add(nfaDecoder.convertPathToStateSerialization(nfa, pivotItem).withSupport(weight));
			}
			sumInputSupports += weight;
		}

		mineOnNFA();

		this.pivotItem = 0;
		this.clear(false);
	}

	/**
	 * Mines for frequent sequences form the given Iterable of NFAs
	 * Works without aggregation of NFAs
	 * @param pivotItem
	 * @param serializedInputNFAs
	 */
	public void mineNFAs(int pivotItem, Iterable<Sequence> serializedInputNFAs) {
		this.pivotItem = pivotItem;

		// if we don't have an nfaDecoder from a previous partition, create one
		if(nfaDecoder == null)
			nfaDecoder = new NFADecoder(fst, itCaches.get(0));

		sumInputSupports = 0;
		// transform aggregated nfas to a by-state representation
        for(Sequence nfa : serializedInputNFAs) {
        	if(useHybrid && nfa.getInt(0) == 0) {
				// this is an input sequence
				nfa.removeInt(0);
				addInputSequence(nfa, 1, true);
			} else {
				inputNFAs.add(nfaDecoder.convertPathToStateSerialization(nfa, pivotItem).withSupport(1));
			}
			sumInputSupports += 1;
		}

		mineOnNFA();

		this.pivotItem = 0;
		this.clear(false);
	}



	/**
	 * Mine with respect to a specific pivot item. e.g, filter out all non-pivot sequences
	 *   (e.g. filter out all sequences where pivotItem is not the max item)
	 * @param pivotItem
	 */
	public void minePivot(int pivotItem) {
		this.pivotItem = pivotItem;

		mine();

		// reset the pivot item, just to be sure. pivotItem=0 means no filter
		this.pivotItem = 0;
		this.clear(false);
	}

	public void mineOnNFA() {
		if (sumInputSupports >= sigma) {
			currentNode = root;
			// and process all given NFAs
			for (int inputId = 0; inputId < inputNFAs.size(); inputId++) {
				currentInputId = inputId;
				currentNFA = inputNFAs.get(inputId);
				incStepOnNFA(0, true);
			}

			// we processed the input sequences already when we added them, see addInputSequence()

            // we have processed both NFAs and sequences, now recursively grow the patterns
			root.pruneInfrequentChildren(sigma);
			expandOnNFA(new IntArrayList(), root);
		}
	}

	/** Expands all children of the given search tree node. The node itself must have been processed/output/expanded
	 * already.
	 *
	 * @param prefix (partial) output sequence corresponding to the given node (must remain unmodified upon return)
	 * @param node the node whose children to expandOnNFA
	 */
	private void expandOnNFA(IntList prefix, DesqDfsTreeNode node) {
		// add a placeholder to prefix for the output item of the child being expanded
		final int lastPrefixIndex = prefix.size();
		prefix.add(-1);

		// iterate over all children
		for (final DesqDfsTreeNode childNode : node.childrenByFid.values() )  {
			// while we expandOnNFA the child node, we also compute its actual support to determine whether or not
			// to output it (and then output it if the support is large enough)
			long support = childNode.partialSupport;
			// check whether pivot expansion worked
			// the idea is that we never expandOnNFA a child>pivotItem at this point
			if(pivotItem != 0) {
				assert childNode.itemFid <= pivotItem;
			}

			// set the current (partial) output sequence
			prefix.set(lastPrefixIndex, childNode.itemFid);

			if(childNode.prefixSupport > 0) {
				// set up the expansion
				boolean expand = childNode.prefixSupport >= sigma;

				/** Iterate over all NFA snapshots for this node */
				projectedDatabaseIt.reset(childNode.projectedNFADatabase);
				currentInputId = -1;
				currentNode = childNode;

				if(projectedDatabaseIt.hasNext()) {
					do {
						// process next NFA
						currentInputId += projectedDatabaseIt.nextNonNegativeInt();
						currentNFA = inputNFAs.get(currentInputId);

						// iterate over state snapshots for this NFA
						boolean reachedFinalStateWithoutOutput = false;
						do {
							final int pos = projectedDatabaseIt.nextNonNegativeInt(); // position of next input item
							reachedFinalStateWithoutOutput |= incStepOnNFA(pos, expand);
						} while (projectedDatabaseIt.hasNext());

						// if we reached a final state without output, increment the support of this child node
						if (reachedFinalStateWithoutOutput) {
							support += currentNFA.weight;
						}

						// now go to next posting (next NFA)
					} while (projectedDatabaseIt.nextPosting());
				}

				/** If we are using hybrid mode, also iterate over input sequence snapshots for this node */
				if(useHybrid) {
					projectedDatabaseIt.reset(childNode.projectedDatabase);
					currentInputId = -1;
					currentNode = childNode;

					if(projectedDatabaseIt.hasNext()) {
						do {
							// process next input sequence
							currentInputId += projectedDatabaseIt.nextNonNegativeInt();
							currentInputSequence = inputSequences.get(currentInputId);
							if (useTwoPass) {
								currentDfaStateSequence = dfaStateSequences.get(currentInputId);
							}
							currentSpReachedWithoutOutput.clear();

							// iterate over state snapshots for this input sequence
							boolean reachedFinalStateWithoutOutput = false;
							do {
								final int stateId = projectedDatabaseIt.nextNonNegativeInt();
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
				}
			}

			// output the patterns for the current child node if it turns out to be frequent
			if (support >= sigma) {
				// if we are mining a specific pivot partition (meaning, pivotItem>0), then we output only sequences with that pivot
				if (ctx.patternWriter != null && (pivotItem==0 || pivot(prefix) == pivotItem)) {
					ctx.patternWriter.write(prefix, support);
				}
			}
			// expandOnNFA the child node
			childNode.pruneInfrequentChildren(sigma);
			childNode.projectedDatabase = null; // not needed anymore
			childNode.projectedNFADatabase = null; // not needed anymore
			expandOnNFA(prefix, childNode);
			childNode.invalidate(); // not needed anymore
		}
		// we are done processing the node, so remove its item from the prefix
		prefix.removeInt(lastPrefixIndex);
	}


	/** Updates the projected databases of the children of the current node (args.node) corresponding to each possible
	 * next output item for the current input sequence (also stored in args). Used only in the one-pass algorithm.
	 *
	 * @param pos next item to read
	 * @param expand if an item is produced, whether to add it to the corresponding child node
	 *
	 * @return true if a final FST state can be reached without producing further output
	 */
	private boolean incStepOnNFA(int pos, final boolean expand) {
	    // "pos" points to a state in the shuffled OutputNFA. This state has 0-n outgoing transitions.
		// Each transition carries a toState and a list of output items
		int nextInt = currentNFA.getInt(pos);
		int currentToStatePos = -1;
		while(nextInt != OutputNFA.END_FINAL && nextInt != OutputNFA.END) {

			if(nextInt < 0) { // this is a (new) toState
				currentToStatePos = -nextInt;
			} else { // otherwise, it's output items
				// we checked that these output items are relevant for the partition when we converted the NFA to a by-state representation
				if(expand) {
					assert currentToStatePos != -1;
					currentNode.expandWithTransition(nextInt, currentInputId, currentNFA.weight, currentToStatePos);
				}
			}
			pos++;
			nextInt = currentNFA.getInt(pos);
		}

		return nextInt == OutputNFA.END_FINAL; // is true if this state is final
	}

	/**
	 * Returns the pivot item of the given sequence
	 * @param sequence
	 * @return pivot item
	 */
	private int pivot(IntList sequence) {
		int pivotItem = 0;
		for(int item : sequence) {
			pivotItem = Math.max(item, pivotItem);
		}
		return pivotItem;
	}

	/**
	 * Returns the pivot item of the given path
	 * @param path
	 * @return pivot items
	 */
	private int pivot(OutputLabel[] path) {
		int pivot = -1;
		for(OutputLabel ol : path) {
			pivot = Math.max(pivot, ol.outputItems.getInt(ol.outputItems.size()-1));
		}
		return pivot;
	}
}