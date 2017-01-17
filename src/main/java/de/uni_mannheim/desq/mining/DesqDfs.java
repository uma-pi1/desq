package de.uni_mannheim.desq.mining;

import com.google.common.base.Stopwatch;
import de.uni_mannheim.desq.dictionary.MiningDictionary;
import de.uni_mannheim.desq.examples.DesqDfsRunDistributedMiningLocally;
import de.uni_mannheim.desq.fst.*;
import de.uni_mannheim.desq.util.CloneableIntHeapPriorityQueue;
import de.uni_mannheim.desq.fst.graphviz.FstVisualizer;
import de.uni_mannheim.desq.patex.PatExUtils;
import de.uni_mannheim.desq.util.DesqProperties;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.log4j.Level;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.*;
import org.apache.commons.io.FilenameUtils;
import scala.Tuple2;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.io.IOException;


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

	// -- helper variables for distributing --------------------------------------------------------------------------
	/** Set of output items created by one input sequence */
	final IntSet pivotItems = new IntAVLTreeSet();

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

	/** Maximum number of output items to shuffle. More output items are encoded using transition numbers */
	final int maxNumShuffleOutputItems;

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

	/** For each pivot item (and input sequence), we mergeAndSerialize a NFA producing the output sequences for that pivot item from the current input sequence */
	private Int2ObjectOpenHashMap<OutputNFA> nfas = new Int2ObjectOpenHashMap<>();
	private Int2ObjectOpenHashMap<Sequence> serializedNFAs = new Int2ObjectOpenHashMap<>();

	/** The output partitions */
	Int2ObjectOpenHashMap<ObjectList<Sequence>> partitions;

	/** An nfaDecoder to decode nfas from a representation by path to one by state. Reuses internal data structures for all input nfas */
	private NFADecoder nfaDecoder = null;

	/** Stores one pivot element heap per level for reuse */
	final ArrayList<CloneableIntHeapPriorityQueue> pivotItemHeaps = new ArrayList<>();

	/** Stats about pivot element search */
	public long counterTotalRecursions = 0;
	private boolean verbose;
	private boolean drawGraphs = DesqDfsRunDistributedMiningLocally.drawGraphs;
	private int numSerializedStates = 0;

	/** Stop watches and counters */
	public Stopwatch swFirstPass = new Stopwatch();
	public Stopwatch swSecondPass = new Stopwatch();
	public Stopwatch swPrep = new Stopwatch();
	public Stopwatch swSetup = new Stopwatch();
	public Stopwatch swTrim = new Stopwatch();
	public Stopwatch swMerge = new Stopwatch();
	public Stopwatch swSerialize = new Stopwatch();
	public Stopwatch swReplace = new Stopwatch();
	public long maxNumStates = 0;
	public long maxRelevantSuccessors = 0;
	public long counterTrimCalls = 0;
	public long counterFollowGroupCalls = 0;
	public long counterIsMergeableIntoCalls = 0;
	public long counterFollowTransitionCalls = 0;
	public long counterTransitionsCreated = 0;
	public long counterSerializedStates = 0;
	public long counterSerializedTransitions = 0;
	public long counterPathsAdded = 0;
	public long maxFollowGroupSetSize = 0;
	public long maxPivotsForOneSequence = 0;
	public long maxPivotsForOnePath = 0;
	public long maxNumOutTrs = 0;
	public long counterPrunedOutputs = 0;
	public long maxNumOutputItems = 0;




	// -- construction/clearing ---------------------------------------------------------------------------------------

	public DesqDfs(DesqMinerContext ctx) {
		this(ctx, false);
	}

	public DesqDfs(DesqMinerContext ctx, boolean skipDfaBuild) {
		super(ctx);
		sigma = ctx.conf.getLong("desq.mining.min.support");
		if(ctx.dict instanceof MiningDictionary)
			largestFrequentFid = ((MiningDictionary)ctx.dict).lastFrequentFid();
		else
			largestFrequentFid = ctx.dict.lastFidAbove(sigma);

		pruneIrrelevantInputs = ctx.conf.getBoolean("desq.mining.prune.irrelevant.inputs");
        useTwoPass = ctx.conf.getBoolean("desq.mining.use.two.pass");
		boolean useLazyDfa = ctx.conf.getBoolean("desq.mining.use.lazy.dfa");

		sendNFAs = ctx.conf.getBoolean("desq.mining.send.nfas", false);
		mergeSuffixes = ctx.conf.getBoolean("desq.mining.merge.suffixes", false);
		maxNumShuffleOutputItems = ctx.conf.getInt("desq.mining.shuffle.max.num.output.items");


		// create FST
		patternExpression = ctx.conf.getString("desq.mining.pattern.expression");
		this.fst = PatExUtils.toFst(ctx.dict, patternExpression);

		// create two pass auxiliary variables (if needed)
		if (useTwoPass && !skipDfaBuild) { // two-pass
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
		if(useTwoPass && !skipDfaBuild) {
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

		if(drawGraphs) fst.exportGraphViz(DesqDfsRunDistributedMiningLocally.useCase + "-fst.pdf");

		// other auxiliary variables
		root = new DesqDfsTreeNode(fst.numStates());
		currentNode = root;
		verbose = DesqDfsRunDistributedMiningLocally.verbose;

		fst.numberTransitions();


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
		if(trimInputSequences) inputSequences.trimToSize();
		if (useTwoPass && dfaStateSequences != null) {
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
					int spIndex = pos * fst.numStates() + toState.getId(); // TODO: understand this part and add it to piStep, if useful
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




	// ---------------- DesqDfs Distributed ---------------------------------------------------------


	public void addNFA(Sequence serializedNfa, long inputSupport, boolean allowBuffering, int partitionPivot) {
		// if we don't have a decoder object yet, create one
		if(nfaDecoder == null)
			nfaDecoder = new NFADecoder();

		// transform the incoming NFA to a representation by state, which is more suitable for efficient mining
		serializedNfa = nfaDecoder.convertPathToStateSerialization(serializedNfa, partitionPivot);
		super.addInputSequence(serializedNfa, inputSupport, allowBuffering);
	}

	/**
	 * Determine pivot items for one sequence, without storing them
	 * @param inputSequences
	 * @param verbose
	 * @return
	 * @throws IOException
	 */
	public Tuple2<Integer,Integer> determinePivotItemsForSequences(ObjectArrayList<Sequence> inputSequences, boolean verbose) throws IOException {
		int numSequences = 0;
		int totalPivotElements = 0;
		IntSet pivotElements;
		for(Sequence inputSequence: inputSequences) {
			pivotElements = getPivotItemsOfOneSequence(inputSequence);
			totalPivotElements += pivotElements.size();
			if(verbose) {
				System.out.println(inputSequence.toString() + " pivots:" + pivotElements.toString());
			}
			numSequences++;
		}
		return new Tuple2(numSequences,totalPivotElements);
	}

	/**
	 * Produces partitions for a given set of input sequences, producing one of two shuffle options:
	 * 1) input sequences
	 * 2) output sequences as nfas, with suffixes optionally merged
	 *
	 * @param inputSequences the input sequences
	 * @return partitions Map
	 */
	public Int2ObjectOpenHashMap<ObjectList<Sequence>> createPartitions(ObjectArrayList<Sequence> inputSequences, boolean verbose) throws IOException {

		partitions = new Int2ObjectOpenHashMap<>();
		IntSet pivotElements;
		Sequence inputSequence;
		ObjectList<Sequence> newPartitionSeqList;

		// for each input sequence, emit (pivot_item, transition_id)
		for(int seqNo=0; seqNo<inputSequences.size(); seqNo++) {
			inputSequence = inputSequences.get(seqNo);
			if (sendNFAs) {
				// NFAs
				createNFAPartitions(inputSequence, true, seqNo);
			} else {
				// input sequences
				pivotElements = getPivotItemsOfOneSequence(inputSequence);

				if(verbose) {
					System.out.println(inputSequence.toString() + " pivots:" + pivotElements.toString());
				}
				for (int pivot : pivotElements) {
					if (partitions.containsKey(pivot)) {
						partitions.get(pivot).add(inputSequence);
					} else {
						newPartitionSeqList = new ObjectArrayList<Sequence>();
						newPartitionSeqList.add(inputSequence);
						partitions.put(pivot, newPartitionSeqList);
					}
					if(DesqDfsRunDistributedMiningLocally.writeShuffleStats) DesqDfsRunDistributedMiningLocally.writeShuffleStats(seqNo, pivot, inputSequence.size());
				}
			}
		}
		return partitions;
	}


	/**
	 * Produces set of frequent output elements created by one input sequence by running the FST
	 * and storing all frequent output items
	 *
	 * We are using one pass for this for now, as it is generally safer to use.
	 *
	 * @param inputSequence
	 * @return pivotItems set of frequent output items of input sequence inputSequence
	 */
	public IntSet getPivotItemsOfOneSequence(IntList inputSequence) {
		pivotItems.clear();
		this.inputSequence = inputSequence;
		// check whether sequence produces output at all. if yes, produce output items
		if(useTwoPass) {
			// Run the DFA to find to determine whether the input has outputs and in which states
			// Then walk backwards and collect the output items
			if (dfa.acceptsReverse(inputSequence, dfaStateSequence, dfaInitialPos)) {
				//dfaStateSequences.add(dfaStateSequence.toArray(new DfaState[dfaStateSequence.size()]));

				// run the first incStep; start at all positions from which a final FST state can be reached
				for (int i = 0; i<dfaInitialPos.size(); i++) {
					// for those positions, start with the initial state
					piStep(-1, dfaInitialPos.getInt(i), fst.getInitialState(), 0); // TODO: not working at the moment
				}

				// clean up
				dfaInitialPos.clear();
			}
			dfaStateSequence.clear();

		} else { // one pass

			if (!pruneIrrelevantInputs || dfa.accepts(inputSequence)) {
				piStep(-1, 0, fst.getInitialState(), 0); // TODO: not working at the moment
			}
		}
		return pivotItems;
	}


	/** Produces the set of pivot items for a given input sequence and constructs a tree representation of the transitions
	 * generating all output sequences of this input sequence
	 *
	 * @param inputSequence
	 * @return pivotItems set of frequent output items of input sequence inputSequence
	 */
	public Int2ObjectOpenHashMap<Sequence> createNFAPartitions(IntList inputSequence, boolean buildPartitions, int seqNo) {

		// get the pivot elements with the corresponding paths through the FST
		this.inputSequence = inputSequence;
		path.clear();
		nfas.clear();
		// TODO: experiment whether nfas.trim() helps here

		if(!buildPartitions)
			serializedNFAs.clear();

		if(useTwoPass) {
			dfaStateSequence.clear();
			swFirstPass.start();
			if (dfa.acceptsReverse(inputSequence, dfaStateSequence, dfaInitialPos)) {

				swFirstPass.stop();
				// run the first incStep; start at all positions from which a final FST state can be reached
				swSecondPass.start();
				for (int i = 0; i < dfaInitialPos.size(); i++) {
					piStep(-1, dfaInitialPos.getInt(i), fst.getInitialState(), 0);
				}
				swSecondPass.stop();

				// clean up
				dfaInitialPos.clear();
			} else {
				swFirstPass.stop();
			}
		} else {
			piStep(-1, 0, fst.getInitialState(), 0);
		}


		if(nfas.size() > maxPivotsForOneSequence) maxPivotsForOneSequence = nfas.size();

		// For each pivot item, trim the NFA and output it
		for(Int2ObjectMap.Entry<OutputNFA> pivotAndNFA : nfas.int2ObjectEntrySet()) {
			int pivotItem =  pivotAndNFA.getIntKey();
			OutputNFA nfa = pivotAndNFA.getValue();

			// Trim the NFA for this pivot and directly mergeAndSerialize it
            Sequence output = nfa.mergeAndSerialize();

//			if(drawGraphs) drawSerializedNFA(output, DesqDfsRunDistributedMiningLocally.useCase + "-seq"+seqNo+"-pivot"+pivotItem+"-NFA.pdf", true, "");
			if(drawGraphs) nfa.exportGraphViz(DesqDfsRunDistributedMiningLocally.useCase + "-seq"+seqNo+"-pivot"+pivotItem+"-NFA.pdf");

			if(DesqDfsRunDistributedMiningLocally.writeShuffleStats) DesqDfsRunDistributedMiningLocally.writeShuffleStats(seqNo, pivotItem, numSerializedStates);

			if(buildPartitions) {
				// emit the list we constructed
				if (!partitions.containsKey(pivotItem)) {
					partitions.put(pivotItem, new ObjectArrayList<>());
				}
				partitions.get(pivotItem).add(output);
			} else {
				serializedNFAs.put(pivotItem, output);
			}
		}
		return serializedNFAs;
	}


	/** Runs one step (along compressed transition) in the FST in order to produce the set of frequent output items
	 *  and directly creates the partitions with the transition representation
	 *
	 * @param largestFidSeenIncoming largest seen fid so far. this will become the first pivot item later on
	 * @param pos   current position
	 * @param state current state
	 * @param level current level
	 */
	private void piStep(final int largestFidSeenIncoming, int pos, State state, int level) {
		counterTotalRecursions++;

		// if we reached a final state, we add the current set of pivot items at this state to the global set of pivot items for this sequences
		if(state.isFinal() && path.size() > processedPathSize) {
			processedPathSize = path.size();

			if(sendNFAs) {
				counterPathsAdded++;

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
					OutputNFA nfa;
					if(!nfas.containsKey(pivotItem)) {
						nfa = new OutputNFA(pivotItem);
						nfas.put(pivotItem, nfa);
					} else {
						nfa = nfas.get(pivotItem);
					}

					// if the path doesn't contain the pivot, something went wrong here.
					assert pivot(pathCopy) < pivotItem;

					// add the path to the pivot (incl. kicking out items>pivot) and retrieve next pivot item
					pivotItem = nfa.addOutLabelPathAndReturnNextPivot(pathCopy);
				}

				if(numPivotsEmitted > maxPivotsForOnePath) maxPivotsForOnePath = numPivotsEmitted;
			} else {
				// if we don't build the transition representation, just keep track of the pivot items
//				for(int i=0; i<currentPivotItems.size(); i++) {
//					pivotItems.add(currentPivotItems.exposeInts()[i]); // This isn't very nice, but it does not drop read elements from the heap. TODO: find a better way?
//				} // TODO: implement or get rid of this
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
				piStep(largestFidSeen, pos + 1, toState, level + 1);

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
					piStep(largestFidSeen, pos + 1, toState, level + 1);
					path.remove(path.size() - 1);
					processedPathSize = path.size();
				}
			}
		}
	}


	/**
	 * Specifies the output of a specific input item / transition combination
	 *
	 */
	private class OutputLabel implements Cloneable, Comparable<OutputLabel> {
	    Transition tr;
		int inputItem = -1;
		IntArrayList outputItems;

		OutputLabel(Transition tr, int inputItemFid, IntArrayList outputItems) {
			this.outputItems = outputItems;
			this.inputItem = inputItemFid;
			this.tr = tr;
		}

		@Override
		protected OutputLabel clone() {
			return new OutputLabel(tr, inputItem, outputItems.clone());
		}

		@Override
		public int hashCode() {
			return outputItems.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;

			OutputLabel other = (OutputLabel) obj;

			return this.outputItems.equals(other.outputItems);
		}

		/*
		 * @param other
		 * @return
		 */
		public int compareTo(OutputLabel other) {
		    return this.outputItems.compareTo(other.outputItems);
		}

		public String toString() {
			return "<" + tr.itemExpression() + ", " + inputItem + ", " + outputItems + ">";
		}
	}


	/**
	 * Mine with respect to a specific pivot item. e.g, filter out all non-pivot sequences
	 *   (e.g. filter out all sequences where pivotItem is not the max item)
	 * @param pivotItem
	 */
	public void minePivot(int pivotItem) {
		this.pivotItem = pivotItem;

		// clean up the NFA decoder
		if(nfaDecoder != null)
			nfaDecoder = null;


		// run the normal mine method. Filtering is done in expand() when the patterns are output
		if(sendNFAs)
			mineOnNFA();
		else
			mine();

		// reset the pivot item, just to be sure. pivotItem=0 means no filter
		this.pivotItem = 0;
	}

	public void mineOnNFA() {
		if (sumInputSupports >= sigma) {
			DesqDfsTreeNode root = new DesqDfsTreeNode(sendNFAs ? 1 : fst.numStates()); // if we use NFAs, we need only one BitSet per node
			currentNode = root;
			// and process all input sequences to compute the roots children
			for (int inputId = 0; inputId < inputSequences.size(); inputId++) {
				currentInputId = inputId;
				currentInputSequence = inputSequences.get(inputId);
				incStepOnNFA(0, true);
			}
			// the root has already been processed; now recursively grow the patterns
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
				projectedDatabaseIt.reset(childNode.projectedDatabase);
				currentInputId = -1;
				currentNode = childNode;

				do {
					// process next input sequence
					currentInputId += projectedDatabaseIt.nextNonNegativeInt();
					currentInputSequence = inputSequences.get(currentInputId);

					// iterate over state@pos snapshots for this input sequence
					boolean reachedFinalStateWithoutOutput = false;
					do {
						final int pos = projectedDatabaseIt.nextNonNegativeInt(); // position of next input item
						reachedFinalStateWithoutOutput |= incStepOnNFA(pos, expand);
					} while (projectedDatabaseIt.hasNext());

					// if we reached a final state without output, increment the support of this child node
					if (reachedFinalStateWithoutOutput) {
						support += currentInputSequence.weight;
					}

					// now go to next posting (next input sequence)
				} while (projectedDatabaseIt.nextPosting());
			}

			// output the patterns for the current child node if it turns out to be frequent
			if (support >= sigma) {
				// if we are mining a specific pivot partition (meaning, pivotItem>0), then we output only sequences with that pivot
				if (ctx.patternWriter != null && (pivotItem==0 || pivot(prefix) == pivotItem)) { // TODO: investigate what is pruned here (whether we can improve)
					ctx.patternWriter.write(prefix, support);
				} else {
					counterPrunedOutputs++;
				}
			}
			// expandOnNFA the child node
			childNode.pruneInfrequentChildren(sigma);
			childNode.projectedDatabase = null; // not needed anymore
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
		int nextInt = currentInputSequence.getInt(pos);
		int currentToStatePos = -1;
		while(nextInt != OutputNFA.END_FINAL && nextInt != OutputNFA.END) {

			if(nextInt < 0) { // this is a (new) toState
				currentToStatePos = -nextInt;
			} else { // otherwise, it's output items
				// we checked that these output items are relevant for the partition when we converted the NFA to a by-state representation
				if(expand) {
					assert currentToStatePos != -1;
					currentNode.expandWithTransition(nextInt, currentInputId, currentInputSequence.weight, currentToStatePos);
				}
			}
			pos++;
			nextInt = currentInputSequence.getInt(pos);
		}

		return nextInt == Integer.MIN_VALUE; // is true if this state is final
	}

	/**
	 * Determines the pivot item of a sequence
	 * @param sequence
	 * @return
	 */
	private int pivot(IntList sequence) {
		int pivotItem = 0;
		for(int item : sequence) {
			pivotItem = Math.max(item, pivotItem);
		}
		return pivotItem;
	}

	private int pivot(OutputLabel[] path) {
		int pivot = -1;
		for(OutputLabel ol : path) {
			pivot = Math.max(pivot, ol.outputItems.getInt(ol.outputItems.size()-1));
		}
		return pivot;
	}

	/**
	 * A NFA that produces the output sequences of one input sequence.
	 *
	 * Per input sequence, we send one NFA to each partition, if the input sequence produces at least one
	 * output sequence at that partition. Before sending the NFA, we trim it so we don't send paths that are
	 * not relevant for that partition. We do the trimming while serializing.
	 */
	private class OutputNFA {
		PathState root;
		int numPathStates = 0;
		int numPaths = 0;
		int numSerializedStates = 0;

		int pivot;

		// special integers for serialization
		final static public int FINAL = Integer.MIN_VALUE;
		final static public int END_FINAL = Integer.MIN_VALUE;
		final static public int END = Integer.MAX_VALUE;

		/** List of states in this NFA */
		private ObjectList<PathState> pathStates = new ObjectArrayList<>();

		/** List of leaf states in this NFA */
		private IntLinkedOpenHashSet leafs = new IntLinkedOpenHashSet();

		public OutputNFA(int pivot) {
			root = new PathState(this, null, 0);
			this.pivot = pivot;
		}

		/** Get the nth PathState of this NFA */
		public PathState getPathStateByNumber(int n) {
			return pathStates.get(n);
		}

		public PathState getMergeTarget(int n) {
			PathState state = getPathStateByNumber(n);
			if(state.id != state.mergedInto)
				return getPathStateByNumber(state.mergedInto);
			else
				return state;
		}

		/** A BitSet with a bit set for every position which holds a state id  */
		BitSet positionsWithStateIds = new BitSet();

		/**
		 * Add a path to this NFA.
		 * Trims the outputItems sets of the OutputLabls in the path for the pivot of this NFA.
		 * Returns the largest fid smaller than the pivot
		 *
		 * @param path
		 */
		protected int addOutLabelPathAndReturnNextPivot(OutputLabel[] path) {
			numPaths++;
			PathState currentState;
			int nextLargest = -1, nextLargestInThisSet;
			boolean seenPivotAsSingle = false;
			OutputLabel ol;

			// Run through the transitions of this path and add them to this NFA
			currentState = this.root;
			for(int i=0; i<path.length; i++) {
				ol = path[i];

				// drop all output items larger than the pivot
				while(ol.outputItems.getInt(ol.outputItems.size()-1) > pivot) {
					ol.outputItems.removeInt(ol.outputItems.size()-1);
				}

				// find next largest item
				if(!seenPivotAsSingle) { // as soon as we know there won't be another round we don't need to do this anymore
					nextLargestInThisSet = ol.outputItems.getInt(ol.outputItems.size()-1);
					// if the largest item is the pivot, we get the next-largest (if there is one)
					if (nextLargestInThisSet == pivot) {
						if (ol.outputItems.size() > 1) {
							nextLargestInThisSet = ol.outputItems.getInt(ol.outputItems.size()-2);
						} else {
							// if this set has only the pivot item, it will be empty next run. so we note
							//    that we need to stop after this run
							seenPivotAsSingle = true;
						}
					}
					nextLargest = Math.max(nextLargest, nextLargestInThisSet);
				}

				currentState = currentState.followTransition(ol, this);
			}

			// we ran through the path, so the current state is a final state.
			currentState.setFinal();

			if(seenPivotAsSingle)
				return -1; // meaning, we have an empty set for any pivot smaller the current pivot, so we stop
			else
				return nextLargest;
		}

		public Sequence mergeAndSerialize() {
			// merge
			swMerge.start();
			counterTrimCalls++;
			followGroup(leafs, true);
			swMerge.stop();

			// serialize
			swSerialize.start();
			Sequence send = new Sequence();
			DesqDfs.this.numSerializedStates = 0;
			root.serializeDfs(send);
			swSerialize.stop();

			return send;
		}

		/**
		 * Process a group of states. That means, we try to merge subsets of the passed group of states. Each of those groups,
		 * we process recursively.
		 * @param stateIds
		 */
		private void followGroup(IntLinkedOpenHashSet stateIds, boolean definitelyMergeable) {
		    if(stateIds.size() > maxFollowGroupSetSize) maxFollowGroupSetSize = stateIds.size();
			counterFollowGroupCalls++;
			IntBidirectionalIterator it = stateIds.iterator();
			int currentTargetStateId;
			int sId;
			PathState target, merge;

			// we use this bitset to mark states that we have already merged in this iteration.
			// we use that to make sure we don't process them more than once
			BitSet alreadyMerged = new BitSet();

			// as we go over the passed set of states, we collect predecessor groups, which we will process afterwards
			IntLinkedOpenHashSet predecessors = new IntLinkedOpenHashSet();

			int i = 0, j;
			while(it.hasNext()) {
				// Retrieve the next state of the group. We will use this one as a merge target for all the following states
				// in the list
				currentTargetStateId = it.nextInt();
				i++;
				if(alreadyMerged.get(i)) // we don't process this state if we have merged it into another state already
					continue;
				target = getMergeTarget(currentTargetStateId);

				// begin a new set of predecessors
                predecessors.clear();
				j = i;

				// Now compare this state to all the following states. If we find merge-compatible states, we merge them into this one
				while(it.hasNext()) {
					sId = it.nextInt();
					j++;

					if(alreadyMerged.get(j))  // don't process if we already merged this state into another one
						continue;

					merge = getMergeTarget(sId); // we retrieve the mapped number here. In case the state has already been merged somewhere else, we don't want to modify a merged state.

					if(merge.id == target.id) { // if the two states we are looking at are the same ones, we don't need to make any changes.
						alreadyMerged.set(j);
					} else if(definitelyMergeable || merge.isMergeableInto(target)) { // check whether the two states are mergeable

                        // we write down that we merged this state into the target.
                        merge.mergedInto = target.id;

                        // also rewrite the original entry if the state we just merged was merged before
                        if(merge.id != sId)
                            getPathStateByNumber(sId).mergedInto = target.id;

						// mark this state as merged, so we don't process it multiple times
						alreadyMerged.set(j);

						// if we have a group of predecessors (more than one), we process them as group
						if(target.predecessor != null && merge.predecessor != null) {
							if (target.predecessor.id != merge.predecessor.id) {
								predecessors.add(merge.predecessor.id);
							}
						}
					}
				}

				// rewind the iterator
				it.back(j-i);

				// if we have multiple predecessors, process them as group (later on)
				if(predecessors.size()>0) {
					predecessors.add(target.predecessor.id);
					followGroup(predecessors, false);
				}
			}
		}

		/**
		 * Export the PathStates of this NFA with their transitions to PDF
		 * @param file
		 */
		public void exportGraphViz(String file) {
			FstVisualizer fstVisualizer = new FstVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
			fstVisualizer.beginGraph();
			for(PathState s : pathStates) {
				if (s.mergedInto == s.id) {
					for (Object2ObjectMap.Entry<OutputLabel, PathState> trEntry : s.outTransitions.object2ObjectEntrySet()) {
						OutputLabel ol = trEntry.getKey();
						String label;
						label = ol.outputItems.toString();
						fstVisualizer.add(String.valueOf(s.id), label, String.valueOf(trEntry.getValue().mergedInto));
					}
					if (s.isFinal)
						fstVisualizer.addFinalState(String.valueOf(s.id));
				}
			}
			fstVisualizer.endGraph();
		}
	}


	/**
	 * One state in a path through the Fst. We use this to build the NFAs we shuffle to the partitions
	 *
	 */
	private class PathState {
		protected final int id;
		protected int mergedInto;
		protected boolean isFinal = false;
		protected int writtenAtPos = -1;
		protected int level;
		protected OutputNFA nfa;
		protected int writtenNum = -1;


		/** Forward pointers */
		protected Object2ObjectSortedMap<OutputLabel,PathState> outTransitions;

		/** Backward pointer (in the tree, each state has only one incoming transition) */
		protected PathState predecessor;

		protected PathState(OutputNFA nfa, PathState from, int level) {
			this.nfa = nfa;
			id = nfa.numPathStates++;
			mergedInto = id;
			nfa.pathStates.add(this);
			outTransitions = new Object2ObjectAVLTreeMap<>();

			predecessor = from;
			this.level = level;

			nfa.leafs.add(this.id);
		}

		/**
		 * Starting from this state, follow a given transition with given input item. Returns the state the transition
		 * leads to.
		 *
		 * @param ol		the OutputLabel for this transition
		 * @param nfa       the nfa the path is added to
		 * @return
		 */
		protected PathState followTransition(OutputLabel ol, OutputNFA nfa) {
			counterFollowTransitionCalls++;

			// if we have a tree branch with this transition already, follow it and return the state it leads to
			if(outTransitions.containsKey(ol)) {
				// return this state
				PathState toState = outTransitions.get(ol);
				return toState;
			} else {
				// if we don't have a path starting with that transition, we create a new one
				//   (unless it is the last transition, in which case we want to transition to the global end state)
				PathState toState;
				// if we merge suffixes, create a state with backwards pointers, otherwise one without
				toState = new PathState(nfa, this, this.level+1);

				// add the transition and the created state to the outgoing transitions of this state and return the state the transition moves to
				outTransitions.put(ol.clone(), toState);

				if(outTransitions.size() == 1)
					nfa.leafs.remove(this.id);

				counterTransitionsCreated++;
				return toState;
			}
		}

		/** Mark this state as final */
		public void setFinal() {
			this.isFinal = true;
		}

		/**
		 * Determine whether this state is mergeable into the passed target state
		 * @param target
		 * @return
		 */
		public boolean isMergeableInto(PathState target) {
			counterIsMergeableIntoCalls++;

			if(this.isFinal != target.isFinal)
				return false;

			if(this.outTransitions.size() != target.outTransitions.size())
				return false;

			ObjectIterator<Object2ObjectMap.Entry<OutputLabel,PathState>> aIt = this.outTransitions.object2ObjectEntrySet().iterator();
			ObjectIterator<Object2ObjectMap.Entry<OutputLabel,PathState>> bIt = target.outTransitions.object2ObjectEntrySet().iterator();
			Object2ObjectMap.Entry<OutputLabel,PathState> a, b;

			// the two states have the same number of out transitions (checked above)
			// now we only check whether those out transitions have the same outKey and point towards the same state

			while(aIt.hasNext()) {
				a = aIt.next();
				b = bIt.next();

				if(!a.getKey().equals(b.getKey()))
					return false;

				if(a.getValue().mergedInto != b.getValue().mergedInto)
					return false;
			}

			return true;
		}


		/**
		 * Serializes this state using by-path representation to the given IntList.
		 * Processes un-processed children states recursively in DFS manner.
		 *
		 * @param send
		 */
		protected void serializeDfs(IntList send) {
			counterSerializedStates++;
			OutputLabel ol;
			PathState toState;

			// keep track of the number of serialized states
            writtenNum = nfa.numSerializedStates;
			nfa.numSerializedStates++;

			if(isFinal) {
				send.add(OutputNFA.FINAL);
				// TODO: we can improve here. if there is only one final state for an NFA and it's the last one, we can leave this out
			}

			boolean firstTransition = true;
			for(Object2ObjectMap.Entry<OutputLabel,PathState> entry : outTransitions.object2ObjectEntrySet()) {
				counterSerializedTransitions++;
				ol = entry.getKey();
				toState = entry.getValue();

				// if this is the first transition we process at this state, we don't need to write and structural information.
				// we just follow the first path
				if(firstTransition) {
					firstTransition = false;
				} else {
					send.add(-this.writtenNum);
				}

				// serialize the output label
				if(ol.outputItems.size() > 1 && ol.outputItems.getInt(1) <= nfa.pivot) { // multiple output items, so we encode them using the trasition
					// TODO: the current serialization format doesn't allow us to send two output items directly. We need to think about this.
					send.add(Integer.MIN_VALUE + 1 + fst.getItemExId(ol.tr));
					send.add(ol.inputItem); // TODO: we can generalize this for the pivot
				} else { // there is only one (relevant) output item, and we know it's the first in the list
					send.add(ol.outputItems.getInt(0));
				}

				// serialize the to-state, either by processing it or by noting down it's number
                toState = nfa.getMergeTarget(toState.id);
                if(toState.writtenNum == -1) {
                    toState.serializeDfs(send);
                } else {
                    send.add(-toState.writtenNum);
                }
			}
		}
	}

	/**
	 * A class to decode serialized NFAs from by-path to by-state representation.
	 * Reuses internal data structures between multiple calls to {@link #convertPathToStateSerialization(Sequence, int)}
	 */
	private class NFADecoder {
		ObjectList<IntArrayList> outgoing = new ObjectArrayList<>();
		int numReadStates, item, inputItem, outputItem, currentState = 0, toState, itExId, sPos, outgoingIntegers, len;
		BitSet finalStates = new BitSet();
		IntList currentOutgoing;
		IntList writtenAtPos = new IntArrayList();
		int maxItemExValue = Integer.MIN_VALUE/2;

		/** Converts the NFA serialized in serializedNFA to a state-based representation **/
		public Sequence convertPathToStateSerialization(Sequence serializedNFA, int partitionPivot) {
			numReadStates = 0;
			finalStates.clear();
			outgoingIntegers = 0;
			currentState = 0;
			// prep outgoing array for root state
			prepOutgoing(0);

			for(int pos = 0; pos<serializedNFA.size(); pos++) {
				item = serializedNFA.getInt(pos);
				if(item == OutputNFA.FINAL) {
				    // mark current state as final
					finalStates.set(currentState);
				} else {
					// this is a transition

					// we might have an explicit toState
					if(item <= 0 && item > maxItemExValue) {
						currentState = -item;
						pos++;
						item = serializedNFA.getInt(pos);
					}

					// from here on, it's the outgoing transition. with either one positive output item, or a negative item ex id + input item
					if(item > 0) {
						sPos = pos+1;
					} else {
						sPos = pos+2;
					}

					// look-ahead for a explicit toState
					if(sPos >= serializedNFA.size() || serializedNFA.getInt(sPos) > 0 || serializedNFA.getInt(sPos) < maxItemExValue) {
						// there is no explicit toState. so we generate an implicit one
						numReadStates++;
						toState = numReadStates;
						prepOutgoing(toState);
					} else {
						// there is an explicit toState
						toState = -serializedNFA.getInt(sPos);
						sPos++;
					}

					// store the toState and the output items
					currentOutgoing = outgoing.get(currentState);
					currentOutgoing.add(-toState);
					outgoingIntegers++;
					if(item > 0) {
						// there is only one output item, we add it and are done
						currentOutgoing.add(item);
						outgoingIntegers++;
					} else {
						// there are multiple output items. we produce all output items and add ones relevant for this partition
						inputItem = serializedNFA.getInt(pos+1);
						itExId = item - Integer.MIN_VALUE - 1;
						Iterator<ItemState> outIt = fst.getPrototypeTransitionByItemExId(itExId)
								                       .consume(inputItem, itCaches.get(0));
						while(outIt.hasNext()) {
							outputItem = outIt.next().itemFid;
							if(partitionPivot >= outputItem) {
                                currentOutgoing.add(outputItem);
								outgoingIntegers++;
							}
						}
					}

					// advance to the new state and set the new reading position correctly
					currentState = toState;
					pos = sPos-1;
				}
			}

			// Create the new serialization. For that, we reuse the array backing the input NFA.
			// We keep track of where we serialized which state. In a second run over the serialization, we replace
			// the state numbers by their index in the array.
			serializedNFA.size(outgoingIntegers+numReadStates+1);
			int[] byState = serializedNFA.elements();
			writtenAtPos.size(numReadStates+1);

			for(int stateNo=0, pos=0; stateNo<=numReadStates; stateNo++) {
			    // we write state stateNo at position pos
				writtenAtPos.set(stateNo,pos);

				// write outgoing data
				if(stateNo < outgoing.size()) { // if this state has outgoing transitions
                    assert !outgoing.get(stateNo).contains(-stateNo);
					len = outgoing.get(stateNo).size();
					System.arraycopy(outgoing.get(stateNo).elements(), 0, byState, pos, len);
				} else {
					len = 0;
				}
				pos += len; // accounting for the outgoing data

				// write the end marker (final or non-final)
				if(finalStates.get(stateNo))
					byState[pos] = OutputNFA.END_FINAL;
				else
					byState[pos] = OutputNFA.END;

				pos++; // accounting for the state end marker
			}

			// replace state numbers by their index in the array
			for(int pos = 0; pos<serializedNFA.size(); pos++) {
				if(byState[pos] < 0 && byState[pos] != OutputNFA.END_FINAL) {
					byState[pos] =  -writtenAtPos.get(-byState[pos]);
				}
			}

			return serializedNFA;
		}

		/** Prepare the outgoing IntList for the state with number stateNo: make sure outgoing array is large enough
		 * and item array exists. If it already exists, clear the previously used array. */
		private void prepOutgoing(int stateNo) {
			if(stateNo >= outgoing.size()) {
				outgoing.add(new IntArrayList());
			} else {
				outgoing.get(stateNo).clear();
			}
		}


		/** Exports the fst using graphviz (type based on extension, e.g., "gv" (source file), "pdf", ...) */
		public void exportGraphViz(String file) {
			FstVisualizer fstVisualizer = new FstVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
			fstVisualizer.beginGraph();
			IntList outItems = new IntArrayList();
			int toState = -1;
            for(int i=0; i<numReadStates+1; i++) {
				if(i < outgoing.size()) {
					for(int j=0; j<outgoing.get(i).size(); j++) {
						int item = outgoing.get(i).getInt(j);
						if(item < 0) {
							// a new to state, so write the previous edge (if there was a previous one) and change to new toState
							if(!outItems.isEmpty()) {
								fstVisualizer.add(String.valueOf(i), outItems.toString(), String.valueOf(toState));
							}
							toState = -item;
							outItems.clear();
						} else {
						    // note output item
							outItems.add(item);
						}
					}
					// write last edge of the last state
					if(!outItems.isEmpty()) {
						fstVisualizer.add(String.valueOf(i), outItems.toString(), String.valueOf(toState));
						outItems.clear();
					}
				}
				// mark final
				if(finalStates.get(i))
					fstVisualizer.addFinalState(String.valueOf(i));
			}
			fstVisualizer.endGraph();
		}
	}
}