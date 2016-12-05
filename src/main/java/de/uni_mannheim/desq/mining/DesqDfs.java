package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.examples.DesqDfsRunDistributedMiningLocally;
import de.uni_mannheim.desq.fst.*;
import de.uni_mannheim.desq.fst.BasicTransition.OutputLabelType;
import de.uni_mannheim.desq.fst.graphviz.FstVisualizer;
import de.uni_mannheim.desq.patex.PatEx;
import de.uni_mannheim.desq.util.CloneableIntHeapPriorityQueue;
import de.uni_mannheim.desq.util.DesqProperties;
import de.uni_mannheim.desq.util.PrimitiveUtils;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.apache.commons.io.FilenameUtils;
import scala.Tuple2;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.io.IOException;
import java.util.*;

public final class DesqDfs extends MemoryDesqMiner {
	private static final Logger logger = Logger.getLogger(DesqDfs.class);

	// -- parameters for mining ---------------------------------------------------------------------------------------

	/** Minimum support */
	final long sigma;

	/** The pattern expression used for mining */
	final String patternExpression;

	/** If true, input sequences that do not match the pattern expression are pruned */
	final boolean pruneIrrelevantInputs;

	/** If true, the two-pass algorithm for DesqDfs is used */
	final boolean useTwoPass;


	// -- helper variables --------------------------------------------------------------------------------------------

    /** Stores the final state transducer for DesqDfs (one-pass) */
	private final Fst fst;

    /** Stores the largest fid of an item with frequency at least sigma. Zsed to quickly determine
     * whether an item is frequent (if fid <= largestFrequentFid, the item is frequent */
	private final int largestFrequentFid;

    /** Stores iterators over output item/next state pairs for reuse. */
	private final ArrayList<Iterator<ItemState>> itemStateIterators = new ArrayList<>();

    /** An iterator over a projected database (a posting list) for reuse */
	private final PostingList.Iterator projectedDatabaseIt = new PostingList.Iterator();

	/** The root node of the search tree. */
	private final DesqDfsTreeNode root;

	/** Bundles arguments for {@link #incStep(IncStepArgs, int)} */
    private final IncStepArgs current = new IncStepArgs();

	// -- helper variables for pruning and twopass --------------------------------------------------------------------

	/** The DFA corresponding to the FST (pruning) or reverse FST (two-pass). */
	private final Dfa dfa;

    /** For each relevant input sequence, the sequence of states taken by dfa (two-pass only) */
	private final ArrayList<DfaState[]> dfaStateSequences;

    /** A sequence of EDFA states for reuse (two-pass only) */
	private final ArrayList<DfaState> dfaStateSequence;

    /** A sequence of positions for reuse (two-pass only) */
	private final IntList dfaInitialPos;



	// -- helper variables for distributing --------------------------------------------------------------------------
	/** Set of output items created by one input sequence */
	final IntSet pivotItems = new IntAVLTreeSet();

	/** Storing the current input sequence */
	protected IntList inputSequence;

	/** pivot item of the currently mined partition */
	private int pivotItem = 0;

	/** Current prefix (used for finding the potential output items of one sequence) */
	final Sequence prefix = new Sequence();

	/** For each current recursion level, a set of to-states that already have been visited by a non-pivot transition */
	final ArrayList<IntSet> nonPivotExpandedToStates = new ArrayList<>();

	/** If true, we maintain a set of already visited to-states for each state */
	final boolean skipNonPivotTransitions;

	/** If true, we keep track of the min and max pivots coming up from recursion for each (state,pos) pair */
	final boolean useMaxPivot;

	/** for comparing to the old version */
	final boolean useFirstPCVersion;

	/** If true, we recurse through compressed transitions and keep track of set of potential pivot elements */
	final boolean useCompressedTransitions;

	/** If true, DesqDfs Distributed trends transition-encoded sequences between transitions */
	final boolean useTransitionRepresentation;

	/** If true, DesqDfs Distributed uses tree-structered transitions as shuffle format */
	final boolean useTreeRepresentation;

	/** If true, DesqDfs Distributed merges shared suffixes in the tree representation */
	final boolean mergeSuffixes;

	/** If true, DesqDfs Distributed generalizes input items of self_generalize transitions for each pivot item before sending them */
	final boolean generalizeInputItemsBeforeSending;

	/** Stores one Transition iterator per recursion level for reuse */
	final ArrayList<Iterator<Transition>> transitionIterators = new ArrayList<>();

	/** Ascendants of current input item */
	IntAVLTreeSet ascendants = new IntAVLTreeSet();

	/** Current transition */
	BasicTransition tr;

	/** Current to-state */
	State toState;

	/** Item added by current transition */
	int addItem;

	/** For each pivot item, a list of paths generating output for that pivot item on the current input sequence */
	private Int2ObjectOpenHashMap<ObjectList<IntList>> paths = new Int2ObjectOpenHashMap<>();

	/** For each pivot item, a NFA producing the output sequences for that pivot item from the current input sequence */
	private Int2ObjectOpenHashMap<OutputNFA> nfas = new Int2ObjectOpenHashMap<>();
	private Int2ObjectOpenHashMap<Sequence> serializedNFAs = new Int2ObjectOpenHashMap<>();

	/** The output partitions */
	Int2ObjectOpenHashMap<ObjectList<IntList>> partitions;

	/** Stores one pivot element heap per level for reuse */
	final ArrayList<CloneableIntHeapPriorityQueue> pivotItemHeaps = new ArrayList<>();

	/** Stats about pivot element search */
	public long counterTotalRecursions = 0;
	public long counterNonPivotTransitionsSkipped = 0;
	public long counterMaxPivotUsed = 0;

	// -- construction/clearing ---------------------------------------------------------------------------------------

	public DesqDfs(DesqMinerContext ctx) {
		super(ctx);
		sigma = ctx.conf.getLong("desq.mining.min.support");
		largestFrequentFid = ctx.dict.lastFidAbove(sigma);
		pruneIrrelevantInputs = ctx.conf.getBoolean("desq.mining.prune.irrelevant.inputs");
		useTwoPass = ctx.conf.getBoolean("desq.mining.use.two.pass");
		skipNonPivotTransitions = ctx.conf.getBoolean("desq.mining.skip.non.pivot.transitions", false);
		useMaxPivot = ctx.conf.getBoolean("desq.mining.use.minmax.pivot", false);
		useFirstPCVersion = ctx.conf.getBoolean("desq.mining.use.first.pc.version", false);
		useCompressedTransitions = ctx.conf.getBoolean("desq.mining.pc.use.compressed.transitions", false);
		useTransitionRepresentation = ctx.conf.getBoolean("desq.mining.use.transition.representation", false);
		useTreeRepresentation = ctx.conf.getBoolean("desq.mining.use.tree.representation", false);
		mergeSuffixes = ctx.conf.getBoolean("desq.mining.merge.suffixes", false);
		generalizeInputItemsBeforeSending = ctx.conf.getBoolean("desq.mining.generalize.input.items.before.sending", false);

		// create FST
		patternExpression = ctx.conf.getString("desq.mining.pattern.expression");
		PatEx p = new PatEx(patternExpression, ctx.dict);
		this.fst = p.translate();
		fst.minimize(); //TODO: move to translate
		fst.annotate();

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
			this.dfa = Dfa.createReverseDfa(fst, ctx.dict, largestFrequentFid, true);
		} else if (pruneIrrelevantInputs) {
			// construct the DFA to prune irrelevant inputs
			// the DFA is constructed for the forward FST
			this.dfa = Dfa.createDfa(fst, ctx.dict, largestFrequentFid, false);
		} else {
			this.dfa = null;
			
		}

		fst.numberTransitions();
//		fst.exportGraphViz("fst.pdf");

		// other auxiliary variables
		root = new DesqDfsTreeNode(fst.numStates());
		current.node = root;
	}

	public static DesqProperties createConf(String patternExpression, long sigma) {
		DesqProperties conf = new DesqProperties();
		conf.setProperty("desq.mining.miner.class", DesqDfs.class.getCanonicalName());
		conf.setProperty("desq.mining.min.support", sigma);
		conf.setProperty("desq.mining.pattern.expression", patternExpression);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", false);
		conf.setProperty("desq.mining.use.two.pass", false);
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
		current.node = root;
	}

	// -- processing input sequences ----------------------------------------------------------------------------------

	@Override
	public void addInputSequence(IntList inputSequence, long inputSupport, boolean allowBuffering) {
	    if(useTransitionRepresentation) {
			super.addInputSequence(inputSequence, inputSupport, allowBuffering);


		} else {
			// two-pass version of DesqDfs
			if (useTwoPass) {
				// run the input sequence through the DFA and compute the state sequences as well as the positions from
				// which a final FST state is reached
				if (dfa.acceptsReverse(inputSequence, dfaStateSequence, dfaInitialPos)) {
					// we now know that the sequence is relevant; remember it
					super.addInputSequence(inputSequence, inputSupport, allowBuffering);
					dfaStateSequences.add(dfaStateSequence.toArray(new DfaState[dfaStateSequence.size()]));

					// run the first incStep; start at all positions from which a final FST state can be reached
//				assert current.node == root;
					current.inputId = inputSequences.size() - 1;
					current.inputSequence = inputSequences.get(current.inputId);
					current.dfaStateSequence = dfaStateSequences.get(current.inputId);
					for (int i = 0; i < dfaInitialPos.size(); i++) {
						// for those positions, start with the initial state
						incStepTraditional(dfaInitialPos.getInt(i), fst.getInitialState(), 0);
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

				// and run the first inc step
				current.inputId = inputSequences.size() - 1;
				current.inputSequence = inputSequences.get(current.inputId);
				incStepTraditional(0, fst.getInitialState(), 0);
			}
		}
	}


	// ---------------- DesqDfs Distributed ---------------------------------------------------------

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
		//Sequence inputSequence = new Sequence();
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
	 * Produces partitions for a given set of input sequences, producing one of three shuffle options:
	 * 1) input sequences
	 * 2) output sequences as concatenated transition pathes
	 * 3) output sequences as nfas, with suffixes optionally merged
	 *
	 * @param inputSequences the input sequences
	 * @return partitions Map
	 */
	public Int2ObjectOpenHashMap<ObjectList<IntList>> createPartitions(ObjectArrayList<Sequence> inputSequences, boolean verbose) throws IOException {

		partitions = new Int2ObjectOpenHashMap<>();
		IntSet pivotElements;
		Sequence inputSequence;
		ObjectList<IntList> newPartitionSeqList;

		// for each input sequence, emit (pivot_item, transition_id)
		for(int seqNo=0; seqNo<inputSequences.size(); seqNo++) {
			inputSequence = inputSequences.get(seqNo);
			if (useTreeRepresentation) {
				// NFAs
				createNFAPartitions(inputSequence, true, seqNo);
			} else if(useTransitionRepresentation) { // concatenated transition representation
                // concatenated paths
				createConcatenatedPathTransitions(inputSequence, true, seqNo);
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
						newPartitionSeqList = new ObjectArrayList<IntList>();
						newPartitionSeqList.add(inputSequence);
						partitions.put(pivot, newPartitionSeqList);
					}
					if(DesqDfsRunDistributedMiningLocally.writeShuffleStats) DesqDfsRunDistributedMiningLocally.writeShuffleStats(seqNo, pivot, 1, inputSequence.size());
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
					if(useCompressedTransitions)
						piStepCompressed(null, dfaInitialPos.getInt(i), fst.getInitialState(), 0);
					else
						piStep(0, dfaInitialPos.getInt(i), fst.getInitialState(), 0);
				}

				// clean up
				dfaInitialPos.clear();
			}
			dfaStateSequence.clear();

		} else { // one pass

			if (!pruneIrrelevantInputs || dfa.accepts(inputSequence)) {
				if(useFirstPCVersion) {
					piStepOnePassV1(0, fst.getInitialState(), 0);
				} else if(useCompressedTransitions) {
					piStepCompressed(null, 0, fst.getInitialState(), 0);
				} else {
					piStep(0, 0, fst.getInitialState(), 0);
				}
			}
		}
		return pivotItems;
	}


	/**
	 * Produces the set of pivot items for a given input sequence and emits one concatenated collection of paths through the
	 * FST for each pivot element, in the current format:
	 * (pivot_element, {(no. pathes), (start-pos-path1), [(start-pos-path2), ...], (trNo,inpItem),(trNo,inpItem),...})
	 *
	 * @param inputSequence
	 * @return pivotItems set of frequent output items of input sequence inputSequence
	 */
	public Int2ObjectOpenHashMap<Sequence> createConcatenatedPathTransitions(IntList inputSequence, boolean buildPartitions, int seqNo) {
		pivotItems.clear();
		this.inputSequence = inputSequence;

		// get the pivot elements with the corresponding paths through the FST
		prefix.clear();
		paths.clear();

		if(!buildPartitions) {
			serializedNFAs.clear();
		}

		if(useTwoPass) {
			if (dfa.acceptsReverse(inputSequence, dfaStateSequence, dfaInitialPos)) {
				dfaStateSequences.add(dfaStateSequence.toArray(new DfaState[dfaStateSequence.size()]));

				// run the first incStep; start at all positions from which a final FST state can be reached
				for (int i = 0; i < dfaInitialPos.size(); i++) {
					piStepCompressed(null, dfaInitialPos.getInt(i), fst.getInitialState(), 0);
				}

				// clean up
				dfaInitialPos.clear();
			}
			dfaStateSequence.clear();
		} else {
			piStepCompressed(null, 0, fst.getInitialState(), 0);
		}

		// join the pathes together into one collection of paths
		for(Map.Entry<Integer, ObjectList<IntList>> partition : paths.entrySet()) {
			int pivotItem = partition.getKey();
			ObjectList<IntList> paths = partition.getValue();

			Sequence sendList = new Sequence();
			// first element of the sent list is the number N of paths we are sending
			sendList.add(paths.size());

			// the next N elements are the starting positions for the N paths, we set them to 0 now and fill them in the second loop
			for(int i=0; i<paths.size(); i++) {
				sendList.add(0);
			}

			// append all the paths and fill in the start-pos at the beginning of the sequence
			int currentOffset = 1+paths.size();
			int pathNo = 0;
			for(IntList path :  partition.getValue()) {
				sendList.set(1+pathNo,currentOffset);
				pathNo++;
				currentOffset += path.size();
				sendList.addAll(path);
			}

			if(DesqDfsRunDistributedMiningLocally.writeShuffleStats) DesqDfsRunDistributedMiningLocally.writeShuffleStats(seqNo, pivotItem, paths.size(), sendList.size());

			if(buildPartitions) {
				// emit the list we constructed
				if (!partitions.containsKey(pivotItem)) {
					partitions.put(pivotItem, new ObjectArrayList<IntList>());
				}
				partitions.get(pivotItem).add(sendList);
			} else {
				serializedNFAs.put(pivotItem, sendList);
			}
		}
		return serializedNFAs;
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
		pivotItems.clear();
		prefix.clear();
        nfas.clear();
        if(!buildPartitions)
        	serializedNFAs.clear();

		if(useTwoPass) {
			dfaStateSequence.clear();
			if (dfa.acceptsReverse(inputSequence, dfaStateSequence, dfaInitialPos)) {
				//dfaStateSequences.add(dfaStateSequence.toArray(new DfaState[dfaStateSequence.size()]));

				// run the first incStep; start at all positions from which a final FST state can be reached
				for (int i = 0; i < dfaInitialPos.size(); i++) {
					piStepCompressed(null, dfaInitialPos.getInt(i), fst.getInitialState(), 0);
				}

				// clean up
				dfaInitialPos.clear();
			}
		} else {
			piStepCompressed(null, 0, fst.getInitialState(), 0);
		}


        // Serialize the partition NFAs for this the item and append them to the partitions
		for(Map.Entry<Integer, OutputNFA> pivotAndNFA: nfas.entrySet()) {
			int pivotItem = pivotAndNFA.getKey();
			OutputNFA nfa = pivotAndNFA.getValue();

			if(mergeSuffixes) {
				nfa.mergeSuffixes();
			}

//			System.out.println("Printing path tree for pivot " + pivotItem + " and seqNo " + seqNo);
//			nfa.exportGraphViz("./OutputNFAs/PathTree-pivot"+pivotItem+"-seqNo"+seqNo+".pdf", false);
			// Next step: construct the sequence we will send to the partition
			Sequence sendList = new Sequence();
			nfa.write(sendList);

			if(DesqDfsRunDistributedMiningLocally.writeShuffleStats) DesqDfsRunDistributedMiningLocally.writeShuffleStats(seqNo, pivotItem, nfa.numPaths, sendList.size());

			if(buildPartitions) {
				// emit the list we constructed
				if (!partitions.containsKey(pivotItem)) {
					partitions.put(pivotItem, new ObjectArrayList<IntList>());
				}
				partitions.get(pivotItem).add(sendList);
			} else {
				serializedNFAs.put(pivotItem, sendList);
			}
		}
		return serializedNFAs;
	}



	/** Runs one step (along compressed transition) in the FST in order to produce the set of frequent output items
	 *  and directly creates the partitions with the transition representation
	 *
	 * @param currentPivotItems set of current potential pivot items
	 * @param pos   current position
	 * @param state current state
	 * @param level current level
	 */
	private void piStepCompressed(CloneableIntHeapPriorityQueue currentPivotItems, int pos, State state, int level) {
		counterTotalRecursions++;
		// if we reached a final state, we add the current set of pivot items at this state to the global set of pivot items for this sequences
		if(state.isFinal() && currentPivotItems.size() != 0 && (!useTransitionRepresentation || prefix.size() != 0)) {
			if(useTransitionRepresentation) {
				// add (pivot_item, transition-input-sequence) pairs to the partitions
				IntSet emittedPivots = new IntAVLTreeSet(); // This is a hack. we should keep track of a set, not possibly mutliple items. TODO
				// on second thought, maybe while we still use one-pass, this might not be 100% stupid
				IntList path = prefix.clone();
				for (int i = 0; i < currentPivotItems.size(); i++) {
					int pivotItem = currentPivotItems.exposeInts()[i]; // This isn't very nice, but it does not drop read elements from the heap. TODO: find a better way?
					if (!emittedPivots.contains(pivotItem)) {
						emittedPivots.add(pivotItem);
						if (!useTreeRepresentation) {
							if (paths.containsKey(pivotItem)) {
								paths.get(pivotItem).add(path);
							} else {
								ObjectList<IntList> add = new ObjectArrayList<>();
								add.add(path);
								paths.put(pivotItem, add);
							}
						} else {
							OutputNFA nfa;
							if (!nfas.containsKey(pivotItem)) {
								// create the nfa
								nfa = new OutputNFA(pivotItem);
								nfas.put(pivotItem, nfa);
							} else {
								nfa = nfas.get(pivotItem);
							}
							nfa.addPath(prefix);
						}
					}
				}
			} else {
			    // if we don't build the transition representation, just keep track of the pivot items
				for(int i=0; i<currentPivotItems.size(); i++) {
                    pivotItems.add(currentPivotItems.exposeInts()[i]); // This isn't very nice, but it does not drop read elements from the heap. TODO: find a better way?
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

		// get set for storing potential pivot elements
		CloneableIntHeapPriorityQueue newCurrentPivotItems;
		if(level >= pivotItemHeaps.size()) {
			newCurrentPivotItems = new CloneableIntHeapPriorityQueue();
			pivotItemHeaps.add(newCurrentPivotItems);
		} else {
			newCurrentPivotItems = pivotItemHeaps.get(level);
		}

		addItem = -1;

		// follow each relevant transition
		while(transitionIt.hasNext()) {
			tr = (BasicTransition) transitionIt.next();
			toState = tr.getToState();
			OutputLabelType ol = tr.getOutputLabelType();

			// We handle the different output label types differently
			if(ol == OutputLabelType.EPSILON) { // EPS
				// an eps transition does not introduce potential pivot elements, so we simply continue recursion
				piStepCompressed(currentPivotItems, pos+1, toState, level+1);

			} else if (ol == OutputLabelType.CONSTANT || ol == OutputLabelType.SELF) { // CONSTANT and SELF
				// SELF and CONSTANT transitions both yield exactly one new potential pivot item.

				// retrieve input item
				if(ol == OutputLabelType.CONSTANT) { // CONSTANT
					addItem = tr.getOutputLabel();
				} else { // SELF
					addItem = itemFid;
				}
				// If the output item is frequent, we merge it into the set of current potential pivot items and recurse
				if (largestFrequentFid >= addItem) {
					//CloneableIntHeapPriorityQueue newCurrentPivotItems;
					if(currentPivotItems == null) { // set of pivot elements is empty so far, so no update is necessary, we just create a new set
						//newCurrentPivotItems = new CloneableIntHeapPriorityQueue();
						newCurrentPivotItems.clear();
						newCurrentPivotItems.enqueue(addItem);
					} else { // update the set of current pivot elements
						// get the first half: current[>=min(add)]
						newCurrentPivotItems.startFromExisting(currentPivotItems);
						while(newCurrentPivotItems.size() > 0  && newCurrentPivotItems.firstInt() < addItem) {
							newCurrentPivotItems.dequeueInt();
						}
						// join in the second half: add[>=min(current)]		  (don't add the item a second time if it is already in the heap)
						if(addItem >= currentPivotItems.firstInt() && (newCurrentPivotItems.size() == 0 || addItem != newCurrentPivotItems.firstInt())) {
							newCurrentPivotItems.enqueue(addItem);
						}
					}
					// we put the current transition together with the input item onto the prefix and take it off when we come back from recursion
					if(useTransitionRepresentation) {
						prefix.add(tr.getTransitionNumber());

						// TODO: instead of doing this, we should write transitions according to output item equivalency
						// meaning, we should store both CONSTANT and SELF like <-outputItemFid, 0>
//						if (tr.getOutputLabelType() == OutputLabelType.CONSTANT)
//							prefix.add(0);
//						else
							prefix.add(addItem);
					}
					piStepCompressed(newCurrentPivotItems, pos+1, toState, level+1);
					if(useTransitionRepresentation) {
						prefix.removeInt(prefix.size() - 1);
						prefix.removeInt(prefix.size() - 1);
					}

				}
			} else { // SELF_GENERALIZE
				addItem = itemFid; // retrieve the input item
				// retrieve ascendants of the input item
				ascendants.clear();
				ascendants.add(addItem);
				tr.addAscendantFids(addItem, ascendants);
				// we only consider this transition if the set of output elements contains at least one frequent item
				if(largestFrequentFid >= ascendants.firstInt()) { // the first item of the ascendants is the most frequent one

					//CloneableIntHeapPriorityQueue newCurrentPivotItems;
					if (currentPivotItems == null) { // if we are starting a new pivot set there is no need for a union
						// we are reusing the heap object, so reset the heap size to 0
						newCurrentPivotItems.clear();
						// headSet(largestFrequentFid + 1) drops all infrequent items
						for(int ascendant : ascendants.headSet(largestFrequentFid + 1)) {
							newCurrentPivotItems.enqueue(ascendant);
						}
					} else {
						// first half of the update union: current[>=min(add)].
						newCurrentPivotItems.startFromExisting(currentPivotItems);
						while(newCurrentPivotItems.size() > 0 && newCurrentPivotItems.firstInt() < ascendants.firstInt()) {
							newCurrentPivotItems.dequeueInt();
						}
						// second half of the update union: add[>=min(curent)]
						// we filter out infrequent items, so in fact we do:  add[<=largestFrequentFid][>=min(current)]
						for(int add : ascendants.headSet(largestFrequentFid + 1).tailSet(currentPivotItems.firstInt())) {
							newCurrentPivotItems.enqueue(add);
						}
					}
					// we put the current transition together with the input item onto the prefix and take it off when we come back from recursion
					if(useTransitionRepresentation) {
						prefix.add(tr.getTransitionNumber());
						prefix.add(addItem);
					}
					piStepCompressed(newCurrentPivotItems, pos+1, toState, level+1);
					if(useTransitionRepresentation) {
						prefix.removeInt(prefix.size() - 1);
						prefix.removeInt(prefix.size() - 1);
					}
				}
			}
		}
	}
	/** Runs one step in the FST in order to produce the set of frequent output items
	 *  returns: - 0 if no accepting path was found in the recursion branch
	 *           - the maximum pivot item found in the branch otherwise
	 *
	 * @param pos   current position
	 * @param state current state
	 * @param level current level
	 */

	private int piStep(int pivot, int pos, State state, int level) {
		counterTotalRecursions++;
		// if we reached a final state, we count the current sequence (if any)
		if(state.isFinal() && pivot != 0) {
			// the current pivot is the pivot of the current output sequence and therefore one pivot element
			// for the input sequence we are scanning right now. so we add it to the set of pivot items for this input sequence.
			pivotItems.add(pivot);
		}

		// check if we already read the entire input
		if (state.isFinalComplete() || pos == inputSequence.size()) {
			return pivot;
		}

		// get iterator over next output item/state pairs; reuse existing ones if possible
		final int itemFid = inputSequence.getInt(pos); // the current input item

		// in two pass, we only go to states we saw in the first pass
		final BitSet validToStates = useTwoPass
				? dfaStateSequence.get(inputSequence.size()-(pos+1)).getFstStates() // only states from first pass
				: null; // all states

		Iterator<ItemState> itemStateIt;
		if(level >= itemStateIterators.size()) {
			itemStateIt = state.consume(itemFid, null, validToStates);
			itemStateIterators.add(itemStateIt);
		} else {
			itemStateIt = state.consume(itemFid, itemStateIterators.get(level), validToStates);
		}

		// get an empty set to maintain the states to-states that we have already visited with non-pivot transitions
		IntSet visitedToStates = null;
		if(skipNonPivotTransitions) {
			if (level >= nonPivotExpandedToStates.size()) {
				visitedToStates = new IntAVLTreeSet();
				nonPivotExpandedToStates.add(visitedToStates);
			} else {
				visitedToStates = nonPivotExpandedToStates.get(level);
				visitedToStates.clear();
			}
		}

		int maxFoundPivot = 0; // maxFoundPivot = 0 means no accepting state and therefore, no pivot was found
		int returnPivot;

		// iterate over output item/state pairs
		while(itemStateIt.hasNext()) {
			final ItemState itemState = itemStateIt.next();
			final int outputItemFid = itemState.itemFid;
			final State toState = itemState.state;

			if(outputItemFid == 0) { // EPS output
				// we did not get an output, so continue with the current prefix
				returnPivot = piStep(pivot, pos + 1, toState, level+1);
				if(useMaxPivot && returnPivot > maxFoundPivot ) maxFoundPivot  = returnPivot;
			} else {
				// we got an output; check whether it is relevant
				if (largestFrequentFid >= outputItemFid) {
					// the seen output is frequent, so we contiune running the FST
					if(outputItemFid > pivot) { // we have a new pivot item
						// if (1) we have already recursed + have found an accepting state in recursion (maxFoundPivot != 0)
						// and (2) the newly found pivot item (outputItemFid) is larger or equal than this pivot
						// then we don't need to recurse for this output item
						// and we add the output item to the set of pivot items if we haven't added it before (e.g. item>maxFoundPivot)
						// after that, we continue on to the next (outputItem, toState) pair
						if(useMaxPivot && maxFoundPivot != 0 && outputItemFid>=maxFoundPivot) {
							if(outputItemFid > maxFoundPivot) pivotItems.add(outputItemFid);
							counterMaxPivotUsed++;
							continue;
						}
						returnPivot = piStep(outputItemFid, pos + 1, toState, level + 1 );
						if(useMaxPivot && returnPivot > maxFoundPivot ) maxFoundPivot  = returnPivot;
					} else { // keep the old pivot
						if(!skipNonPivotTransitions || !visitedToStates.contains(toState.getId())) { // we go to each toState only once with non-pivot transitions
							returnPivot = piStep(pivot, pos + 1, toState, level + 1 );
							if(useMaxPivot && returnPivot > maxFoundPivot ) maxFoundPivot  = returnPivot;
							if(skipNonPivotTransitions) {
								visitedToStates.add(toState.getId());
							}
						} else {
							counterNonPivotTransitionsSkipped++;
						}
					}
				}
			}
		}
		return maxFoundPivot; // returns 0 if no accepting path was found
	}

	private void piStepOnePassV1(int pos, State state, int level) {
		counterTotalRecursions++;
		// if we reached a final state, we count the current sequence (if any)
		if(state.isFinal() && prefix.size()>0) {
			// the current pivot is the pivot of the current output sequence and therefore one pivot element
			// for the input sequence we are scanning right now. so we add it to the set of pivot items for this input sequence.
			pivotItems.add(pivot(prefix));
		}

		// check if we already read the entire input
		if (state.isFinalComplete() || pos == inputSequence.size()) {
			return;
		}

		// get iterator over next output item/state pairs; reuse existing ones if possible
		final int itemFid = inputSequence.getInt(pos); // the current input item

		// in two pass, we only go to states we saw in the first pass
		final BitSet validToStates = useTwoPass
				? dfaStateSequence.get(inputSequence.size()-(pos+1)).getFstStates() // only states from first pass
				: null; // all states

		Iterator<ItemState> itemStateIt;
		if(level >= itemStateIterators.size()) {
			itemStateIt = state.consume(itemFid, null, validToStates);
			itemStateIterators.add(itemStateIt);
		} else {
			itemStateIt = state.consume(itemFid, itemStateIterators.get(level), validToStates);
		}



		// iterate over output item/state pairs
		while(itemStateIt.hasNext()) {
			final ItemState itemState = itemStateIt.next();
			final int outputItemFid = itemState.itemFid;
			final State toState = itemState.state;

			if(outputItemFid == 0) { // EPS output
				// we did not get an output, so continue with the current prefix
				piStepOnePassV1(pos + 1, toState, level+1);
			} else {
				// we got an output; check whether it is relevant
				if (largestFrequentFid >= outputItemFid) {
					prefix.add(outputItemFid);
					piStepOnePassV1(pos + 1, toState, level + 1 );
					prefix.removeInt(prefix.size()-1);
				}
			}
		}
	}


	// -- mining ------------------------------------------------------------------------------------------------------


	/**
	 * Mine with respect to a specific pivot item. e.g, filter out all non-pivot sequences
	 *   (e.g. filter out all sequences where pivotItem is not the max item)
	 * @param pivotItem
	 */
	public void minePivot(int pivotItem) {
		this.pivotItem = pivotItem;


		// run the normal mine method. Filtering is done in expand() when the patterns are output
		if(useTransitionRepresentation)
			mine();
		else
			mineTraditional();

		// reset the pivot item, just to be sure. pivotItem=0 means no filter
		this.pivotItem = 0;
	}

	@Override
	public void mine() {
		if (sumInputSupports >= sigma) {
			DesqDfsTreeNode root = new DesqDfsTreeNode(useTreeRepresentation ? 1 : fst.numStates()); // if we use NFAs, we need only one BitSet per node
			final IncStepArgs incStepArgs = new IncStepArgs();
			incStepArgs.node = root;
			// and process all input sequences to compute the roots children
			for (int inputId = 0; inputId < inputSequences.size(); inputId++) {
				incStepArgs.inputId = inputId;
				incStepArgs.inputSequence = inputSequences.get(inputId);
				incStep(incStepArgs, 0);
			}
			// the root has already been processed; now recursively grow the patterns
			root.pruneInfrequentChildren(sigma);
			expand(new IntArrayList(), root);
		}
	}

	/** Expands all children of the given search tree node. The node itself must have been processed/output/expanded
	 * already.
	 *
	 * @param prefix (partial) output sequence corresponding to the given node (must remain unmodified upon return)
	 * @param node the node whose children to expand
	 */
	private void expand(IntList prefix, DesqDfsTreeNode node) {
		// this bundles common arguments to incStepOnePass or incStepTwoPass
		final IncStepArgs incStepArgs = new IncStepArgs();
		// add a placeholder to prefix for the output item of the child being expanded
		final int lastPrefixIndex = prefix.size();
		prefix.add(-1);
		// iterate over all children
		for (final DesqDfsTreeNode childNode : node.childrenByFid.values() )  {
			// while we expand the child node, we also compute its actual support to determine whether or not
			// to output it (and then output it if the support is large enough)
			long support = 0;
			// check whether pivot expansion worked
			// the idea is that we never expand a child>pivotItem at this point
			if(pivotItem != 0) {
				assert childNode.itemFid <= pivotItem;
			}
			// set up the expansion
			assert childNode.prefixSupport >= sigma;
			prefix.set(lastPrefixIndex, childNode.itemFid);
			projectedDatabaseIt.reset(childNode.projectedDatabase);
			incStepArgs.inputId = -1;
			incStepArgs.node = childNode;

            do {
                // process next input sequence
                incStepArgs.inputId += projectedDatabaseIt.nextNonNegativeInt();
                incStepArgs.inputSequence = inputSequences.get(incStepArgs.inputId);

//				if (useTwoPass) {
//					current.dfaStateSequence = dfaStateSequences.get(current.inputId);
//				}

                // iterate over state@pos snapshots for this input sequence
                boolean reachedFinalStateWithoutOutput = false;
                do {
                    final int pos = projectedDatabaseIt.nextNonNegativeInt(); // position of next input item
                    reachedFinalStateWithoutOutput |= incStep(incStepArgs, pos);
                } while (projectedDatabaseIt.hasNext());

                // if we reached a final state without output, increment the support of this child node
                if (reachedFinalStateWithoutOutput) {
                    support += incStepArgs.inputSequence.weight;
                }

                // now go to next posting (next input sequence)
            } while (projectedDatabaseIt.nextPosting());

			// output the patterns for the current child node if it turns out to be frequent
			if (support >= sigma) {
				// if we are mining a specific pivot partition (meaning, pivotItem>0), then we output only sequences with that pivot
				if (ctx.patternWriter != null && (pivotItem==0 || pivot(prefix) == pivotItem)) { // TODO: investigate what is pruned here (whether we can improve)
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

	/**
	 * Convenience function to switch between concat transition representation and trees
	 * @param args
	 * @param pos
	 * @return
	 */
	private boolean incStep(final IncStepArgs args, final int pos) {
		if(useTreeRepresentation)
			return incStepOnePassTree(args, pos);
		else
			return incStepOnePassConcat(args, pos);
	}



	/** Updates the projected databases of the children of the current node (args.node) corresponding to each possible
	 * next output item for the current input sequence (also stored in args). Used only in the one-pass algorithm.
	 *
	 * @param args information about the input sequence and the current search tree node
	 * @param pos next item to read
	 *
	 * @return true if a final FST state can be reached without producing further output
	 */
	private boolean incStepOnePassTree(final IncStepArgs args, final int pos) {
		// in transition representation, we store pairs of integers: (transition id, input element)
		int trNo;
		int inputItem;
		int followPos;
		IntList outputItems;
		int readPos = pos;
		int nextInt = args.inputSequence.getInt(readPos);
		while(nextInt != Integer.MIN_VALUE && nextInt != Integer.MAX_VALUE) {

			if(nextInt < 0) { // CONSTANT or SELF transition, nextInt is -inputItem
				inputItem = -nextInt;
				assert inputItem <= pivotItem; // otherwise, this path should not have been sent to the partition
				followPos = args.inputSequence.getInt(readPos+ 1); // next int is the offset of the next state
				args.node.expandWithTransition(inputItem, args.inputId, args.inputSequence.weight, followPos); // expand with this output item
				readPos = readPos+2; // we read 2 items from the list
			} else { // SELF_ASCENDANTS transition, so we stored the inputItem (in nextInt) and the number of the transition
                inputItem = nextInt;
                trNo = args.inputSequence.getInt(readPos+1);
                followPos = args.inputSequence.getInt(readPos+2);
				outputItems = fst.getBasicTransitionByNumber(trNo).getOutputElements(inputItem);

				for (int outputItem : outputItems) {
					if (pivotItem >= outputItem) { // no check for largestFrequentFid necessary, as largestFrequentFid >= pivotItem
						args.node.expandWithTransition(outputItem, args.inputId, args.inputSequence.weight, followPos);
					}
				}
				readPos = readPos + 3; // we read 3 elements
			}

			nextInt = args.inputSequence.getInt(readPos);
		}

		return nextInt == Integer.MIN_VALUE; // is true if this state is final
	}


	/** Updates the projected databases of the children of the current node (args.node) corresponding to each possible
	 * next output item for the current input sequence (also stored in args). Used only in the one-pass algorithm.
	 *
	 * @param args information about the input sequence and the current search tree node
	 * @param pos next item to read
	 *
	 * @return true if a final FST state can be reached without producing further output
	 */
	private boolean incStepOnePassConcat(final IncStepArgs args, final int pos) {
		// check whether we are at the end of the path we are following. There are two options.
		// Option 1: we are through the last path, meaning, we are through the input
		if (pos >= args.inputSequence.size()) {
			return true;
		}
		// Option 2: We finished one of the other pathes (not the last one in the input)
		for(int i=0; i<args.inputSequence.getInt(0); i++) {
			if(pos == args.inputSequence.getInt(i+1)) { // if pos is at the start of the next path, we are done with the current one
				// when we first call pos for a specific path, pos is already at pos+2
				return true;
			}
		}
		// iterate over output item/state pairs and remember whether we hit a final state without producing output
		// (i.e., no transitions or only transitions with epsilon output)
		boolean reachedFinalStateWithoutOutput = false; // state.isFinal() && !fst.getRequireFullMatch();
		// in transition representation, we store pairs of integers: (transition id, input element)
		int trNo ;
		int inputItem;
		BasicTransition tr;
		OutputLabelType ol;
		// if we are at position 0, we have to follow each of the pathes in the input
		int pathsToFollow = 1;
		int localPos;
		if(pos == 0) {
			pathsToFollow=args.inputSequence.getInt(pos);
		}
		for(int pathNo=0; pathNo<pathsToFollow; pathNo++) {
			if(pos == 0) {
				localPos = args.inputSequence.getInt(1+pathNo);
			} else {
				localPos = pos;
			}
			// get (transition_id, input_item_fid) pair
			trNo = args.inputSequence.getInt(localPos);
			inputItem = args.inputSequence.getInt(localPos+1);
			tr = (BasicTransition) fst.getTransitionByNumber(trNo);
			ol = tr.getOutputLabelType();
			if (ol == OutputLabelType.EPSILON) {
				reachedFinalStateWithoutOutput |= incStep(args, localPos + 2);
			} else {
				// we have new outputs, so we run through them and update the corresponding projected databases if the item is frequent
				IntList outputItems = tr.getOutputElements(inputItem);
				for (int outputItem : outputItems) {
					if (pivotItem >= outputItem) { // no check for largestFrequentFid necessary, as largestFrequentFid >= pivotItem
						args.node.expandWithTransition(outputItem, args.inputId, args.inputSequence.weight,
								localPos + 2);
					}
				}
			}
		}
		return reachedFinalStateWithoutOutput;
	}


	public void mineTraditional() {
		if (sumInputSupports >= sigma) {
			// the root has already been processed; now recursively grow the patterns
			root.pruneInfrequentChildren(sigma);
			expandTraditional(new IntArrayList(), root);
		}
	}

	/** Expands all children of the given search tree node. The node itself must have been processed/output/expanded
	 * already.
	 *
	 * @param prefix (partial) output sequence corresponding to the given node (must remain unmodified upon return)
	 * @param node the node whose children to expand
	 */

	private void expandTraditional(IntList prefix, DesqDfsTreeNode node) {
		// add a placeholder to prefix for the output item of the child being expanded
		final int lastPrefixIndex = prefix.size();
		prefix.add(-1);

		// iterate over all children
		for (final DesqDfsTreeNode childNode : node.childrenByFid.values() )  {
			// while we expand the child node, we also compute its actual support to determine whether or not
			// to output it (and then output it if the support is large enough)
			long support = 0;
			// check whether pivot expansion worked
			// the idea is that we never expand a child>pivotItem at this point
			if(pivotItem != 0) {
				assert childNode.itemFid <= pivotItem;
			}

			// set up the expansion
			assert childNode.prefixSupport >= sigma;
			prefix.set(lastPrefixIndex, childNode.itemFid);
			projectedDatabaseIt.reset(childNode.projectedDatabase);
			current.inputId = -1;
			current.node = childNode;

			do {
				// process next input sequence
				current.inputId += projectedDatabaseIt.nextNonNegativeInt();
				current.inputSequence = inputSequences.get(current.inputId);
				if (useTwoPass) {
					current.dfaStateSequence = dfaStateSequences.get(current.inputId);
				}

				// iterate over state@pos snapshots for this input sequence
                boolean reachedFinalStateWithoutOutput = false;
				do {
					final int stateId = projectedDatabaseIt.nextNonNegativeInt();
					final int pos = projectedDatabaseIt.nextNonNegativeInt(); // position of next input item
					reachedFinalStateWithoutOutput |= incStepTraditional(pos, fst.getState(stateId), 0);
				} while (projectedDatabaseIt.hasNext());

                // if we reached a final state without output, increment the support of this child node
				if (reachedFinalStateWithoutOutput) {
					support += current.inputSequence.weight;
				}

				// now go to next posting (next input sequence)
			} while (projectedDatabaseIt.nextPosting());

			// output the patterns for the current child node if it turns out to be frequent
			if (support >= sigma) {
				// if we are mining a specific pivot partition (meaning, pivotItem>0), then we output only sequences with that pivot
				if (ctx.patternWriter != null && (pivotItem==0 || pivot(prefix) == pivotItem)) {
					if (!useTwoPass) { // one-pass
						ctx.patternWriter.write(prefix, support);
					} else { // two-pass
						// for the two-pass algorithm, we need to reverse the output because the search tree is built
						// in reverse order
						ctx.patternWriter.write(prefix, support);
					}
				}
			}

			// expand the child node
			childNode.pruneInfrequentChildren(sigma);
			childNode.projectedDatabase = null; // not needed anymore
			expandTraditional(prefix, childNode);
			childNode.invalidate(); // not needed anymore
		}

		// we are done processing the node, so remove its item from the prefix
		prefix.removeInt(lastPrefixIndex);
	}

	/** Updates the projected databases of the children of the current node (<code>current.node</code>) corresponding to each possible
	 * next output item for the current input sequence (also stored in <code>currect</code>).
	 *
	 * @param pos next item to read
	 * @param state current FST state
	 * @param level recursion level (used for reusing iterators without conflict)
	 *
	 * @return true if the initial FST state can be reached without producing further output
	 */
	private boolean incStepTraditional(final int pos, final State state, final int level) {
		// check if we reached a final complete state or consumed entire input and reached a final state
		if ( state.isFinalComplete() || pos == current.inputSequence.size() )
			return state.isFinal();

		// get iterator over next output item/state pairs; reuse existing ones if possible
		// in two-pass, only iterates over states that we saw in the first pass (the other ones can safely be skipped)
		final int itemFid = current.inputSequence.getInt(pos);
		final BitSet validToStates = useTwoPass
				? current.dfaStateSequence[ current.inputSequence.size()-(pos+1) ].getFstStates() // only states from first pass
				: null; // all states
		Iterator<ItemState> itemStateIt;
		if (level>=itemStateIterators.size()) {
			itemStateIt = state.consume(itemFid, null, validToStates);
			itemStateIterators.add(itemStateIt);
		} else {
			itemStateIt = state.consume(itemFid, itemStateIterators.get(level), validToStates);
		}

		// iterate over output item/state pairs and remember whether we hit the final or finalComplete state without producing output
		// (i.e., no transitions or only transitions with epsilon output)
		boolean reachedInitialStateWithoutOutput = false;
		while (itemStateIt.hasNext()) {
			final ItemState itemState = itemStateIt.next();
			final int outputItemFid = itemState.itemFid;
			final State toState = itemState.state;

			if (outputItemFid == 0) { // EPS output
				// we did not get an output, so continue running the FST
				int newLevel = level + (itemStateIt.hasNext() ? 1 : 0); // no need to create new iterator if we are done on this level
				reachedInitialStateWithoutOutput |= incStepTraditional(pos + 1, toState, newLevel);
			} else if (largestFrequentFid >= outputItemFid && (pivotItem == 0 || pivotItem >= outputItemFid)) {
				// we have an output and its frequent, so update the corresponding projected database
				current.node.expandWithItemTraditional(outputItemFid, current.inputId, current.inputSequence.weight,
						pos+1, toState.getId());

			}
		}

		return reachedInitialStateWithoutOutput;
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


	/** Bundles arguments for {@link #incStep}. These arguments are not modified during the method's recursions, so
	 * we keep them at a single place. */
	private static class IncStepArgs {
		int inputId;
		WeightedSequence inputSequence;
		DfaState[] dfaStateSequence;
		DesqDfsTreeNode node;
	}


	/**
	 * A NFA that produces output sequences.
	 *
	 * Per input sequence, we send one such NFA to each partition, at which the input sequence generates at least one
	 * output sequence.
	 */
	private class OutputNFA {
		PathState root;
		ObjectList<PathState> leafs = new ObjectArrayList<PathState>();
		int numPathStates = 0;
		int pivot;
		private ObjectList<PathState> pathStates = new ObjectArrayList<PathState>(); // TODO: only for printing. can be removed
		private PathState[] pathStatesByNumber;
		protected int numPaths = 0; // only for stat purposes, can be removed

		public OutputNFA(int pivot) {
			root = new PathState(this);
			this.pivot = pivot;
		}

		/**
		 * Add a path to this NFA
		 *
		 * @param path
		 */
		protected void addPath(IntList path) {
			numPaths++;
			int trId = 0;
			int inpItem = 0;
			PathState currentState;
			BasicTransition tr;
			// Run through the transitions of this path and add them to this NFA
			currentState = root;
			for(int i=0; i<path.size(); ) {
				trId = path.getInt(i);
				inpItem = path.getInt(i+1);

				if(generalizeInputItemsBeforeSending) {
					tr = fst.getBasicTransitionByNumber(trId);
					if (tr.getOutputLabelType() == OutputLabelType.SELF_ASCENDANTS) {
						inpItem = tr.generalizeItemForPivot(inpItem, this.pivot);
					}
				}
				currentState = currentState.followTransition(trId, inpItem, this);
				i = i+2;
			}
			// we ran through the path, so the current state is a final state.
			currentState.setFinal();
		}


		/**
		 * Merge the suffixes of this NFA
		 */
		protected void mergeSuffixes() {
			// we merge all leafs into the first leaf
			PathState finalLeaf = null;
			for(PathState leaf : this.leafs) {
				if(finalLeaf == null) {
					finalLeaf = leaf;
				} else {
					boolean merged = finalLeaf.attemptMerge(leaf);
					assert merged;
				}
			}
		}


		/**
		 * Setup numbered array to access states
		 */
		private void setupNumberedPathStates() {
			pathStatesByNumber = new PathState[numPathStates];
			for(PathState s : pathStates) {
				pathStatesByNumber[s.id] = s;
			}
		}

		public PathState getPathStateByNumber(int num) {
		    if(pathStatesByNumber == null) {
		    	System.out.println("PathState access by number wasn't set up yet");
		    	System.exit(1);
			}
			return pathStatesByNumber[num];
		}

		/**
		 * Serialize this NFA to a given IntList
		 * @param send
		 */
		public void write(IntList send) {
			// set up a way to reliably get states by number
			setupNumberedPathStates();
			// keep track of where we wrote state IDs
			BitSet stateIdAtPos = new BitSet();
			int inpItem;
			int trId;
			OutputLabelType olt;

			// for each each state, write all outgoing transitions
			for(PathState state : pathStates) {
				state.writtenAtPos = send.size();
				// write all outgoing transitions


				for(Map.Entry<Long,PathState> entry : state.outTransitions.entrySet()) {
					trId = PrimitiveUtils.getLeft(entry.getKey());
					inpItem = PrimitiveUtils.getRight(entry.getKey());
					olt = fst.getBasicTransitionByNumber(trId).getOutputLabelType();

					if(olt == OutputLabelType.CONSTANT) {
						send.add(-inpItem);
					} else if(olt == OutputLabelType.SELF) {
						send.add(-inpItem);
					} else {
						send.add(inpItem);
						send.add(trId);
					}

					// write to-state
					stateIdAtPos.set(send.size());
					send.add(entry.getValue().id);
				}

				// write state end symbol
				if(state.isFinal)
					send.add(Integer.MIN_VALUE);
				else
					send.add(Integer.MAX_VALUE);
			}

			System.out.println(stateIdAtPos);
			for(int i=stateIdAtPos.nextSetBit(0); i!=-1; i=stateIdAtPos.nextSetBit(i+1)) {
					send.set(i, getPathStateByNumber(send.getInt(i)).writtenAtPos);
			}

		}

		/**
		 * Export the set of path states currently stored in pathStates with their transitions to PDF
		 * @param file
		 * @param paintIncoming
		 */
		public void exportGraphViz(String file, boolean paintIncoming) {
			FstVisualizer fstVisualizer = new FstVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
			fstVisualizer.beginGraph();
			for(PathState s : pathStates) {
				for(Map.Entry<Long, PathState> trEntry : s.outTransitions.entrySet()) {
					int trId = PrimitiveUtils.getLeft(trEntry.getKey());
					int inputId = PrimitiveUtils.getRight(trEntry.getKey());

					fstVisualizer.add(String.valueOf(s.id), "i" + inputId + "@T" + trId+(fst.getBasicTransitionByNumber(trId).getOutputLabelType() == OutputLabelType.SELF_ASCENDANTS ? "^" : ""), String.valueOf(trEntry.getValue().id));
				}
				if(paintIncoming && s.inTransitions!=null) {
					for(Map.Entry<Long, ObjectList<PathState>> trEntry : s.inTransitions.entrySet()) {
						int trId = PrimitiveUtils.getLeft(trEntry.getKey());
						int inputId = PrimitiveUtils.getRight(trEntry.getKey());

						for(PathState s2 : trEntry.getValue())
							fstVisualizer.add(String.valueOf(s.id), (-trId)+"{"+inputId+"}", String.valueOf(s2.id));
					}
				}
				if(s.isFinal)
					fstVisualizer.addFinalState(String.valueOf(s.id));
			}
			fstVisualizer.endGraph();
		}
	}


	/**
	 * One state in a path through the Fst. We use this to build the NFAs we shuffle to the partitions
	 *
	 * If we want to merge suffixes (mergeSuffixes true), in addition to the forward pointers (outTransitions),
	 * we maintain backward pointers (inTransitions)
	 */
	private class PathState {
		protected final int id;
		protected boolean isFinal = false;
		protected int writtenAtPos = -1;
		protected OutputNFA nfa;

		/** Forward pointers */
		protected Object2ObjectOpenHashMap<Long, PathState> outTransitions;

		/** Backward pointers (for merging suffixes) */
		protected Object2ObjectOpenHashMap<Long, ObjectList<PathState>> inTransitions;

		protected PathState(OutputNFA nfa) {
			this.nfa = nfa;
			id = nfa.numPathStates++;
			nfa.pathStates.add(this);
			outTransitions = new Object2ObjectOpenHashMap<>();
			if(mergeSuffixes)
				inTransitions = new Object2ObjectOpenHashMap<>();
		}

		/** Construct new state with backwards pointers */
		protected PathState(long trInp, PathState from, OutputNFA nfa) {
			this(nfa);
			ObjectList<PathState> fromState = new ObjectArrayList<>();
			fromState.add(from);
			inTransitions.put(trInp, fromState);
			// initially, every state is a leaf state
			nfa.leafs.add(this);
		}

		/**
		 * Starting from this state, follow a given transition with given input item. Returns the state the transition
		 * leads to.
		 *
		 * @param trId		the number of the input transition to follow
		 * @param inpItem	the input item for the transition to follow
		 * @param nfa       the nfa the path is added to
		 * @return
		 */
		protected PathState followTransition(int trId, int inpItem, OutputNFA nfa) {
			long trInp = PrimitiveUtils.combine(trId,inpItem);
			// if we have a tree branch with this transition already, follow it and return the state it leads to
			if(outTransitions.containsKey(trInp)) {
				// return this state
				PathState toState = outTransitions.get(trInp);
				return toState;
			} else {
				// if we don't have a path starting with that transition, we create a new one
				//   (unless it is the last transition, in which case we want to transition to the global end state)
				PathState toState;
				// if we merge suffixes, create a state with backwards pointers, otherwise one without
				if(mergeSuffixes)
					toState = new PathState(trInp, this, nfa);
				else
					toState = new PathState(nfa);

				// add the transition and the created state to the outgoing transitions of this state and return the state the transition moves to
				outTransitions.put(trInp, toState);
				// if this was a leaf state before adding this outTransition, we need to remove it from the list of leafs
				if(outTransitions.size() == 1) {
					nfa.leafs.remove(this);
				}
				return toState;
			}
		}


		/**
		 * Attempts to merge PathState 'drop' into the called state.
		 * Merges parent states recursively
		 *
		 * If it is possible to join the two states, updates forward and backward pointers of affected states
		 *
		 * @param drop	The state that is supposed to be merged into the current one
		 * @return
		 */
		private boolean attemptMerge(PathState drop) {
			// we want to merge the passed State "drop" into this state if possible.

			// if it is the same state, there is no need for action
			if(this == drop) {
				return true;
			}

			// we only merge final states in to final states and non-final ones into non-final ones
			// also, we can only merge if the two states have the same outgoing transitions. otherwise, we would
			// 	produce incorrect prefixes
			if(isFinal == drop.isFinal && outTransitions.equals(drop.outTransitions))  {
				// we merge the incoming transitions of the two merge states: each incoming transition can have multiple
				// from states. within these sets, we see whether we can join predecessor states. Imagine the in transitions
				// we aim to join like this:

				// mergeTarget                              drop
				// tr1/inp1: from1, from2					tr1/inp1: from3
				// tr1/inp2: from4
				//											tr1/inp3: from4
				// tr2/inp3: from5							tr2/inp3: from6, from7

				// so potentially, we could join tr1/inp1/from3 and tr2/inp3/from5 -- if the from states are mergeable
				//   into one of the other from states for that TR/INP

				for(Map.Entry<Long, ObjectList<PathState>> inTransitionEntry : drop.inTransitions.entrySet()) {
					long incomingTrInp = inTransitionEntry.getKey();
					ObjectList<PathState> incomingStatesForThisTr = inTransitionEntry.getValue();

					// we might be able to merge some of the parent states
					if(this.inTransitions.containsKey(incomingTrInp)) {
						for(PathState mergeInState : incomingStatesForThisTr) {
							// update the forward pointer of the parent state
							mergeInState.outTransitions.put(incomingTrInp, this);

							// check all predecessor states for this tr/inp in the mergeTarget state as merge candidates
							boolean mergedThisState = false;
							for(PathState potentialMergeTargetState : inTransitions.get(incomingTrInp)) {
								mergedThisState = potentialMergeTargetState.attemptMerge(mergeInState);
								if(mergedThisState) {
									break;
								}
							}
							// if no merge was possible, we add this state to the incoming states for this tr/inp
							if(!mergedThisState) {
								inTransitions.get(incomingTrInp).add(mergeInState);
							}
						}
					} else {
						// if there is no transition like this pointing to the keep state yet, we add it
						inTransitions.put(incomingTrInp, incomingStatesForThisTr);
						// Update the forward pointers
						for(PathState s : incomingStatesForThisTr) {
							s.outTransitions.put(incomingTrInp, this);
						}
					}
				}

				// after merging, we need to remove the the drop state from the incomingTr lists of the outgoing states
				for(Map.Entry<Long, PathState> targetStateEntry : drop.outTransitions.entrySet()) {
					targetStateEntry.getValue().inTransitions.get(targetStateEntry.getKey()).remove(drop);
				}

				// also we can remove the state from the list of states
				nfa.pathStates.remove(drop);

				return true;
			} else {
				return false;
			}
		}

		/** Mark this state as final */
		public void setFinal() {
			this.isFinal = true;
		}

		/**
		 * Case 1 (the state was not before reached with given TR/INP): Add the given fromState to the list of
		 * 			from states for the given TR/INP, return null
		 * Case 2 (the state was reached with the given TR/INP before): Don't add given TR/INP, return the list of
		 * 			fromStates this states was reached from with given TR/INP
		 *
		 * @param trInp		TR/INP to check
		 * @param fromState state the TR/INP is coming from
		 * @return
		 */
		public ObjectList<PathState> addIncomingTransitionOrGetExistingOnes(long trInp, PathState fromState) {
			if(inTransitions.containsKey(trInp)) {
				// TR/INP exists, return list of from states
				return inTransitions.get(trInp);
			} else {
				// TR/INP doesn't exist yet, create list, add given from state, return null
				ObjectList<PathState> incomingStates = new ObjectArrayList<>();
				incomingStates.add(fromState);
				inTransitions.put(trInp, incomingStates);
				return null;
			}
		}

		/**
		 * Serialize this state, appending to the given send list.
		 * Run serialization of successor states recursively
		 *
		 * @param send
		 */
		@Deprecated
		protected void write(IntList send) {
			int numOutgoing = outTransitions.size();
			int startOffset = send.size();
			int inpItem;
			int trId;
			// note down that and where we have written this state
			this.writtenAtPos = send.size();

			// write number of outgoing transitions
			send.add(isFinal ? -numOutgoing : numOutgoing); // if the current state is a final state, we store a negative integer

			// for each outgoing transition, write one placeholder integer for the offset to the beginning of the outgoing path
			for(int i=0; i<numOutgoing; i++) {
				send.add(0);
			}

			// follow each outgoing transition
			int outPathNo = 0;
			for(Map.Entry<Long,PathState> entry : outTransitions.entrySet()) {
				// fill in the placeholder, then write transition information: transition number and the input item
				send.set(startOffset+1+outPathNo, send.size());
				trId = PrimitiveUtils.getLeft(entry.getKey());
				send.add(trId);
				inpItem = PrimitiveUtils.getRight(entry.getKey());
				if(fst.getBasicTransitionByNumber(trId).getOutputLabelType() != OutputLabelType.CONSTANT)
					send.add(inpItem); // don't add the input item for constant transitions
				if(mergeSuffixes) {
					// if we merge suffixes, multiple transitions can point to the same state, so we need a pointer for that
					// if the state was already written, we know it's position and can write it down
					if(entry.getValue().writtenAtPos != -1) {
						send.add(entry.getValue().writtenAtPos);
					} else {
						// otherwise, we process the to state now, so we also know it's position and can write it
						send.add(send.size()+1);
						entry.getValue().write(send);
					}
				} else {
					// if we don't merge suffixes, we simply run the serialization of the next state
					entry.getValue().write(send);
				}
				outPathNo++;
			}
		}
	}


}