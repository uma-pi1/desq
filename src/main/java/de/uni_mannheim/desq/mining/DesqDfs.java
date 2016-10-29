package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.fst.*;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.patex.PatEx;
import de.uni_mannheim.desq.util.DesqProperties;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import scala.Tuple2;

import org.apache.log4j.Logger;

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
	final Fst fst;

    /** Stores the reverse final state transducer for DesqDfs (two-pass) */
    final Fst reverseFst;

    /** Stores the largest fid of an item with frequency at least sigma. Zsed to quickly determine
     * whether an item is frequent (if fid <= largestFrequentFid, the item is frequent */
    final int largestFrequentFid;

    /** Stores iterators over output item/next state pairs for reuse. */
	final ArrayList<Iterator<ItemState>> itemStateIterators = new ArrayList<>();

    /** An iterator over a projected database (a posting list) for reuse */
    final PostingList.Iterator projectedDatabaseIt = new PostingList.Iterator();


	// -- helper variables for pruning and twopass --------------------------------------------------------------------

	/** The DFA corresponding to the pattern expression. Accepts if the FST can reach a final state on an input */
    final ExtendedDfa edfa;

	// helper variables for twopass
    /** For each relevant input sequence, the sequence of states taken by edfa */
	final ArrayList<ExtendedDfaState[]> edfaStateSequences;

    /** For each relevant input sequence, the set of positions at which the FST can reach a final state (before
     * reading the item at that position) */
    final ArrayList<int[]> edfaFinalStatePositions; // per relevant input sequence

    /** A sequence of EDFA states for reuse */
    final ArrayList<ExtendedDfaState> edfaStateSequence;

    /** A sequence of positions for reuse */
    final IntList finalPos;


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

	/** For each (state, pos) combination, store the max seen and min guaranteed pivot item that came up from recursion to this state */
	// int[] maxPivotPerStatePos = null;
	int numFstStates; // TODO: remove, not needed anymore

	/** Stats about pivot element search */
	public long counterTotalRecursions = 0;
	public long counterNonPivotTransitionsSkipped = 0;
	public long counterMaxPivotUsed = 0;

    // -- construction/clearing ---------------------------------------------------------------------------------------

	public DesqDfs(DesqMinerContext ctx) {
		super(ctx);
		sigma = ctx.conf.getLong("desq.mining.min.support");
		largestFrequentFid = ctx.dict.getLargestFidAboveDfreq(sigma);
		pruneIrrelevantInputs = ctx.conf.getBoolean("desq.mining.prune.irrelevant.inputs");
        useTwoPass = ctx.conf.getBoolean("desq.mining.use.two.pass");
		skipNonPivotTransitions = ctx.conf.getBoolean("desq.mining.skip.non.pivot.transitions", false);
		useMaxPivot = ctx.conf.getBoolean("desq.mining.use.minmax.pivot", false);


        // construct pattern expression and FST
		patternExpression = ctx.conf.getString("desq.mining.pattern.expression");
		PatEx p = new PatEx(patternExpression, ctx.dict);
		Fst tempFst = p.translate();
		tempFst.minimize(); //TODO: move to translate

		if (useTwoPass) { // two-pass
            // two-pass will always prune irrelevant input sequences, so notify the user when the corresponding
            // property is not set
			if (!pruneIrrelevantInputs) {
				logger.warn("property desq.mining.prune.irrelevant.inputs=false will be ignored because " +
						"desq.mining.use.two.pass=true");
			}

            // construct the DFA for the FST (for the forward pass)
			this.edfa = new ExtendedDfa(tempFst, ctx.dict);

            // and then reverse the FST (which we now only need for the backward pass)
            tempFst.reverse(false); // here we need the reverse fst
			fst = null;
			reverseFst = tempFst;

            // initialize helper variables for two-pass
			edfaStateSequences = new ArrayList<>();
			edfaFinalStatePositions = new ArrayList<>();
			edfaStateSequence = new ArrayList<>();
			finalPos = new IntArrayList();
		} else { // one-pass
			// store the FST
            fst = tempFst;
			reverseFst = null;
			numFstStates = fst.numStates();

			// if we prune irrelevant inputs, construct the DFS for the FST
            if (pruneIrrelevantInputs) {
                this.edfa = new ExtendedDfa(fst, ctx.dict);
            } else {
                this.edfa = null;
            }

            // invalidate helper variables for two-pass
            edfaStateSequences = null;
			edfaFinalStatePositions = null;
			edfaStateSequence = null;
			finalPos  = null;
		}
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
			edfaStateSequences.clear();
            edfaStateSequences.trimToSize();
			edfaFinalStatePositions.clear();
            edfaFinalStatePositions.trimToSize();
		}
	}

	// -- processing input sequences ----------------------------------------------------------------------------------

	@Override
	public void addInputSequence(IntList inputSequence, long inputSupport, boolean allowBuffering) {
        // two-pass version of DesqDfs
        if (useTwoPass) {
            // run the input sequence through the EDFA and compute the state sequences as well as the positions before
            // which a final FST state is reached
			if (edfa.isRelevant(inputSequence, 0, edfaStateSequence, finalPos)) {
			    // we now know that the sequence is relevant; remember it
				super.addInputSequence(inputSequence, inputSupport, allowBuffering);
				edfaStateSequences.add(edfaStateSequence.toArray(new ExtendedDfaState[edfaStateSequence.size()]));
				//TODO: directly do the first incStepOnePass to avoid copying and storing finalPos
				edfaFinalStatePositions.add(finalPos.toIntArray());
				finalPos.clear();
			}
			edfaStateSequence.clear();
            return;
		}

		// one-pass version of DesqDfs
		if (!pruneIrrelevantInputs || edfa.isRelevant(inputSequence, 0, 0)) {
			// if we reach this place, we either don't want to prune irrelevant inputs or the input is relevant
            // -> remember it
		    super.addInputSequence(inputSequence, inputSupport, allowBuffering);
		}
	}


	// ---------------- DesqDfs Distributed ---------------------------------------------------------


    public Tuple2<Integer,Integer> determinePivotElementsForSequences(SequenceReader in) throws IOException {
        int numSequences = 0;
        int totalPivotElements = 0;
        Sequence inputSequence = new Sequence();
        IntSet pivotElements;
        while (in.readAsFids(inputSequence)) {
            pivotElements = getPivotItemsOfOneSequence(inputSequence);
            totalPivotElements += pivotElements.size();
            numSequences++;
        }
        return new Tuple2(numSequences,totalPivotElements);
    }

	public Tuple2<Integer,Integer> determinePivotElementsForSequences(ObjectArrayList<Sequence> inputSequences) throws IOException {
		int numSequences = 0;
		int totalPivotElements = 0;
		//Sequence inputSequence = new Sequence();
		IntSet pivotElements;
		for(Sequence inputSequence: inputSequences) {
			pivotElements = getPivotItemsOfOneSequence(inputSequence);
			totalPivotElements += pivotElements.size();
			numSequences++;
		}
		return new Tuple2(numSequences,totalPivotElements);
	}


	/** Produces set of frequent output elements created by one input sequence by running the FST 
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
		// TODO: one-pass probably won't work at the moment
		// check whether sequence produces output at all. if yes, produce output items
		if(useTwoPass) {
			System.out.println("Please use onePass with DesqDfs distributed. Exiting.");
			System.exit(0);
			// Run the DFA to find to determine whether the input has outputs and in which states
			// Then walk backwards and collect the output items
			/*if (edfa.isRelevant(inputSequence, 0, edfaStateSequence, finalPos)) {
				// starting from all final states for all final positions, walk backwards
				for (final int pos : finalPos) {
					for (State fstFinalState : edfaStateSequence.get(pos).getFstFinalStates()) {
						osCountStepTwoPass(0, pos-1, fstFinalState, 0);
					}
				}
				finalPos.clear();
			}
			edfaStateSequence.clear();
            finalPos.clear();*/
			
		} else { // one pass

			if (!pruneIrrelevantInputs || edfa.isRelevant(inputSequence, 0, 0)) {
				//if(useMaxPivot && (maxPivotPerStatePos == null || inputSequence.size() * numFstStates > maxPivotPerStatePos.length)) {
				//	maxPivotPerStatePos = new int[inputSequence.size() * numFstStates * 2];
				//	counterCreateObject++;
				//}
				osCountStepOnePass(0, 0, fst.getInitialState(), 0);
			}
		}

		// reset max pivot items for next sequence
		//if(useMaxPivot) {
		//	Arrays.fill(maxPivotPerStatePos, 0);
		//}

		return pivotItems;
	}


	/** Runs one step in the FST in order to produce the set of frequent output items
     *  returns: - 0 if no accepting path was found in the recursion branch
     *           - the maximum pivot item found in the branch otherwise
	 *
	 * @param pos   current position
	 * @param state current state
	 * @param level current level
	 */

	private int osCountStepOnePass(int pivot, int pos, State state, int level) {
		counterTotalRecursions++;
		// if we reached a final state, we count the current sequence (if any)
		if(state.isFinal() && pivot != 0 && (!fst.getRequireFullMatch() || pos==inputSequence.size())) {
			// the current pivot is the pivot of the current output sequence and therefore one pivot element
			// for the input sequence we are scanning right now. so we add it to the set of pivot items for this input sequence.
			pivotItems.add(pivot);

			/*if(useMaxPivot) {
				// this current pivot is a pivot for the sequence. if it's either the max seen or the min seen pivot in this
				// recursion branch, let's keep track of it.
				if (pivot > upperMaxPivot) {
					upperMaxPivot= pivot;
				}
				//if (pivot < prevMinMaxPivot.getMin()) {
				//	prevMinMaxPivot.setMin(pivot);
				//}
			}*/
		}

		// check if we already read the entire input
		if (pos == inputSequence.size()) {
			return pivot;
		}

		// get iterator over next output item/state pairs; reuse existing ones if possible
		final int itemFid = inputSequence.getInt(pos); // the current input item
		Iterator<ItemState> itemStateIt;
		if(level >= itemStateIterators.size()) {
			itemStateIt = state.consume(itemFid);
			itemStateIterators.add(itemStateIt);
		} else {
			itemStateIt = state.consume(itemFid, itemStateIterators.get(level));
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

		// minMaxPivots: in recursion, we track the maximum and minimum pivot seen and store that information for each
		// (state, pos) pair. if we reach a (state, pos) from which we have already recursed, we check whether the current pivot
		// is larger than the max pivot from recursion or smaller than the min seen pivot from recursion. in both cases,
		// we can safely stop recursion here.

		// current (state,pos)
		//Long statePos = PrimitiveUtils.combine(state.getId(),pos);
		/*int statePos = pos*numFstStates+state.getId();
		int maxPivot = 0;
		int returnPivot;

		if(useMaxPivot) {
			maxPivot = maxPivotPerStatePos[statePos];
			if (maxPivotPerStatePos[statePos] != 0) { // max pivot is set, means that also min pivot is set
				if (pivot >= maxPivotPerStatePos[statePos] ) { // current pivot is larger than anything that will be produced in the recursion, so we don't need to recurse
					// if maxPivot(this_state, this_pos)!=0, we have reached a final state with recursion from here.
					// so we know the current pivot will be the pivot element of an output sequence.
					// so we add it to the set of pivot items
					if(pivot > maxPivotPerStatePos[statePos] ) { // if the current pivot is actually larger, output. otherwise it was already output
						pivotItems.add(pivot);
					}
					// we also note it down as the max pivot seen for the previous recursion level
					if(pivot > upperMaxPivot) {
						upperMaxPivot = pivot;
					}
					counterMaxPivotUsed++;
					return upperMaxPivot;
				}
			}
		}*/

		int maxFoundPivot = 0; // maxFoundPivot = 0 means no accepting state and therefore, no pivot was found
        int returnPivot;

		// iterate over output item/state pairs
		while(itemStateIt.hasNext()) {
			final ItemState itemState = itemStateIt.next();
			final int outputItemFid = itemState.itemFid;
			final State toState = itemState.state;

			if(outputItemFid == 0) { // EPS output
				// we did not get an output, so continue with the current prefix
				returnPivot = osCountStepOnePass(pivot, pos + 1, toState, level+1);
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
						returnPivot = osCountStepOnePass(outputItemFid, pos + 1, toState, level + 1 );
                        if(useMaxPivot && returnPivot > maxFoundPivot ) maxFoundPivot  = returnPivot;
					} else { // keep the old pivot
						if(!skipNonPivotTransitions || !visitedToStates.contains(toState.getId())) { // we go to each toState only once with non-pivot transitions
							returnPivot = osCountStepOnePass(pivot, pos + 1, toState, level + 1 );
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


    // -- mining ------------------------------------------------------------------------------------------------------


	/**
	 * Mine with respect to a specific pivot item. e.g, filter out all non-pivot sequences
	 *   (e.g. filter out all sequences where pivotItem is not the max item)
	 * @param pivotItem
	 */
	public void minePivot(int pivotItem) {
		this.pivotItem = pivotItem;

		// run the normal mine method. Filtering is done in expand() when the patterns are output
		mine();

		// reset the pivot item, just to be sure. pivotItem=0 means no filter
		this.pivotItem = 0;
	}

	@Override
	public void mine() {
	    // this bundles common arguments to incStepOnePass or incStepTwoPass
		final IncStepArgs incStepArgs = new IncStepArgs();


		if (sumInputSupports >= sigma) {
			// stores the root of the search tree
		    DesqDfsTreeNode root;

			if (!useTwoPass) { // one-pass
                // create the root
				root = new DesqDfsTreeNode(fst.numStates());
				incStepArgs.node = root;

                // and process all input sequences to compute the roots children
				for (int inputId = 0; inputId < inputSequences.size(); inputId++) {
					incStepArgs.inputId = inputId;
					incStepArgs.inputSequence = inputSequences.get(inputId);
					incStepOnePass(incStepArgs, 0, fst.getInitialState(), 0);
				}
			} else { // two-pass
                // create the root
				root = new DesqDfsTreeNode(reverseFst.numStates());
				incStepArgs.node = root;

                // and process all input sequences to compute the roots children
                // note that we read the input sequences backwards in in the two-pass algorithm
                // the search tree is also constructed backwards
				for (int inputId = 0; inputId < inputSequences.size(); inputId++) {
					incStepArgs.inputId = inputId;
					incStepArgs.inputSequence = inputSequences.get(inputId);
					incStepArgs.edfaStateSequence = edfaStateSequences.get(inputId);
					final int[] finalStatePos = edfaFinalStatePositions.get(inputId);

                    // look at all positions before which a final FST state can be reached
                    for (final int pos : finalStatePos) {
                        // for those positions, start with each possible final FST state and go backwards
						for (State fstFinalState : incStepArgs.edfaStateSequence[pos].getFstFinalStates()) {
							incStepTwoPass(incStepArgs, pos-1, fstFinalState, 0);
						}
					}
				}

				// we don't need this anymore, so let's save the memory
				edfaFinalStatePositions.clear();
                edfaFinalStatePositions.trimToSize();
			}

			// recursively grow the patterns
			root.pruneInfrequentChildren(sigma);
			expand(new IntArrayList(), root);
		}
	}

    /** Updates the projected databases of the children of the current node (args.node) corresponding to each possible
     * next output item for the current input sequence (also stored in args). Used only in the one-pass algorithm.
     *
     * @param args information about the input sequence and the current search tree node
     * @param pos next item to read
     * @param state current FST state
     * @param level recursion level (used for reusing iterators without conflict)
     *
     * @return true if a final FST state can be reached without producing further output
     */
	private boolean incStepOnePass(final IncStepArgs args, final int pos, final State state, final int level) {
		// check if we reached the end of the input sequence
	    if (pos >= args.inputSequence.size())
			return state.isFinal(); // whether or not we require a full match

		// get iterator over next output item/state pairs; reuse existing ones if possible
		final int itemFid = args.inputSequence.getInt(pos);
		Iterator<ItemState> itemStateIt;
		if (level>=itemStateIterators.size()) {
			itemStateIt = state.consume(itemFid);
			itemStateIterators.add(itemStateIt);
		} else {
			itemStateIt = state.consume(itemFid, itemStateIterators.get(level));
		}

		// iterate over output item/state pairs and remember whether we hit a final state without producing output
        // (i.e., no transitions or only transitions with epsilon output)
		boolean reachedFinalStateWithoutOutput = state.isFinal() && !fst.getRequireFullMatch();
		while (itemStateIt.hasNext()) {
			final ItemState itemState = itemStateIt.next();
			final int outputItemFid = itemState.itemFid;
			final State toState = itemState.state;

			if (outputItemFid == 0) { // EPS output
                // we did not get an output, so continue running the FST
				int newLevel = level + (itemStateIt.hasNext() ? 1 : 0); // no need to create new iterator if we are done on this level
				reachedFinalStateWithoutOutput |= incStepOnePass(args, pos+1, toState, newLevel);
			} else if (largestFrequentFid >= outputItemFid) {
			    // we have an output and its frequent, so update the corresponding projected database
				args.node.expandWithItem(outputItemFid, args.inputId, args.inputSequence.support,
						pos+1, toState.getId());
			}
		}

		return reachedFinalStateWithoutOutput;
	}

    /** Updates the projected databases of the children of the current node (args.node) corresponding to each possible
     * previous output item for the current input sequence (also stored in args). Used only in the two-pass algorithm.
     *
     * @param args information about the input sequence and the current search tree node
     * @param pos next item to read
     * @param state current FST state
     * @param level recursion level (used for reusing iterators without conflict)
     *
     * @return true if the initial FST state can be reached without producing further output
     */
	private boolean incStepTwoPass(final IncStepArgs args, final int pos,
								   final State state, final int level) {
		// check if we reached the beginning of the input sequence
	    if(pos == -1) {
			// we consumed entire input in reverse -> we must have reached the inital state by two-pass correctness
			assert state.getId() == 0;
			return true;
		}

        // get iterator over next output item/state pairs; reuse existing ones if possible
        // note that the reverse FST is used here (since we process inputs backwards)
		// only iterates over states that we saw in the forward pass (the other ones can safely be skipped)
		final int itemFid = args.inputSequence.getInt(pos);
		Iterator<ItemState> itemStateIt;
		if (level>=itemStateIterators.size()) {
			itemStateIt = state.consume(itemFid, null, args.edfaStateSequence[pos].getFstStates());
			itemStateIterators.add(itemStateIt);
		} else {
			itemStateIt = state.consume(itemFid, itemStateIterators.get(level),
					args.edfaStateSequence[pos].getFstStates());
		}

        // iterate over output item/state pairs and remember whether we hit the initial state without producing output
        // (i.e., no transitions or only transitions with epsilon output)
        boolean reachedInitialStateWithoutOutput = false;
		while (itemStateIt.hasNext()) {
			final ItemState itemState = itemStateIt.next();
			final State toState = itemState.state;

			// we need to process that state because we saw it in the forward pass (assertion checks this)
			assert args.edfaStateSequence[pos].getFstStates().get(toState.getId());

			final int outputItemFid = itemState.itemFid;
			if (outputItemFid == 0) { // EPS output
                // we did not get an output, so continue running the reverse FST
				int newLevel = level + (itemStateIt.hasNext() ? 1 : 0); // no need to create new iterator if we are done on this level
				reachedInitialStateWithoutOutput |= incStepTwoPass(args, pos-1, toState, newLevel);
			} else if (largestFrequentFid >= outputItemFid && (pivotItem == 0 || pivotItem >= outputItemFid)) {
                // we have an output and its frequent, so update the corresponding projected database
				// if we are mining a pivot partition, we don't expand with items largerthan the pivot
				
				// Note: we do not store pos-1 in the projected database to having avoid write -1's when the
                // position was 0. When we read the posting list later, we substract 1
				args.node.expandWithItem(outputItemFid, args.inputId, args.inputSequence.support,
						pos, toState.getId());
			}
		}

		return reachedInitialStateWithoutOutput;
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

			if (!useTwoPass) { // one-pass
				do {
					// process next input sequence
					incStepArgs.inputId += projectedDatabaseIt.nextNonNegativeInt();
					incStepArgs.inputSequence = inputSequences.get(incStepArgs.inputId);

					// iterate over state@pos snapshots for this input sequence
                    boolean reachedFinalStateWithoutOutput = false;
					do {
						final int stateId = projectedDatabaseIt.nextNonNegativeInt();
						final int pos = projectedDatabaseIt.nextNonNegativeInt(); // position of next input item
						reachedFinalStateWithoutOutput |= incStepOnePass(incStepArgs, pos, fst.getState(stateId), 0);
					} while (projectedDatabaseIt.hasNext());

                    // if we reached a final state without output, increment the support of this child node
					if (reachedFinalStateWithoutOutput) {
						support += incStepArgs.inputSequence.support;
					}

					// now go to next posting (next input sequence)
				} while (projectedDatabaseIt.nextPosting());
			} else { // two-pass
				do {
					// process next input sequence (backwards)
					incStepArgs.inputId += projectedDatabaseIt.nextNonNegativeInt();
					incStepArgs.inputSequence = inputSequences.get(incStepArgs.inputId);
					incStepArgs.edfaStateSequence = edfaStateSequences.get(incStepArgs.inputId);

					// iterate over state@pos snapshots for this input sequence
                    boolean reachedInitialStateWithoutOutput = false;
					do {
						final int stateId = projectedDatabaseIt.nextNonNegativeInt();
						final int pos = projectedDatabaseIt.nextNonNegativeInt() - 1; // position of next input item
                                            // (-1 because we added a position incremented by one; see incStepTwoPass)
						reachedInitialStateWithoutOutput |= incStepTwoPass(incStepArgs, pos,
								reverseFst.getState(stateId), 0);
					} while (projectedDatabaseIt.hasNext());

                    // if we reached the initial state without output, increment the support of this child node
                    if (reachedInitialStateWithoutOutput) {
						support += incStepArgs.inputSequence.support;
					}

                    // now go to next posting (next input sequence)
				} while (projectedDatabaseIt.nextPosting());
			}

			// output the patterns for the current child node if it turns out to be frequent
			if (support >= sigma) {
				// if we are mining a specific pivot partition (meaning, pivotItem>0), then we output only sequences with that pivot
				if (ctx.patternWriter != null && (pivotItem==0 || pivot(prefix) == pivotItem)) {
					if (!useTwoPass) { // one-pass
						ctx.patternWriter.write(prefix, support);
					} else { // two-pass
                        // for the two-pass algorithm, we need to reverse the output because the search tree is built
                        // in reverse order
						ctx.patternWriter.writeReverse(prefix, support);
					}
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


	/** Bundles arguments for {@link #incStepOnePass} and {@link #incStepTwoPass}. These arguments are not modified
     * during the method's recursions, so we keep them at a single place. */
	private static class IncStepArgs {
		int inputId;
		WeightedSequence inputSequence;
		ExtendedDfaState[] edfaStateSequence;
		DesqDfsTreeNode node;
	}
}
