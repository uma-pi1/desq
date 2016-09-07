package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.ItemState;
import de.uni_mannheim.desq.fst.State;
import de.uni_mannheim.desq.fst.ExtendedDfa;
import de.uni_mannheim.desq.fst.ExtendedDfaState;
import de.uni_mannheim.desq.patex.PatEx;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;

public final class DesqDfs extends MemoryDesqMiner {
	private static final Logger logger = Logger.getLogger(DesqDfs.class);

	// -- parameters for de.uni_mannheim.desq.old.mining ---------------------------------------------------------------------------------------

	/** Minimum support */
    final long sigma;

    /** The pattern expression used for de.uni_mannheim.desq.old.mining */
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


    // -- construction/clearing ---------------------------------------------------------------------------------------

	public DesqDfs(DesqMinerContext ctx) {
		super(ctx);
		sigma = ctx.conf.getLong("desq.mining.min.support");
		largestFrequentFid = ctx.dict.getLargestFidAboveDfreq(sigma);
		pruneIrrelevantInputs = ctx.conf.getBoolean("desq.mining.prune.irrelevant.inputs");
        useTwoPass = ctx.conf.getBoolean("desq.mining.use.two.pass");

        // construct pattern expression and FST
		patternExpression = ".* [" + ctx.conf.getString("desq.mining.pattern.expression").trim() + "]";
		PatEx p = new PatEx(patternExpression, ctx.dict);
		Fst tempFst = p.translate();
		tempFst.minimize(); //TODO: move to translate

		if (useTwoPass) { // two-pass
            // two-pass will always prune irrelevant input sequences, so notify the user when the corresponding
            // property is not set
			if (!pruneIrrelevantInputs) {
				logger.warn("property desq.de.uni_mannheim.desq.old.mining.prune.irrelevant.inputs=false will be ignored because " +
						"desq.mining.use.two.pass=true");
			}

            // construct the DFA for the FST (for the forward pass)
			this.edfa = new ExtendedDfa(tempFst, ctx.dict);

            // and then reverse the FST (which we now only need for the backward pass)
            tempFst.reverse(false); // here we need the reverse de.uni_mannheim.desq.old.fst
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

	public static Configuration createConf(String patternExpression, long sigma) {
		PropertiesConfiguration conf = new PropertiesConfiguration();
		conf.setThrowExceptionOnMissing(true);
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
	public void addInputSequence(IntList inputSequence, int inputSupport) {
        // two-pass version of DesqDfs
        if (useTwoPass) {
            // run the input sequence through the EDFA and compute the state sequences as well as the positions before
            // which a final FST state is reached
			if (edfa.isRelevant(inputSequence, 0, edfaStateSequence, finalPos)) {
			    // we now know that the sequence is relevant; remember it
				super.addInputSequence(inputSequence, inputSupport);
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
		    super.addInputSequence(inputSequence, inputSupport);
		}
	}


    // -- de.uni_mannheim.desq.old.mining ------------------------------------------------------------------------------------------------------

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
				for (int inputId = 0; inputId < inputSupports.size(); inputId++) {
					incStepArgs.inputId = inputId;
					incStepArgs.inputSequence = inputSequences.get(inputId);
					incStepArgs.inputSupport = inputSupports.get(inputId);
					incStepOnePass(incStepArgs, 0, fst.getInitialState(), 0);
				}
			} else { // two-pass
                // create the root
				root = new DesqDfsTreeNode(reverseFst.numStates());
				incStepArgs.node = root;

                // and process all input sequences to compute the roots children
                // note that we read the input sequences backwards in in the two-pass algorithm
                // the search tree is also constructed backwards
				for (int inputId = 0; inputId < inputSupports.size(); inputId++) {
					incStepArgs.inputId = inputId;
					incStepArgs.inputSequence = inputSequences.get(inputId);
					incStepArgs.inputSupport = inputSupports.get(inputId);
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
	    if (pos >= args.inputSequence.length)
			return state.isFinal();

		// get iterator over next output item/state pairs; reuse existing ones if possible
		final int itemFid = args.inputSequence[pos];
		Iterator<ItemState> itemStateIt;
		if (level>=itemStateIterators.size()) {
			itemStateIt = state.consume(itemFid);
			itemStateIterators.add(itemStateIt);
		} else {
			itemStateIt = state.consume(itemFid, itemStateIterators.get(level));
		}

		// iterate over output item/state pairs and remember whether we hit a final state without producing output
        // (i.e., no transitions or only transitions with epsilon output)
		boolean reachedFinalStateWithoutOutput = state.isFinal();
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
				args.node.expandWithItem(outputItemFid, args.inputId, args.inputSupport,
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
		// TODO: this is inefficient because we generate item/state pairs that we are not going to need
		final int itemFid = args.inputSequence[pos];
		Iterator<ItemState> itemStateIt;
		if (level>=itemStateIterators.size()) {
			itemStateIt = state.consume(itemFid);
			itemStateIterators.add(itemStateIt);
		} else {
			itemStateIt = state.consume(itemFid, itemStateIterators.get(level));
		}

        // iterate over output item/state pairs and remember whether we hit the initial state without producing output
        // (i.e., no transitions or only transitions with epsilon output)
        boolean reachedInitialStateWithoutOutput = false;
		while (itemStateIt.hasNext()) {
			final ItemState itemState = itemStateIt.next();
			final State toState = itemState.state;

			// if the toState was not a reachable state in the forward pass, we can safely skip it
			if (!args.edfaStateSequence[pos].getFstStates().get(toState.getId()))
				continue;

			final int outputItemFid = itemState.itemFid;
			if (outputItemFid == 0) { // EPS output
                // we did not get an output, so continue running the reverse FST
				int newLevel = level + (itemStateIt.hasNext() ? 1 : 0); // no need to create new iterator if we are done on this level
				reachedInitialStateWithoutOutput |= incStepTwoPass(args, pos-1, toState, newLevel);
			} else if (largestFrequentFid >= outputItemFid) {
                // we have an output and its frequent, so update the corresponding projected database
				// Note: we do not store pos-1 in the projected database to having avoid write -1's when the
                // position was 0. When we read the posting list later, we substract 1
				args.node.expandWithItem(outputItemFid, args.inputId, args.inputSupport,
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
					incStepArgs.inputSupport = inputSupports.getInt(incStepArgs.inputId);

					// iterate over state@pos snapshots for this input sequence
                    boolean reachedFinalStateWithoutOutput = false;
					do {
						final int stateId = projectedDatabaseIt.nextNonNegativeInt();
						final int pos = projectedDatabaseIt.nextNonNegativeInt(); // position of next input item
						reachedFinalStateWithoutOutput |= incStepOnePass(incStepArgs, pos, fst.getState(stateId), 0);
					} while (projectedDatabaseIt.hasNext());

                    // if we reached a final state without output, increment the support of this child node
					if (reachedFinalStateWithoutOutput) {
						support += incStepArgs.inputSupport;
					}

					// now go to next posting (next input sequence)
				} while (projectedDatabaseIt.nextPosting());
			} else { // two-pass
				do {
					// process next input sequence (backwards)
					incStepArgs.inputId += projectedDatabaseIt.nextNonNegativeInt();
					incStepArgs.inputSequence = inputSequences.get(incStepArgs.inputId);
					incStepArgs.inputSupport = inputSupports.getInt(incStepArgs.inputId);
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
						support += incStepArgs.inputSupport;
					}

                    // now go to next posting (next input sequence)
				} while (projectedDatabaseIt.nextPosting());
			}

			// output the patterns for the current child node if it turns out to be frequent
			if (support >= sigma) {
				if (ctx.patternWriter != null) {
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

	/** Bundles arguments for {@link #incStepOnePass} and {@link #incStepTwoPass}. These arguments are not modified
     * during the method's recursions, so we keep them at a single place. */
	private static class IncStepArgs {
		int inputId;
		int[] inputSequence;
		ExtendedDfaState[] edfaStateSequence;
		int inputSupport;
		DesqDfsTreeNode node;
	}
}
