package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.fst.*;
import de.uni_mannheim.desq.patex.PatEx;
import de.uni_mannheim.desq.util.DesqProperties;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;

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

    /** Stores the largest fid of an item with frequency at least sigma. Zsed to quickly determine
     * whether an item is frequent (if fid <= largestFrequentFid, the item is frequent */
    final int largestFrequentFid;

    /** Stores iterators over output item/next state pairs for reuse. */
	final ArrayList<Iterator<ItemState>> itemStateIterators = new ArrayList<>();

    /** An iterator over a projected database (a posting list) for reuse */
    final PostingList.Iterator projectedDatabaseIt = new PostingList.Iterator();


	// -- helper variables for pruning and twopass --------------------------------------------------------------------

	/** The DFA corresponding to the pattern expression. Accepts if the FST can reach a final state on an input */
    final ExtendedDfa rdfa;

	// helper variables for twopass
    /** For each relevant input sequence, the sequence of states taken by rdfa */
	final ArrayList<ExtendedDfaState[]> rdfaStateSequences;

    /** For each relevant input sequence, the set of positions from which the FST can reach a final state (before
     * reading the item at that position) */
    final ArrayList<int[]> rdfaInitialPos; // per relevant input sequence

    /** A sequence of EDFA states for reuse */
    final ArrayList<ExtendedDfaState> stateSequence;

    /** A sequence of positions for reuse */
    final IntList initialPos;


    // -- construction/clearing ---------------------------------------------------------------------------------------

	public DesqDfs(DesqMinerContext ctx) {
		super(ctx);
		sigma = ctx.conf.getLong("desq.mining.min.support");
		largestFrequentFid = ctx.dict.lastFidAbove(sigma);
		pruneIrrelevantInputs = ctx.conf.getBoolean("desq.mining.prune.irrelevant.inputs");
        useTwoPass = ctx.conf.getBoolean("desq.mining.use.two.pass");

		// create FST
		patternExpression = ctx.conf.getString("desq.mining.pattern.expression");
		PatEx p = new PatEx(patternExpression, ctx.dict);
		this.fst = p.translate();
		fst.minimize(); //TODO: move to translate
		fst.annotateFinalStates();

		// create two pass auxiliary variables (if needed)
		if (useTwoPass) { // two-pass
            // two-pass will always prune irrelevant input sequences, so notify the user when the corresponding
            // property is not set
			if (!pruneIrrelevantInputs) {
				logger.warn("property desq.mining.prune.irrelevant.inputs=false will be ignored because " +
						"desq.mining.use.two.pass=true");
			}

            // initialize helper variables for two-pass
			rdfaStateSequences = new ArrayList<>();
			rdfaInitialPos = new ArrayList<>();
			stateSequence = new ArrayList<>();
			initialPos = new IntArrayList();
		} else { // invalidate helper variables for two-pass
            rdfaStateSequences = null;
			rdfaInitialPos = null;
			stateSequence = null;
			initialPos = null;
		}

		// create reverse DFA (if needed)
		if (pruneIrrelevantInputs || useTwoPass) {
			// construct the DFA for the FST (for the first pass)
			// the DFA is constructed for the reverse FST!
			this.rdfa = new ExtendedDfa(fst, ctx.dict, true);
		} else {
			this.rdfa = null;
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
			rdfaStateSequences.clear();
            rdfaStateSequences.trimToSize();
			rdfaInitialPos.clear();
            rdfaInitialPos.trimToSize();
		}
	}

	// -- processing input sequences ----------------------------------------------------------------------------------

	@Override
	public void addInputSequence(IntList inputSequence, long inputSupport, boolean allowBuffering) {
        // two-pass version of DesqDfs
        if (useTwoPass) {
            // run the input sequence through the EDFA and compute the state sequences as well as the positions from
            // which a final FST state is reached
			if (rdfa.isRelevantReverse(inputSequence, stateSequence, initialPos)) {
			    // we now know that the sequence is relevant; remember it
				super.addInputSequence(inputSequence, inputSupport, allowBuffering);
				rdfaStateSequences.add(stateSequence.toArray(new ExtendedDfaState[stateSequence.size()]));
				//TODO: directly do the first incStep to avoid copying and storing positions
				rdfaInitialPos.add(initialPos.toIntArray());
				initialPos.clear();
			}
			stateSequence.clear();
            return;
		}

		// one-pass version of DesqDfs
		if (!pruneIrrelevantInputs || rdfa.isRelevantReverse(inputSequence)) {
			// if we reach this place, we either don't want to prune irrelevant inputs or the input is relevant
            // -> remember it
		    super.addInputSequence(inputSequence, inputSupport, allowBuffering);
		}
	}


    // -- mining ------------------------------------------------------------------------------------------------------

	@Override
	public void mine() {
	    // this bundles common arguments to incStep
		final IncStepArgs incStepArgs = new IncStepArgs();

		if (sumInputSupports >= sigma) {
			// stores the root of the search tree
		    DesqDfsTreeNode root = new DesqDfsTreeNode(fst.numStates());
			incStepArgs.node = root;

			// process all input sequences to compute the roots children
			if (!useTwoPass) { // one-pass
				for (int inputId = 0; inputId < inputSequences.size(); inputId++) {
					incStepArgs.inputId = inputId;
					incStepArgs.inputSequence = inputSequences.get(inputId);
					incStep(incStepArgs, 0, fst.getInitialState(), 0);
				}
			} else { // two-pass
				for (int inputId = 0; inputId < inputSequences.size(); inputId++) {
					incStepArgs.inputId = inputId;
					incStepArgs.inputSequence = inputSequences.get(inputId);
					incStepArgs.rdfaStateSequence = rdfaStateSequences.get(inputId);
					final int[] initialStatePos = rdfaInitialPos.get(inputId);

                    // look at all positions from which a final FST state can be reached
                    for (final int pos : initialStatePos) {
                        // for those positions, start with the initial state
						incStep(incStepArgs, pos, fst.getInitialState(), 0);
					}
				}

				// we don't need this anymore, so let's save the memory
				rdfaInitialPos.clear();
                rdfaInitialPos.trimToSize();
			}

			// recursively grow the patterns
			root.pruneInfrequentChildren(sigma);
			expand(new IntArrayList(), root);
		}
	}

    /** Updates the projected databases of the children of the current node (args.node) corresponding to each possible
     * next output item for the current input sequence (also stored in args).
     *
     * @param args information about the input sequence and the current search tree node
     * @param pos next item to read
     * @param state current FST state
     * @param level recursion level (used for reusing iterators without conflict)
     *
     * @return true if the initial FST state can be reached without producing further output
     */
	private boolean incStep(final IncStepArgs args, final int pos,
							final State state, final int level) {
		// check if we reached a final complete state or consumed entire input and reached a final state
		if ( state.isFinalComplete() || pos == args.inputSequence.size() )
		    return state.isFinal();

        // get iterator over next output item/state pairs; reuse existing ones if possible
		// in two-pass, only iterates over states that we saw in the first pass (the other ones can safely be skipped)
		final int itemFid = args.inputSequence.getInt(pos);
		final BitSet validToStates = useTwoPass
				? args.rdfaStateSequence[ args.inputSequence.size()-(pos+1) ].getFstStates() // only states from first pass
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
				reachedInitialStateWithoutOutput |= incStep(args, pos + 1, toState, newLevel);
			} else if (largestFrequentFid >= outputItemFid) {
                // we have an output and its frequent, so update the corresponding projected database
				args.node.expandWithItem(outputItemFid, args.inputId, args.inputSequence.weight,
						pos+1, toState.getId());
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

			do {
				// process next input sequence
				incStepArgs.inputId += projectedDatabaseIt.nextNonNegativeInt();
				incStepArgs.inputSequence = inputSequences.get(incStepArgs.inputId);
				if (useTwoPass) {
					incStepArgs.rdfaStateSequence = rdfaStateSequences.get(incStepArgs.inputId);
				}

				// iterate over state@pos snapshots for this input sequence
                boolean reachedFinalStateWithoutOutput = false;
				do {
					final int stateId = projectedDatabaseIt.nextNonNegativeInt();
					final int pos = projectedDatabaseIt.nextNonNegativeInt(); // position of next input item
					reachedFinalStateWithoutOutput |= incStep(incStepArgs, pos, fst.getState(stateId), 0);
				} while (projectedDatabaseIt.hasNext());

                // if we reached a final state without output, increment the support of this child node
				if (reachedFinalStateWithoutOutput) {
					support += incStepArgs.inputSequence.weight;
				}

				// now go to next posting (next input sequence)
			} while (projectedDatabaseIt.nextPosting());

			// output the patterns for the current child node if it turns out to be frequent
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

	/** Bundles arguments for {@link #incStep}. These arguments are not modified during the method's recursions, so
	 * we keep them at a single place. */
	private static class IncStepArgs {
		int inputId;
		WeightedSequence inputSequence;
		ExtendedDfaState[] rdfaStateSequence;
		DesqDfsTreeNode node;
	}
}
