package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.fst.*;
import de.uni_mannheim.desq.fst.distributed.DfaCache;
import de.uni_mannheim.desq.fst.distributed.DfaCacheKey;
import de.uni_mannheim.desq.fst.distributed.FstCache;
import de.uni_mannheim.desq.fst.distributed.FstCacheKey;
import de.uni_mannheim.desq.patex.PatExUtils;
import de.uni_mannheim.desq.util.DesqProperties;
import de.uni_mannheim.desq.util.PrimitiveUtils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;

public final class DesqCount extends DesqMiner {
	private static final Logger logger = Logger.getLogger(DesqCount.class);

	// -- parameters for mining ---------------------------------------------------------------------------------------

	/** Minimum support */
	final long sigma;

	/** The pattern expression used for mining */
	final String patternExpression;

	/** Whether or not to use the f-list for pruning output sequences that are guaranteed to be infrequent. */
	final boolean useFlist;

	/** If true, input sequences that do not match the pattern expression are pruned */
	final boolean pruneIrrelevantInputs;

	/** If true, the two-pass algorithm is used */
	final boolean useTwoPass;


	// -- helper variables --------------------------------------------------------------------------------------------

	/** Stores the final state transducer  */
	private Fst fst;

	/** Stores the largest fid of an item with frequency at least sigma. Used to quickly determine
	 * whether an item is frequent (if fid <= largestFrequentFid, the item is frequent */
	final int largestFrequentFid;

	/** Sequence id of the next input sequence */
	int inputId;

	// -- helper variables --------------------------------------------------------------------------------------------

	/** The input sequence currenlty processed */
	IntList inputSequence;

	/** The support of the current input sequence */
	long inputSupport;

	/** Stores all mined sequences along with their frequency. Each long is composed of a 32-bit integer storing the
	 * actual count and a 32-bit integer storing the input id of the last input sequence that produced this output.  */
	final Object2LongOpenHashMap<Sequence> outputSequences = new Object2LongOpenHashMap<>();

	/** Stores iterators over output item/next state pairs for reuse. Indexed by input position. */
	final ArrayList<State.ItemStateIterator> itemStateIterators = new ArrayList<>();

	/** Stores the part of the output sequence produced so far. */
	final Sequence prefix;

	// -- helper variables for pruning and twopass --------------------------------------------------------------------

	/** The DFA corresponding to the FST (pruning) or reverse FST (two-pass). */
	private Dfa dfa;

	/** Sequence of EDFA states for current input (two-pass only) */
	final ArrayList<DfaState> dfaStateSequence;

	/** Positions for which dfa reached an initial FST state (two-pass only) */
	final IntList dfaInitalPos;


	// -- construction/clearing ---------------------------------------------------------------------------------------

	public DesqCount(DesqMinerContext ctx) {
		super(ctx);
		this.sigma = ctx.conf.getLong("desq.mining.min.support");
		this.useFlist = ctx.conf.getBoolean("desq.mining.use.flist");
		this.pruneIrrelevantInputs = ctx.conf.getBoolean("desq.mining.prune.irrelevant.inputs");
		this.useTwoPass = ctx.conf.getBoolean("desq.mining.use.two.pass");
		boolean useLazyDfa = ctx.conf.getBoolean("desq.mining.use.lazy.dfa");
		boolean isRunningAsUnitTest = ctx.conf.getBoolean("desq.mining.is.running.as.unit.test");

		// initalize helper variable for FST simulation
		this.largestFrequentFid = ctx.dict.lastFidAbove(sigma);
		this.inputId = 0;
		prefix = new Sequence();
		outputSequences.defaultReturnValue(-1L);

		// create FST
		patternExpression = ctx.conf.getString("desq.mining.pattern.expression");
		if(isRunningAsUnitTest) {
			this.fst = PatExUtils.toFst(ctx.dict, patternExpression);
		} else {
			this.fst = FstCache.getFst(new FstCacheKey(patternExpression,
					() -> PatExUtils.toFst(ctx.dict, patternExpression)));
		}

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
		if (useTwoPass) {
			// construct the DFA for the FST (for the first pass)
			// the DFA is constructed for the reverse FST
			if(isRunningAsUnitTest) {
				this.dfa = Dfa.createReverseDfa(fst, ctx.dict, largestFrequentFid, true, useLazyDfa);
			} else {
				this.dfa = DfaCache.getDfa(new DfaCacheKey(patternExpression,
						() -> Dfa.createReverseDfa(fst, ctx.dict, largestFrequentFid, true, useLazyDfa)));
			}
		} else if (pruneIrrelevantInputs) {
			// construct the DFA to prune irrelevant inputs
			// the DFA is constructed for the forward FST
			if(isRunningAsUnitTest) {
				this.dfa = Dfa.createDfa(fst, ctx.dict, largestFrequentFid, false, useLazyDfa);
			} else {
				this.dfa = DfaCache.getDfa(new DfaCacheKey(patternExpression,
						() -> Dfa.createDfa(fst, ctx.dict, largestFrequentFid, false, useLazyDfa)));
			}
		} else {
			this.dfa = null;
		}
	}

	public static DesqProperties createConf(String patternExpression, long sigma) {
		DesqProperties conf = new DesqProperties();
		conf.setProperty("desq.mining.miner.class", DesqCount.class.getCanonicalName());
		conf.setProperty("desq.mining.min.support", sigma);
		conf.setProperty("desq.mining.pattern.expression", patternExpression);
		conf.setProperty("desq.mining.use.flist", true);
		conf.setProperty("desq.mining.prune.irrelevant.inputs", true);
		conf.setProperty("desq.mining.use.lazy.dfa", false);
		conf.setProperty("desq.mining.use.two.pass", true);
		conf.setProperty("desq.mining.is.running.as.unit.test", false);
		return conf;
	}


	// -- processing input sequences ---------------------------------------------------------------------------------

	@Override
	public void addInputSequence(IntList sequence, long support, boolean allowBuffering) {
		assert prefix.isEmpty(); // will be maintained by stepOnePass()
		this.inputSequence = sequence;
		this.inputSupport = support;

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
		// by this time, the result is already stored in outputSequences. We only need to filter out the infrequent
		// ones.
		for(Object2LongMap.Entry<Sequence> entry : outputSequences.object2LongEntrySet()) {
			long value = entry.getLongValue();
			int support = PrimitiveUtils.getLeft(value);
			if (support >= sigma) {
				if (ctx.patternWriter != null) {
					ctx.patternWriter.write(entry.getKey(), support);
				}
			}
		}
	}

	/** Produces all outputs of the given input sequence that would be generated by DesqCount. Note that if you
	 * use this method, all inputs added previously will be cleared.
	 *
	 * @param inputSequence
	 * @return
	 */
	public ObjectSet<Sequence> mine1(IntList inputSequence, long inputSupport) {
		outputSequences.clear();
		addInputSequence(inputSequence, inputSupport, true);
		return outputSequences.keySet();
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
			if (!prefix.isEmpty() && state.isFinal()) {
				countSequence(prefix);
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
					// now append this item to the prefix, continue running the FST, and remove the item once done
					prefix.add(outputItemFid);
					final int newLevel = level + (itemStateIt.hasNext() ? 1 : 0); // no need to create new iterator if we are done on this level
					step(pos + 1, toState, newLevel);
					prefix.removeInt(prefix.size() - 1);
				}
			}
		}
	}

	/** Counts the provided output sequence. Avoids double-counting.
	 *
	 * TODO: does not work correctly if supports grow beyond integer size
	 */
	private void countSequence(Sequence sequence) {
		long supSid = outputSequences.getLong(sequence);

		// add sequence if never mined before
		if (supSid == -1) { // return value when key not present
			outputSequences.put(new Sequence(sequence), PrimitiveUtils.combine((int)inputSupport, inputId)); // need to copy here
			return;
		}

		// otherwise increment frequency when if hasn't been mined from the current input sequence already
		if (PrimitiveUtils.getRight(supSid) != inputId) {
			int newCount = PrimitiveUtils.getLeft(supSid) + (int)inputSupport;
			outputSequences.put(sequence, PrimitiveUtils.combine(newCount, inputId));
		}
	}

	// -- accessors to internal data structures (use with care) -------------------------------------------------------

	public Dfa getDfa() {
		return dfa;
	}

	public Fst getFst() {
		return fst;
	}

}
