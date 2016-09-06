package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.ItemState;
import de.uni_mannheim.desq.fst.State;
import de.uni_mannheim.desq.patex.PatEx;
import de.uni_mannheim.desq.util.PrimitiveUtils;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;

import java.util.ArrayList;
import java.util.Iterator;

public final class DesqCount extends DesqMiner {

	// -- parameters for mining ---------------------------------------------------------------------------------------

	/** Minimum support */
	final long sigma;

	/** The pattern expression used for mining */
	final String patternExpression;

	/** Whether or not to use the f-list for pruning output sequences that are guaranteed to be infrequent. */
	final boolean useFlist;

	/** If true, iterative (instead of recursive) implementation is used */
	final boolean iterative;

	/** If true, input sequences that do not match the pattern expression are pruned */
	final boolean pruneIrrelevantInputs = false;

	/** If true, the two-pass algorithm for DesqDfs is used */
	final boolean useTwoPass = false;


	// -- helper variables --------------------------------------------------------------------------------------------

	/** Stores the final state transducer for DesqDfs (one-pass) */
	final Fst fst;

	/** Stores the largest fid of an item with frequency at least sigma. Used to quickly determine
	 * whether an item is frequent (if fid <= largestFrequentFid, the item is frequent */
	final int largestFrequentFid;

	/** Sequence id of the next input sequence */
	int inputId;

	// -- helper variables --------------------------------------------------------------------------------------------

	/** The input sequence currenlty processed */
	IntList inputSequence;

	/** Stores all mined sequences along with their frequency. Each long is composed of a 32-bit integer storing the
	 * actual count and a 32-bit integer storing the input id of the last input sequence that produced this output.  */
	final Object2LongOpenHashMap<IntList> outputSequences = new Object2LongOpenHashMap<>();

	/** Stores iterators over output item/next state pairs for reuse. Indexed by input position. */
	final ArrayList<Iterator<ItemState>> itemStateIterators = new ArrayList<>();

	/** Stores the part of the output sequence produced so far. */
	final IntList prefix;


	// -- helper variables for iterative processing -------------------------------------------------------------------

	/** For each position, whether {@link #prefix} was modified when processing this position last. */
	final BooleanList prefixModified;

	/** For each prefix length, whether the current {@link #prefix} up to this length has already been output.
	 * Used to avoid unnecessary duplicate output checking */
	final BooleanList prefixOutput;

	// -- construction/clearing ---------------------------------------------------------------------------------------

	public DesqCount(DesqMinerContext ctx) {
		super(ctx);
		this.sigma = ctx.conf.getLong("desq.mining.min.support");
		this.useFlist = ctx.conf.getBoolean("desq.mining.use.flist");
		this.iterative = ctx.conf.getBoolean("desq.mining.iterative");
		this.largestFrequentFid = ctx.dict.getLargestFidAboveDfreq(sigma);
		this.inputId = 0;
		
		patternExpression = ".* [" + ctx.conf.getString("desq.mining.pattern.expression") + "]";
		PatEx p = new PatEx(patternExpression, ctx.dict);
		this.fst = p.translate();
		fst.minimize();//TODO: move to translate

		prefix = new IntArrayList();
		prefixModified = new BooleanArrayList();
		prefixOutput = new BooleanArrayList();
		outputSequences.defaultReturnValue(-1L);
	}

	public static Configuration createConf(String patternExpression, long sigma) {
		PropertiesConfiguration conf = new PropertiesConfiguration();
		conf.setThrowExceptionOnMissing(true);
		conf.setProperty("desq.mining.miner.class", DesqCount.class.getCanonicalName());
		conf.setProperty("desq.mining.min.support", sigma);
		conf.setProperty("desq.mining.pattern.expression", patternExpression);
		conf.setProperty("desq.mining.use.flist", true);
		conf.setProperty("desq.mining.iterative", true);
		return conf;
	}


	// -- processing input sequences ---------------------------------------------------------------------------------

	@Override
	protected void addInputSequence(IntList inputSequence) {
		assert prefix.isEmpty(); // will be maintained by step()
		this.inputSequence = inputSequence;
		if (iterative) {
			stepIterative();
		} else {
			step(0, fst.getInitialState());
		}
		inputId++;
	}

	// -- mining ------------------------------------------------------------------------------------------------------

	@Override
	public void mine() {
		// by this time, the result is already stored in outputSequences. We only need to filter out the infrequent
		// ones.
		for(Object2LongMap.Entry<IntList> entry : outputSequences.object2LongEntrySet()) {
			long value = entry.getLongValue();
			int support = PrimitiveUtils.getLeft(value);
			if (support >= sigma) {
				if (ctx.patternWriter != null)
					ctx.patternWriter.write(entry.getKey(), support);
			}
		}
	}

	/** Simulates the FST starting from the given position and state. Maintains the invariant that the current
	 * output is stored in {@link #prefix}. Recursive version.
	 *
 	 * @param pos position of next input item
	 * @param state current state of FST
	 */
	private void step(int pos, State state) {
		// if we reached a final state, we count the current sequence (if any)
		if(state.isFinal() && !prefix.isEmpty()) {
			countSequence(prefix);
		}

		// check if we already read the entire input
		if (pos == inputSequence.size()) {
			return;
		}

		// get iterator over next output item/state pairs; reuse existing ones if possible
		final int itemFid = inputSequence.getInt(pos); // the current input item
		Iterator<ItemState> itemStateIt;
		if(pos >= itemStateIterators.size()) {
			itemStateIt = state.consume(itemFid);
			itemStateIterators.add(itemStateIt);
		} else {
			itemStateIt = state.consume(itemFid, itemStateIterators.get(pos));
		}

		// iterate over output item/state pairs
        while(itemStateIt.hasNext()) {
            final ItemState itemState = itemStateIt.next();
			final int outputItemFid = itemState.itemFid;
			final State toState = itemState.state;

			if(outputItemFid == 0) { // EPS output
				// we did not get an output, so continue with the current prefix
				step(pos + 1, toState);
			} else {
				// we got an output; check whether it is relevant
				if (!useFlist || largestFrequentFid >= outputItemFid) {
					// now append this item to the prefix, continue running the FST, and remove the item once done
					prefix.add(outputItemFid);
					step(pos + 1, toState);
					prefix.removeInt(prefix.size() - 1);
				}
			}
		}
	}

	/** Simulates the FST starting from the position 0 and the initial state. Iterative version. */
	private void stepIterative() {
		// something to do?
		if (inputSequence.size() == 0)
			return;

		// make sure we have enough space
		// we store in this array for each position whether or not the prefix (which contains the current output
		// sequence) has been modified when processing this position
		if (prefixModified.size() < inputSequence.size()) {
			prefixModified.size(inputSequence.size());
			prefixOutput.size(inputSequence.size() + 1); // that's the maximum length of an output sequence + 1
		}

		// get the first output item/state pairs
		int pos = 0; // position of current input item
		int itemFid = inputSequence.getInt(pos); // the current input item
		Iterator<ItemState> itemStateIt;
		if(pos >= itemStateIterators.size()) {
			itemStateIt = fst.getInitialState().consume(itemFid);
			itemStateIterators.add(itemStateIt);
		} else {
			itemStateIt = fst.getInitialState().consume(itemFid, itemStateIterators.get(pos));
			itemStateIterators.set(pos, itemStateIt); // make sure we remember the right one
		}
		prefixModified.set(0, false);

		// as long as we are not done
		while (pos >= 0) {
			// update the prefix by removing last item, if necessary
			if (prefixModified.get(pos)) {
				prefix.removeInt(prefix.size() - 1);
			}

			// are we done with the current position?
			itemStateIt = itemStateIterators.get(pos);
			if (!itemStateIt.hasNext()) {
				pos--;
				continue;
			}

			// get next output item/state pair for current position
			final ItemState itemState = itemStateIt.next();
			final int outputItemFid = itemState.itemFid;
			final State toState = itemState.state;

			// skip irrelevant items
			if (useFlist && largestFrequentFid < outputItemFid) {
				prefixModified.set(pos, false);
				continue;
			}

			// update prefix and count it (if final state has been reached)
			if(outputItemFid == 0) { // EPS output
				// we did not get an output, so keep current prefix
				prefixModified.set(pos, false);
			} else {
				// we got an output so update prefix
				prefix.add(outputItemFid);
				prefixModified.set(pos, true);
				prefixOutput.set(prefix.size(), false);
			}

			// if we reached a final state, we count the current sequence (if any)
			if (toState.isFinal() && !prefix.isEmpty() && !prefixOutput.getBoolean(prefix.size())) {
				countSequence(prefix);
				prefixOutput.set(prefix.size(), true);
			}

			// now we move to the next item (if any)
			if (pos < inputSequence.size()-1) {
				pos++;
				prefixModified.set(pos, false);
				itemFid = inputSequence.getInt(pos);
				if (pos >= itemStateIterators.size()) {
					itemStateIterators.add( toState.consume(itemFid) );
				} else {
					itemStateIterators.set(pos, toState.consume(itemFid, itemStateIterators.get(pos)) );
				}
			}
		}
	}

	/** Counts the provided output sequence. Avoids double-counting. */
	private void countSequence(IntList sequence) {
		long supSid = outputSequences.getLong(sequence);

		// add sequence if never mined before
		if (supSid == -1) { // set as return value when key not present
			outputSequences.put(new IntArrayList(sequence), PrimitiveUtils.combine(1, inputId)); // need to copy here
			return;
		}

		// otherwise increment frequency when if hasn't been mined from the current input sequence already
		if (PrimitiveUtils.getRight(supSid) != inputId) {
		    // TODO: can overflow
			int newCount = PrimitiveUtils.getLeft(supSid) + 1;
			outputSequences.put(sequence, PrimitiveUtils.combine(newCount, inputId));
		}
	}

}
