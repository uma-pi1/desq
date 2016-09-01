package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.ItemState;
import de.uni_mannheim.desq.fst.State;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.journal.edfa.ExtendedDfa;
import de.uni_mannheim.desq.patex.PatEx;
import de.uni_mannheim.desq.util.PropertiesUtils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

public class DesqDfs extends MemoryDesqMiner {
    // parameters for mining
	final long sigma;
	final String patternExpression;
	final boolean pruneIrrelevantInputs;

	// helper variables
	final Fst fst;
	final ExtendedDfa edfa;
	final int largestFrequentFid; // used to quickly determine whether an item is frequent
	final ArrayList<Iterator<ItemState>> itemStateIterators = new ArrayList<>();
	final PostingList.Iterator postingsIt = new PostingList.Iterator();


	public DesqDfs(DesqMinerContext ctx) {
		super(ctx);
		this.sigma = PropertiesUtils.getLong(ctx.properties, "minSupport");
		this.largestFrequentFid = ctx.dict.getLargestFidAboveDfreq(sigma);
		
		patternExpression = ".* [" + PropertiesUtils.get(ctx.properties, "patternExpression").trim() + "]";
		PatEx p = new PatEx(patternExpression, ctx.dict);
		this.fst = p.translate();
		fst.minimize(); //TODO: move to translate

		pruneIrrelevantInputs = PropertiesUtils.getBoolean(ctx.properties, "pruneIrrelevantInputs");
		if (pruneIrrelevantInputs) {
			this.edfa = new ExtendedDfa(fst, ctx.dict);
		} else {
			this.edfa = null; // don't need it
		}
	}
	
	public static Properties createProperties(String patternExpression, long sigma) {
		Properties properties = new Properties();
		PropertiesUtils.set(properties, "patternExpression", patternExpression);
		PropertiesUtils.set(properties, "minSupport", sigma);
		PropertiesUtils.set(properties, "minerClass", DesqDfs.class.getCanonicalName());
		PropertiesUtils.set(properties, "pruneIrrelevantInputs", false);
		return properties;
	}
	
	public void clear() {
		inputSequences.clear();
	}


	@Override
	public void addInputSequence(IntList inputSequence, int inputSupport) {
		if (!pruneIrrelevantInputs || edfa.isRelevant(inputSequence, 0, 0)) {
			super.addInputSequence(inputSequence, inputSupport);
		}
	}

	@Override
	public void mine() {
		if (sumInputSupports >= sigma) {
			DesqDfsTreeNode root = new DesqDfsTreeNode(new DesqDfsProjectedDatabase(fst.numStates()));

			for (int inputId = 0; inputId < inputSupports.size(); inputId++) {
				int[] inputSequence = inputSequences.get(inputId);
				int inputSupport = inputSupports.get(inputId);
				incStep(inputId, inputSequence, 0, inputSupport, fst.getInitialState(), root, 0);
			}

			root.expansionsToChildren(sigma);
			expand(new IntArrayList(), root);
		}
	}

	private boolean incStep(final int inputId, final int[] inputSequence, final int pos, final int inputSupport,
						 final State state, final DesqDfsTreeNode node, final int level) {
		if (pos >= inputSequence.length)
			return state.isFinal();

		// get iterator over output item/state pairs; reuse existing ones if possible
		final int itemFid = inputSequence[pos];
		Iterator<ItemState> itemStateIt;
		if (level>=itemStateIterators.size()) {
			itemStateIt = state.consume(itemFid);
			itemStateIterators.add(itemStateIt);
		} else {
			itemStateIt = state.consume(itemFid, itemStateIterators.get(level));
		}

		// iterator over output item/state pairs
		boolean reachedFinalStateWithoutOutput = state.isFinal();
		while (itemStateIt.hasNext()) {
			final ItemState itemState = itemStateIt.next();
			final int outputItemFid = itemState.itemFid;
			final State toState = itemState.state;

			if (outputItemFid == 0) { // EPS output
				reachedFinalStateWithoutOutput |= incStep(inputId, inputSequence, pos+1, inputSupport, toState, node, level+1);
			} else if (largestFrequentFid >= outputItemFid) {
				node.expandWithItem(outputItemFid, inputId, inputSupport, pos+1, toState.getId());
			}
		}

		return reachedFinalStateWithoutOutput;
	}

	// node must have been processed/output/expanded already, but children not
	// upon return, prefix must be unmodified
	private void expand(IntList prefix, DesqDfsTreeNode node) {
		// add a placeholder to prefix
		final int lastPrefixIndex = prefix.size();
		prefix.add(-1);

		// iterate over children
		for (final DesqDfsTreeNode childNode : node.children )  {
			// while we expand the child node, we also compute its actual support to determine whether or not
			// to output it
			long support = 0;

			// we first expand
			final DesqDfsProjectedDatabase projectedDatabase = childNode.projectedDatabase;
			assert projectedDatabase.prefixSupport >= sigma;
			prefix.set(lastPrefixIndex, projectedDatabase.itemFid);
			postingsIt.reset(projectedDatabase.postingList);
			do {
				// process next input sequence
				final int inputId = postingsIt.nextNonNegativeInt();
				final int[] inputSequence = inputSequences.get(inputId);
				final int inputSupport = inputSupports.getInt(inputId);
				boolean reachedFinalStateWithoutOutput = false;

				// iterate over state@pos snapshots for this input sequence
				do {
					final int stateId = postingsIt.nextNonNegativeInt();
					final int pos = postingsIt.nextNonNegativeInt(); // position of next input item
					reachedFinalStateWithoutOutput |= incStep(inputId, inputSequence, pos, inputSupport,
							fst.getState(stateId), childNode, 0);
				} while (postingsIt.hasNext());

				if (reachedFinalStateWithoutOutput) {
					support += inputSupport;
				}
			} while (postingsIt.nextPosting());

			// and output if p-frequent
			if (support >= sigma) {
				if (ctx.patternWriter != null) {
					ctx.patternWriter.write(prefix, support);
				}
			}

			// expand the just created node
			childNode.expansionsToChildren(sigma);
			childNode.projectedDatabase = null; // not needed anymore
			expand(prefix, childNode);
			childNode.invalidate(); // not needed anymore
		}

		prefix.removeInt(lastPrefixIndex);
	}
}
