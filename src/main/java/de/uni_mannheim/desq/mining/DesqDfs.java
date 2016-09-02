package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.ItemState;
import de.uni_mannheim.desq.fst.State;
import de.uni_mannheim.desq.journal.edfa.ExtendedDfa;
import de.uni_mannheim.desq.journal.edfa.ExtendedDfaState;
import de.uni_mannheim.desq.patex.PatEx;
import de.uni_mannheim.desq.util.PropertiesUtils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.log4j.Logger;

import java.util.*;

public class DesqDfs extends MemoryDesqMiner {
	private static final Logger logger = Logger.getLogger(DesqDfs.class);

	// parameters for mining
	final long sigma;
	final String patternExpression;
	final boolean pruneIrrelevantInputs;
	final boolean useTwoPass;

	// helper variables
	final Fst fst;
	final int largestFrequentFid; // used to quickly determine whether an item is frequent
	final ArrayList<Iterator<ItemState>> itemStateIterators = new ArrayList<>();
	final PostingList.Iterator postingsIt = new PostingList.Iterator();

	// helper variables for pruning and twopass
	final ExtendedDfa edfa;

	// helper variables for twopass
	final List<ExtendedDfaState[]> edfaStateSequences; // per relevant input sequence
	final List<int[]> edfaFinalStatePositions; // per relevant input sequence
	final List<ExtendedDfaState> edfaStateSequence;
	final IntList finalPos;

	public DesqDfs(DesqMinerContext ctx) {
		super(ctx);
		this.sigma = PropertiesUtils.getLong(ctx.properties, "minSupport");
		this.largestFrequentFid = ctx.dict.getLargestFidAboveDfreq(sigma);
		
		patternExpression = ".* [" + PropertiesUtils.get(ctx.properties, "patternExpression").trim() + "]";
		PatEx p = new PatEx(patternExpression, ctx.dict);
		this.fst = p.translate();
		fst.minimize(); //TODO: move to translate

		useTwoPass = PropertiesUtils.getBoolean(ctx.properties, "useTwoPass");
		pruneIrrelevantInputs = PropertiesUtils.getBoolean(ctx.properties, "pruneIrrelevantInputs");

		if (useTwoPass) {
			if (!pruneIrrelevantInputs) {
				logger.warn("setting of pruneIrrelevantInputs=false will be ignored because useTwoPass=true");
			}

			this.edfa = new ExtendedDfa(fst, ctx.dict);
			fst.reverse(false); // here we need the reverse fst
			edfaStateSequences = new ArrayList<>();
			edfaFinalStatePositions = new ArrayList<>();
			edfaStateSequence = new ArrayList<>();
			finalPos = new IntArrayList();
		} else {
			edfaStateSequences = null;
			edfaFinalStatePositions = null;
			edfaStateSequence = null;
			finalPos  = null;
			if (pruneIrrelevantInputs) {
				this.edfa = new ExtendedDfa(fst, ctx.dict);
			} else {
				this.edfa = null;
			}
		}
	}
	
	public static Properties createProperties(String patternExpression, long sigma) {
		Properties properties = new Properties();
		PropertiesUtils.set(properties, "patternExpression", patternExpression);
		PropertiesUtils.set(properties, "minSupport", sigma);
		PropertiesUtils.set(properties, "minerClass", DesqDfs.class.getCanonicalName());
		PropertiesUtils.set(properties, "pruneIrrelevantInputs", false);
		PropertiesUtils.set(properties, "useTwoPass", false);
		return properties;
	}
	
	public void clear() {
		inputSequences.clear();
		if (useTwoPass) {
			edfaStateSequences.clear();
			edfaFinalStatePositions.clear();
		}
	}


	@Override
	public void addInputSequence(IntList inputSequence, int inputSupport) {
		if (useTwoPass) {
			if (edfa.isRelevant(inputSequence, 0, edfaStateSequence, finalPos)) {
				super.addInputSequence(inputSequence, inputSupport);
				edfaStateSequences.add(edfaStateSequence.toArray(new ExtendedDfaState[edfaStateSequence.size()]));
				//TODO: directly do the first incStep to avoid copying finalPos
				edfaFinalStatePositions.add(finalPos.toIntArray());
				finalPos.clear();
			}
			edfaStateSequence.clear();
			return;
		}

		// one-pass
		if (!pruneIrrelevantInputs || edfa.isRelevant(inputSequence, 0, 0)) {
			super.addInputSequence(inputSequence, inputSupport);
		}
	}

	@Override
	public void mine() {
		if (sumInputSupports >= sigma) {
			DesqDfsTreeNode root = new DesqDfsTreeNode(new DesqDfsProjectedDatabase(fst.numStates()));

			if (!useTwoPass) {
				for (int inputId = 0; inputId < inputSupports.size(); inputId++) {
					final int[] inputSequence = inputSequences.get(inputId);
					final int inputSupport = inputSupports.get(inputId);
					incStep(inputId, inputSequence, 0, inputSupport, fst.getInitialState(), root, 0);
				}
			} else { // twopass
				for (int inputId = 0; inputId < inputSupports.size(); inputId++) {
					final int[] inputSequence = inputSequences.get(inputId);
					final int inputSupport = inputSupports.get(inputId);
					final ExtendedDfaState[] edfaStateSequence = edfaStateSequences.get(inputId);
					final int[] finalStatePos = edfaFinalStatePositions.get(inputId);
					for (final int pos : finalStatePos) {
						for (State fstFinalState : edfaStateSequence[pos].getFstFinalStates()) {
							incStepTwoPass(inputId, inputSequence, edfaStateSequence, pos-1, inputSupport,
									fstFinalState, root, 0);
						}
					}
				}
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

	// this runs backwards
	private boolean incStepTwoPass(final int inputId, final int[] inputSequence,
								   final ExtendedDfaState[] edfaStateSequence,
								   final int pos, final int inputSupport,
								   final State state, final DesqDfsTreeNode node, final int level) {
		if(pos == -1) {
			// we consumed entire input in reverse -> we must have reached inital state by twopass correctness
			assert state.getId() == 0;
			return true;
		}

		// get iterator over output item/state pairs; reuse existing ones if possible
		// todo: this is inefficient because we generate item/state pairs that we are not going to need
		final int itemFid = inputSequence[pos];
		Iterator<ItemState> itemStateIt;
		if (level>=itemStateIterators.size()) {
			itemStateIt = state.consume(itemFid);
			itemStateIterators.add(itemStateIt);
		} else {
			itemStateIt = state.consume(itemFid, itemStateIterators.get(level));
		}

		// iterator over output item/state pairs
		boolean reachedFinalStateWithoutOutput = false;
		while (itemStateIt.hasNext()) {
			final ItemState itemState = itemStateIt.next();
			final State toState = itemState.state;

			// skip irrelevant tostates
			if (!edfaStateSequence[pos].getFstStates().get(toState.getId()))
				continue;

			final int outputItemFid = itemState.itemFid;
			if (outputItemFid == 0) { // EPS output
				reachedFinalStateWithoutOutput |= incStepTwoPass(inputId, inputSequence, edfaStateSequence,
						pos-1, inputSupport, toState, node, level+1);
			} else if (largestFrequentFid >= outputItemFid) {
				// we do not expand with pos-1 but with pos to avoid writing -1's when the position was 0
				// when we read the posting list, we have to substract 1
				node.expandWithItem(outputItemFid, inputId, inputSupport, pos, toState.getId());
			}
		}

		return reachedFinalStateWithoutOutput;
	}

		// node must have been processed/output/expanded already, but children not
	// upon return, prefix must be unmodified
	private void expand(IntArrayList prefix, DesqDfsTreeNode node) {
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

			if (!useTwoPass) {
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
			} else {
				do {
					// process next input sequence
					final int inputId = postingsIt.nextNonNegativeInt();
					final int[] inputSequence = inputSequences.get(inputId);
					final int inputSupport = inputSupports.getInt(inputId);
					final ExtendedDfaState[] edfaStateSequence = edfaStateSequences.get(inputId);
					boolean reachedFinalStateWithoutOutput = false;

					// iterate over state@pos snapshots for this input sequence
					do {
						final int stateId = postingsIt.nextNonNegativeInt();
						final int pos = postingsIt.nextNonNegativeInt() - 1; // position of next input item (-1 because ehre we added a position incremented by one)
						reachedFinalStateWithoutOutput |= incStepTwoPass(inputId, inputSequence,edfaStateSequence,
								pos, inputSupport, fst.getState(stateId), childNode, 0);
					} while (postingsIt.hasNext());

					if (reachedFinalStateWithoutOutput) {
						support += inputSupport;
					}
				} while (postingsIt.nextPosting());
			}

			// and output if p-frequent
			if (support >= sigma) {
				if (ctx.patternWriter != null) {
					if (!useTwoPass) {
						ctx.patternWriter.write(prefix, support);
					} else {
						// TODO: make more efficient (avoid double reversal)
						java.util.Collections.reverse(prefix);
						ctx.patternWriter.write(prefix, support);
						java.util.Collections.reverse(prefix);
					}
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
