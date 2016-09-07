package de.uni_mannheim.desq.journal.mining;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.ItemState;
import de.uni_mannheim.desq.journal.edfa.ExtendedDfa;
import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import de.uni_mannheim.desq.patex.PatEx;
import de.uni_mannheim.desq.util.PrimitiveUtils;
import de.uni_mannheim.desq.util.PropertiesUtils;


// TODO: integrate into DesqCount
public class DesqCountWithPruning extends DesqMiner {

	// parameters for mining
	String patternExpression;
	long sigma;
	boolean useFlist = true;

	// helper variables
	int largestFrequentFid;
	Fst fst;
	ExtendedDfa eDfa;
	int initialStateId;
	int sid;
	IntList buffer;
	IntList inputSequence;
	Object2LongMap<IntList> outputSequences = new Object2LongOpenHashMap<>();
	ArrayList<Iterator<ItemState>> itemStateIterators = new ArrayList<>();
	

	public DesqCountWithPruning(DesqMinerContext ctx) {
		super(ctx);
		this.sigma = ctx.conf.getLong("minSupport");
		this.useFlist = ctx.conf.getBoolean("useFlist", true);
		this.largestFrequentFid = ctx.dict.getLargestFidAboveDfreq(sigma);
		this.sid = 0;

		this.patternExpression = ctx.conf.getString("patternExpression");
		patternExpression = ".* [" + patternExpression.trim() + "]";
		PatEx p = new PatEx(patternExpression, ctx.dict);
		this.fst = p.translate();
		fst.minimize();// TODO: move to translate
		this.initialStateId = fst.getInitialState().getId();

		// extendedDfa
		this.eDfa = new ExtendedDfa(fst, ctx.dict);

		buffer = new IntArrayList();
	}

	public static Properties createProperties(String patternExpression, int sigma) {
		Properties properties = new Properties();
		PropertiesUtils.set(properties, "desq.mining.miner.class", DesqCountWithPruning.class.getCanonicalName());
		PropertiesUtils.set(properties, "patternExpression", patternExpression);
		PropertiesUtils.set(properties, "minSupport", sigma);
		return properties;
	}

	@Override
	protected void addInputSequence(IntList inputSequence) {
		if (eDfa.isRelevant(inputSequence, 0, 0)) {
			this.inputSequence = inputSequence;
			step(0, initialStateId);
			sid++;
		}
	}

	@Override
	public void mine() {
		for (Map.Entry<IntList, Long> entry : outputSequences.entrySet()) {
			long value = entry.getValue();
			int support = PrimitiveUtils.getLeft(value);
			if (support >= sigma) {
				if (ctx.patternWriter != null)
					ctx.patternWriter.write(entry.getKey(), support);
			}
		}
	}

	private void step(int pos, int stateId) {

		if (fst.getState(stateId).isFinal()) {
			if (!buffer.isEmpty())
				countSequence(buffer);
		}
		if (pos == inputSequence.size())
			return;

		int itemFid = inputSequence.getInt(pos); // current input item in the
													// sequence

		// reuse iterators or create a new one
		Iterator<ItemState> itemStateIt;
		if(pos >= itemStateIterators.size()) {
			itemStateIt = fst.getState(stateId).consume(itemFid);
			itemStateIterators.add(itemStateIt);
		} else {
			itemStateIt = fst.getState(stateId).consume(itemFid, itemStateIterators.get(pos));
		}
		
		while (itemStateIt.hasNext()) {
			ItemState itemState = itemStateIt.next();
			int outputItemFid = itemState.itemFid;

			int toStateId = itemState.state.getId();
			if (outputItemFid == 0) { // EPS output
				step(pos + 1, toStateId);
			} else {
				if (!useFlist || largestFrequentFid >= outputItemFid) {
					buffer.add(outputItemFid);
					step(pos + 1, toStateId);
					buffer.remove(buffer.size() - 1);
				}
			}
		}
	}

	private void countSequence(IntList sequence) {
		Long supSid = outputSequences.get(sequence);
		if (supSid == null) {
			outputSequences.put(new IntArrayList(sequence), PrimitiveUtils.combine(1, sid)); // need
																								// to
																								// copy
																								// here
			return;
		}
		if (PrimitiveUtils.getRight(supSid) != sid) {
			// TODO: can overflow
			// if chang order: newCount = count + 1 // no combine
			int newCount = PrimitiveUtils.getLeft(supSid) + 1;
			outputSequences.put(sequence, PrimitiveUtils.combine(newCount, sid));
		}
	}

}
