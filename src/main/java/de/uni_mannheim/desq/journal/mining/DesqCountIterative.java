package de.uni_mannheim.desq.journal.mining;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.ItemState;
import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import de.uni_mannheim.desq.patex.PatEx;
import de.uni_mannheim.desq.util.PrimitiveUtils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;

import java.util.Iterator;
import java.util.Map;

// TODO: already integrated into DesqCount
public class DesqCountIterative extends DesqMiner {

	// parameters for mining
	String patternExpression;
	long sigma;
	boolean useFlist = true;

	// helper variables
	int largestFrequentFid;
	Fst fst;
	int initialStateId;
	int sid;
	IntList buffer;
	IntList inputSequence;
	Object2LongMap<IntList> outputSequences = new Object2LongOpenHashMap<>();
	Iterator<ItemState> itemStateIt = null;

	// parallel arrays for iteratively simulating fst using a stack
	IntList stateIdList;
	IntList posList;
	IntList suffixIdList;
	IntList prefixPointerList;

	// int currentStackIndex = 0;

	public DesqCountIterative(DesqMinerContext ctx) {
		super(ctx);
		this.sigma = ctx.conf.getLong("desq.mining.min.support");
		this.useFlist = ctx.conf.getBoolean("desq.mining.use.flist");
		this.largestFrequentFid = ctx.dict.getLargestFidAboveDfreq(sigma);
		this.sid = 0;

		this.patternExpression = ctx.conf.getString("desq.mining.pattern.expression");
		patternExpression = ".* [" + patternExpression.trim() + "]";
		PatEx p = new PatEx(patternExpression, ctx.dict);
		this.fst = p.translate();
		fst.minimize();// TODO: move to translate
		this.initialStateId = fst.getInitialState().getId();

		buffer = new IntArrayList();

		stateIdList = new IntArrayList();
		posList = new IntArrayList();
		suffixIdList = new IntArrayList();
		prefixPointerList = new IntArrayList();
	}

	public static Configuration createConf(String patternExpression, long sigma) {
		PropertiesConfiguration conf = new PropertiesConfiguration();
		conf.setThrowExceptionOnMissing(true);
		conf.setProperty("desq.mining.miner.class", DesqCountIterative.class.getCanonicalName());
		conf.setProperty("desq.mining.min.support", sigma);
		conf.setProperty("desq.mining.pattern.expression", patternExpression);
		conf.setProperty("desq.mining.use.flist", true);
		return conf;
	}

	private void clear() {
		stateIdList.clear();
		posList.clear();
		suffixIdList.clear();
		prefixPointerList.clear();
	}

	@Override
	protected void addInputSequence(IntList inputSequence) {
		if (!inputSequence.isEmpty()) {
			this.inputSequence = inputSequence;
			/*stateIdList.add(initialStateId);
			posList.add(0);
			suffixIdList.add(0);
			prefixPointerList.add(-1);*/
			addToStack(0, 0, initialStateId, false, -1);
			stepIteratively();
			sid++;
			clear();
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

	private void stepIteratively() {

		int pos;// position of next input item
		int itemFid; // next input item
		int fromStateId; // current state
		int toStateId; // next state
		int outputItemFid; // output item
		int currentStackIndex = 0;

		while (currentStackIndex < stateIdList.size()) {
			pos = posList.getInt(currentStackIndex);
			itemFid = inputSequence.getInt(pos);
			fromStateId = stateIdList.getInt(currentStackIndex);

			itemStateIt = fst.getState(fromStateId).consume(itemFid, itemStateIt);
			while (itemStateIt.hasNext()) {
				ItemState itemState = itemStateIt.next();
				outputItemFid = itemState.itemFid;
				toStateId = itemState.state.getId();

				boolean isFinal = fst.getState(toStateId).isFinal();

				if (outputItemFid == 0) { // EPS output
					addToStack(pos + 1, outputItemFid, toStateId, isFinal, currentStackIndex);
				} else {
					if (!useFlist || largestFrequentFid >= outputItemFid) {
						addToStack(pos + 1, outputItemFid, toStateId, isFinal, currentStackIndex);
					}
				}
			}
			currentStackIndex++;
		}

	}

	private void addToStack(int pos, int outputItemFid, int toStateId, boolean isFinal, int prefixPointerIndex) {
		if (isFinal) {
			computeOutput(outputItemFid, prefixPointerIndex);
		}
		if (pos == inputSequence.size())
			return;
		stateIdList.add(toStateId);
		posList.add(pos);
		suffixIdList.add(outputItemFid);
		prefixPointerList.add(prefixPointerIndex);
	}

	private void computeOutput(int outputItemFid, int prefixPointerIndex) {
		if (outputItemFid > 0)
			buffer.add(outputItemFid);
		while (prefixPointerIndex > 0) {
			if (suffixIdList.getInt(prefixPointerIndex) > 0) {
				buffer.add(suffixIdList.getInt(prefixPointerIndex));
			}
			prefixPointerIndex = prefixPointerList.getInt(prefixPointerIndex);
		}
		if (!buffer.isEmpty()) {
			reverse(buffer);
			countSequence(buffer);
			buffer.clear();
		}
	}

	private void reverse(IntList a) {
		int i = 0;
		int j = a.size() - 1;
		while (j > i) {
			a.set(i, (a.getInt(i) ^ a.getInt(j)));
			a.set(j, (a.getInt(j) ^ a.getInt(i)));
			a.set(i, (a.getInt(i) ^ a.getInt(j)));
			i++;
			j--;
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
