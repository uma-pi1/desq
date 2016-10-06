package old.journal.mining;

import de.uni_mannheim.desq.fst.*;
import de.uni_mannheim.desq.mining.DesqMiner;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import de.uni_mannheim.desq.patex.PatEx;
import de.uni_mannheim.desq.util.PrimitiveUtils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import old.journal.PropertiesUtils;

import java.util.BitSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * DesqCountTwoPass.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */


// TODO: integrate into DesqCount
public class DesqCountIterativeTwoPass extends DesqMiner {

	
	// parameters for old.mining
	String patternExpression;
	long sigma;
	boolean useFlist = true;
	
	// helper variables
	int largestFrequentFid;
	Fst fst;
	int sid;
	IntList buffer;
	IntList inputSequence;
	Object2LongMap<IntList> outputSequences = new Object2LongOpenHashMap<>();
	Iterator<ItemState> itemStateIt = null;
	ExtendedDfa eDfa;
	
	
	// parallel arrays for iteratively simulating old.fst using a stack
	IntList stateIdList;
	IntList posList;
	IntList suffixIdList;
	IntList prefixPointerList;	
	
	
	// variables for two pass
	BitSet[] posStateIndex;
	IntList finalPos;
	IntList finalStateIds;
	BitSet initBitSet = new BitSet(1);
	
	public DesqCountIterativeTwoPass(DesqMinerContext ctx) {
		super(ctx);
		this.sigma = ctx.conf.getLong("minSupport");
		this.useFlist = ctx.conf.getBoolean("useFlist", true);
		this.largestFrequentFid = ctx.dict.getLargestFidAboveDfreq(sigma);
		this.sid = 0;
		
		this.patternExpression = ctx.conf.getString("patternExpression");
		patternExpression = patternExpression.trim();
		PatEx p = new PatEx(patternExpression, ctx.dict);
		this.fst = p.translate();
		fst.minimize();// TODO: move to translate
		
		buffer = new IntArrayList();
		stateIdList = new IntArrayList();
		posList = new IntArrayList();
		suffixIdList = new IntArrayList();
		prefixPointerList = new IntArrayList();
		
		this.eDfa = new ExtendedDfa(fst, ctx.dict);
		finalPos = new IntArrayList();
		
		finalStateIds = new IntArrayList();
		for(State state : fst.getFinalStates()) {
			finalStateIds.add(state.getId());
		}
		//create reverse old.fst
		fst.reverse(false);
		
		//TODO: this always assumes that initialStateId is 0!
		initBitSet.set(0);
	}

	public static Properties createProperties(String patternExpression, int sigma) {
		Properties properties = new Properties();
		PropertiesUtils.set(properties, "desq.mining.miner.class", DesqCountIterativeTwoPass.class.getCanonicalName());
		PropertiesUtils.set(properties, "patternExpression", patternExpression);
		PropertiesUtils.set(properties, "minSupport", sigma);
		return properties;
	}
	
	private void clear() {
		finalPos.clear();
	}
	
	private void clearStack() {
		stateIdList.clear();
		posList.clear();
		suffixIdList.clear();
		prefixPointerList.clear();		
	}
	
	
	@Override
	protected void addInputSequence(IntList inputSequence) {
		// Make the forward pass to compute reachability
		posStateIndex = new BitSet[inputSequence.size() + 1];
		posStateIndex[0] = initBitSet;
		if(eDfa.computeReachability(inputSequence, 0, posStateIndex, finalPos)) {
			this.inputSequence = inputSequence;
			for(int pos : finalPos) {
				for(int stateId : finalStateIds) {
					if(posStateIndex[pos+1].get(stateId)) {
						/*stateIdList.add(stateId);
						posList.add(pos);
						suffixIdList.add(0);
						prefixPointerList.add(-1);*/
						addToStack(pos, 0, stateId, -1);
						stepIteratively();
						clearStack();
					}
				}
			}
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
			
			for(Transition transition : fst.getState(fromStateId).getTransitions()) {
				toStateId = transition.getToState().getId();
				if(transition.matches(itemFid) && posStateIndex[pos].get(toStateId)) {
					itemStateIt = transition.consume(itemFid, itemStateIt);
					while(itemStateIt.hasNext()) {
						outputItemFid = itemStateIt.next().itemFid;
						if(largestFrequentFid >= outputItemFid) {
							addToStack(pos-1, outputItemFid, toStateId, currentStackIndex);
						}
					}
				}
			}
			currentStackIndex++;
		}
	}
	
	private void addToStack(int pos, int outputItemFid, int toStateId, int prefixPointerIndex) {
		if(pos < 0) {
			// We will always reach inital state after consuming the input (two pass correctness)
			//We assume that toStateId is zero
			//TODO: better to check posStateIndex[pos+1].get(toStateId)
			computeOutput(outputItemFid, prefixPointerIndex);
			return;
		}
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
		if(!buffer.isEmpty()) {
			countSequence(buffer);
			buffer.clear();
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
