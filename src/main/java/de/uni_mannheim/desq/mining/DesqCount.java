package de.uni_mannheim.desq.mining;

import java.util.Iterator;
import java.util.Map;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.ItemState;
import de.uni_mannheim.desq.fst.Transition;
import de.uni_mannheim.desq.util.IntArrayStrategy;
import de.uni_mannheim.desq.util.PrimitiveUtils;
import de.uni_mannheim.desq.util.PropertiesUtils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenCustomHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

public class DesqCount extends DesqMiner {
	
	// parameters for mining
	Fst fst;
	long sigma;
	boolean useFlist = true;
	
	
	// helper variables
	int initialStateId;
	int sid;
	int[] flist;
	IntList buffer;
	IntList inputSequence;
	Object2LongMap<IntList> outputSequences = new Object2LongOpenHashMap<>();

	public DesqCount(DesqMinerContext ctx, boolean useFlist) {
		super(ctx);
		// TODO Auto-generated constructor stub
		this.fst = ctx.fst;
		this.sigma = PropertiesUtils.getLong(ctx.properties, "minSupport");
		this.flist = ctx.dict.getFlist().toIntArray();
		buffer = new IntArrayList();
//		outputSequences =
//				new IntArrayStrategy());
		this.sid = 0;
		this.initialStateId = ctx.fst.getInitialState().getId();
		this.useFlist = useFlist;
	}

	@Override
	protected void addInputSequence(IntList inputSequence) {
		this.inputSequence = inputSequence;
		step(0, initialStateId);
		sid++;
	}

	@Override
	public void mine() {
		for(Map.Entry<IntList, Long> entry : outputSequences.entrySet()) {
			long value = entry.getValue();
			int support = PrimitiveUtils.getLeft(value);
			if(support >= sigma) {
				if (ctx.patternWriter != null) 
					ctx.patternWriter.write(entry.getKey(), support);
			}
		}
	}

	private void step(int pos, int stateId) {
		
		if(fst.getState(stateId).isFinal()) {
			if(!buffer.isEmpty())
				countSequence(buffer);
		}
		if(pos == inputSequence.size())
			return;
		
		int itemFid = inputSequence.getInt(pos); // current input item in the sequence

        //TODO: reuse iterators!
        Iterator<ItemState> itemStateIt = fst.getState(stateId).consume(itemFid);
		while(itemStateIt.hasNext()) {
            ItemState itemState = itemStateIt.next();
            int outputItemFid = itemState.itemFid;
					
			int toStateId = itemState.state.getId();
			if(outputItemFid == 0) { //EPS output
				step(pos + 1, toStateId);
			} else {
				if(!useFlist || flist[outputItemFid] >= sigma) {
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
			outputSequences.put(new IntArrayList(sequence), PrimitiveUtils.combine(1, sid)); // need to copy here
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
