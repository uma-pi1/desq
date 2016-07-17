package de.uni_mannheim.desq.mining;

import java.util.Iterator;
import java.util.Map;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.ItemState;
import de.uni_mannheim.desq.fst.Transition;
import de.uni_mannheim.desq.utils.IntArrayStrategy;
import de.uni_mannheim.desq.utils.PrimitiveUtils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2LongOpenCustomHashMap;

public class DesqCount extends DesqMiner {
	
	// parameters for mining
	Fst fst;
	int sigma;
	boolean useFlist = true;
	
	
	// helper variables
	int initialStateId;
	int sid;
	int[] flist;
	IntList buffer;
	IntList inputSequence;
	Object2LongOpenCustomHashMap<int[]> outputSequences;

	public DesqCount(DesqMinerContext ctx, boolean useFlist) {
		super(ctx);
		// TODO Auto-generated constructor stub
		this.fst = ctx.fst;
		this.sigma = ctx.sigma;
		this.flist = ctx.dict.getFlist().toIntArray();
		buffer = new IntArrayList();
		outputSequences = new Object2LongOpenCustomHashMap<int[]>(
				new IntArrayStrategy());
		this.sid = 0;
		this.initialStateId = ctx.fst.getInitialState().getStateId();
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
		for(Map.Entry<int[], Long> entry : outputSequences.entrySet()) {
			long value = entry.getValue();
			int support = PrimitiveUtils.getLeft(value);
			if(support >= sigma) {
				if (ctx.patternWriter != null) 
					ctx.patternWriter.write(new IntArrayList(entry.getKey()), support);
			}
		}
	}

	private void step(int pos, int stateId) {
		
		if(fst.getState(stateId).isFinal()) {
			if(!buffer.isEmpty())
				countSequence(buffer.toIntArray());
		}
		if(pos == inputSequence.size())
			return;
		
		int itemFid = inputSequence.getInt(pos); // current input item in the sequence
		
		Iterator<Transition> it = fst.getState(stateId).consume(itemFid);
		while(it.hasNext()) {
			Transition t = it.next();
			if(t.matches(itemFid)) {
				Iterator<ItemState> is = t.consume(itemFid);
				while(is.hasNext()) {
					ItemState itemState = is.next();
					int outputItemFid = itemState.itemFid;
					
					int toStateId = itemState.state.getStateId();
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
		}
	}

	private void countSequence(int[] sequence) {
		Long supSid = outputSequences.get(sequence);
		if (supSid == null) {
			outputSequences.put(sequence, PrimitiveUtils.combine(1, sid));
			return;
		}
		if (PrimitiveUtils.getRight(supSid) != sid) {
			int newCount = PrimitiveUtils.getLeft(supSid) + 1;
			outputSequences.put(sequence, PrimitiveUtils.combine(newCount, sid));
		}
	}

}
