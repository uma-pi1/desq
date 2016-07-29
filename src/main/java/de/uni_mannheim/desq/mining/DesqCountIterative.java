package de.uni_mannheim.desq.mining;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;


import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.ItemState;
import de.uni_mannheim.desq.patex.PatEx;
import de.uni_mannheim.desq.util.PrimitiveUtils;
import de.uni_mannheim.desq.util.PropertiesUtils;

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
		int numFstStates;
		
		//helpers for iteratively simulating fst
		ObjectArrayList<Node>[] statePrefix;
		int[] stateList;
		int stateListSize = 0;
		int pos = 0;


		@SuppressWarnings("unchecked")
		public DesqCountIterative(DesqMinerContext ctx) {
			super(ctx);
			this.sigma = PropertiesUtils.getLong(ctx.properties, "minSupport");
			if(PropertiesUtils.isSet(ctx.properties, "useFlist"))
				this.useFlist = PropertiesUtils.getBoolean(ctx.properties, "useFlist");
			this.largestFrequentFid = ctx.dict.getLargestFidAboveDfreq(sigma);
			this.sid = 0;
			
			this.patternExpression = PropertiesUtils.get(ctx.properties, "patternExpression");
			patternExpression = ".* [" + patternExpression.trim() + "]";
			PatEx p = new PatEx(patternExpression, ctx.dict);
			this.fst = p.translate();
			fst.minimize();//TODO: move to translate
			this.initialStateId = fst.getInitialState().getId();
			
			buffer = new IntArrayList();
			this.numFstStates = fst.numStates();
			
			statePrefix = (ObjectArrayList<Node>[]) new ObjectArrayList[numFstStates];
			stateList = new int[numFstStates];
		}

		public static Properties createProperties(String patternExpression, int sigma) {
			Properties properties = new Properties();
			PropertiesUtils.set(properties, "patternExpression", patternExpression);
			PropertiesUtils.set(properties, "minSupport", sigma);
			return properties;
		}
		
		private void reset() {
			pos = 0;
			for(int i = 0; i < numFstStates; i++) {
				statePrefix[i] = null;
			}
			stateListSize = 0;
		}
		
		@Override
		protected void addInputSequence(IntList inputSequence) {
			this.inputSequence = inputSequence;
			statePrefix[initialStateId] = new ObjectArrayList<Node>();
			stateList[stateListSize++] = initialStateId;
			statePrefix[initialStateId].add(null);
			while(pos < inputSequence.size()) {
				stepIteratively();
				pos++;
			}
			sid++;
			reset();
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
		
		Iterator<ItemState> itemStateIt = null;
		
		private void stepIteratively() {
			@SuppressWarnings("unchecked")
			ObjectArrayList<Node>[] nextStatePrefix = (ObjectArrayList<Node>[]) new ObjectArrayList[numFstStates];
			int[] nextStateList = new int[numFstStates];
			int nextStateListSize = 0;
			
			int itemFid = inputSequence.getInt(pos);
			for(int i = 0; i < stateListSize; i++) {
				int fromStateId = stateList[i];
				itemStateIt = fst.getState(fromStateId).consume(itemFid, itemStateIt);
				while(itemStateIt.hasNext()) {
					ItemState itemState = itemStateIt.next();
					int outputItemFid = itemState.itemFid;
					int toStateId = itemState.state.getId();
					
					boolean isFinal = fst.getState(toStateId).isFinal();
					if(null == nextStatePrefix[toStateId]) {
						nextStatePrefix[toStateId] = new ObjectArrayList<>();
						nextStateList[nextStateListSize++] = toStateId;
					}
					if(outputItemFid == 0) { // EPS output
						if(isFinal)
							computeOutput(statePrefix[fromStateId]);
						for(Node node : statePrefix[fromStateId])
							nextStatePrefix[toStateId].add(node);
					} else {
						if(!useFlist || largestFrequentFid >= outputItemFid) {
							Node node = new Node(outputItemFid, statePrefix[fromStateId]);
							if(isFinal)
								computeOutput(node);
							nextStatePrefix[toStateId].add(node);
						}
					}
				}
			}
			this.stateList = nextStateList;
			this.stateListSize = nextStateListSize;
			this.statePrefix = nextStatePrefix;
		}
		
		private void computeOutput(ObjectArrayList<Node> suffixes) {
			for(Node node : suffixes)
				computeOutput(node);
		}
		
		private void computeOutput(Node node) {
			if(node == null) {
				outputBuffer();
				return;
			}
			buffer.add(node.item);
			for(Node m : node.prefixes) {
				computeOutput(m);
			}
			buffer.remove(buffer.size()-1);
		}
		
		private void outputBuffer() {
			if(!buffer.isEmpty()) {
				reverse(buffer);
				countSequence(buffer);
				reverse(buffer);
			}
		}
		
		private void reverse(IntList a) {
			int i = 0;
			int j = a.size() - 1;
			while(j > i) {
				a.set(i, (a.getInt(i)^a.getInt(j)));
				a.set(j, (a.getInt(j)^a.getInt(i)));
				a.set(i, (a.getInt(i)^a.getInt(j)));
				i++;j--;
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
		
		class Node {
			int item;
			ObjectArrayList<Node> prefixes;

			Node(int item, ObjectArrayList<Node> suffixes) {
				this.item = item;
				this.prefixes = suffixes;
			}
		}
		

		
}
