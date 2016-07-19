package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.patex.PatEx;
import de.uni_mannheim.desq.util.PropertiesUtils;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.*;

import java.util.BitSet;
import java.util.Iterator;
import java.util.Properties;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.ItemState;
import de.uni_mannheim.desq.fst.Transition;

public class DesqDfs extends MemoryDesqMiner {

	
	// parameters for mining
	//Fst fst;
	long sigma;
	String patternExpression;
	
	// helper variables
	Fst fst;
	//int[] flist;
	int largestFrequentFid;
	int[] inputSequence;
	boolean reachedFinalState;
	int dfsLevel = 0;
    IntList outputSequence = new IntArrayList();

	public DesqDfs(DesqMinerContext ctx) {
		super(ctx);
		this.patternExpression = PropertiesUtils.get(ctx.properties, "patternExpression");
		this.sigma = PropertiesUtils.getLong(ctx.properties, "minSupport");
		//this.flist = ctx.dict.getFlist().toIntArray();
		this.largestFrequentFid = ctx.dict.getLargestFidAboveDfreq(sigma);
		
		patternExpression = ".* [" + patternExpression.trim() + "]";
		PatEx p = new PatEx(patternExpression, ctx.dict);
		this.fst = p.translate();
		fst.minimize();//TODO: move to translate
	}
	
	public static Properties createProperties(String patternExpression, int sigma) {
		Properties properties = new Properties();
		PropertiesUtils.set(properties, "patternExpression", patternExpression);
		PropertiesUtils.set(properties, "minSupport", sigma);
		return properties;
	}
	
	public void clear() {
		inputSequences.clear();
	}

	@Override
	public void mine() {
		Node root = new Node(null, 0);
		for(int sid = 0; sid < inputSequences.size(); ++sid) {
			inputSequence = inputSequences.get(sid);
			incStep(sid, 0, fst.getInitialState().getId(), root);
		}
		
		final IntIterator it = root.children.keySet().iterator();
		while(it.hasNext()) {
			int itemId = it.nextInt();
			Node child = root.children.get(itemId);
			if(child.prefixSupport >= sigma) {
				expand(child);
			}
			child.clear();
		}
		root.clear();
		
		clear();
	}
	
	private void expand(Node node) {
		dfsLevel++;
		int support = 0;
		PostingList.Decompressor projectedDatabase = new PostingList.Decompressor(node.projectedDatabase);
	
		// For all sequences in projected database
		do {
			int sid = projectedDatabase.nextValue();
			inputSequence = inputSequences.get(sid);
			reachedFinalState = false;

			// For all state@pos for a sequence
			do {
				int stateId = projectedDatabase.nextValue();
				int pos = projectedDatabase.nextValue();
				
				// for each T[pos@state]
				incStep(sid, pos, stateId, node);

			} while (projectedDatabase.hasNextValue());

			// increment support if atleast one snapshop corresponds to final state
			if (reachedFinalState) {
				support++;
			}

		} while (projectedDatabase.nextPosting());
		
		// Output if P-frequent
		if (support >= sigma) {
			
			if (ctx.patternWriter != null) {
				// compute output sequence
                outputSequence.size(dfsLevel);
				int size = dfsLevel;
				
				outputSequence.set(--size, node.suffixItemId);
				Node parent = node.parent;
				while(parent.parent != null) {
					outputSequence.set(--size, parent.suffixItemId);
					parent = parent.parent;
				}
				ctx.patternWriter.write(outputSequence, support);
				//System.out.println(outputSequence + " : " + support);
			}
		}
		
		// Expand children with sufficient prefix support
		final IntIterator it = node.children.keySet().iterator();
		while (it.hasNext()) {
			int itemId = it.nextInt();
			Node child = node.children.get(itemId);
			if (child.prefixSupport >= sigma) {
				expand(child);
			}
			child.clear();
		}
		node.clear();

		dfsLevel--;
	}
	
	private void incStep(int sid, int pos, int stateId, Node node) {
		reachedFinalState |= fst.getState(stateId).isFinal();
		if(pos == inputSequence.length)
			return;
		int itemFid = inputSequence[pos];
		
		//TODO: reuse iterators!
        Iterator<ItemState> itemStateIt = fst.getState(stateId).consume(itemFid);
        while(itemStateIt.hasNext()) {
			ItemState itemState = itemStateIt.next();
			int outputItemFid = itemState.itemFid;
					
			int toStateId = itemState.state.getId();
			if(outputItemFid == 0) { //EPS output
				incStep(sid, pos + 1, toStateId, node);
			} else {
				if(largestFrequentFid >= outputItemFid) {
					node.append(outputItemFid, sid, pos + 1, toStateId);
				}
			}
		}
	}
	
	// Dfs tree
		private final class Node {
			int prefixSupport = 0;
			int lastSequenceId = -1;
			int suffixItemId;
			Node parent;
			ByteArrayList projectedDatabase = new ByteArrayList();
			BitSet[] statePosSet = new BitSet[fst.numStates()];
			Int2ObjectMap<Node> children = new Int2ObjectOpenHashMap<>();

			Node(Node parent, int suffixItemId) {
				this.parent = parent;
				this.suffixItemId = suffixItemId;

				for (int i = 0; i < fst.numStates(); ++i) {
					statePosSet[i] = new BitSet();
				}
			}

			void flush() {
				for (int i = 0; i < fst.numStates(); ++i) {
					statePosSet[i].clear();
				}
			}

			void append(int itemId, int sequenceId, int position, int state) {
				Node node = children.get(itemId);

				if (node == null) {
					node = new Node(this, itemId);
					children.put(itemId, node);
				}

				if (node.lastSequenceId != sequenceId) {

					if (node.lastSequenceId != -1)
						node.flush();

					/** Add transaction separator */
					if (node.projectedDatabase.size() > 0) {
						PostingList.addCompressed(0, node.projectedDatabase);
					}

					node.lastSequenceId = sequenceId;
					node.prefixSupport++;

					PostingList.addCompressed(sequenceId + 1, node.projectedDatabase);
					PostingList.addCompressed(state + 1, node.projectedDatabase);
					PostingList.addCompressed(position + 1, node.projectedDatabase);

					node.statePosSet[state].set(position);
				} else if (!node.statePosSet[state].get(position)) {
					node.statePosSet[state].set(position);
					PostingList.addCompressed(state + 1, node.projectedDatabase);
					PostingList.addCompressed(position + 1, node.projectedDatabase);
				}
			}

			void clear() {
				projectedDatabase = null;
				statePosSet = null;
				children = null;
			}
		}
}
