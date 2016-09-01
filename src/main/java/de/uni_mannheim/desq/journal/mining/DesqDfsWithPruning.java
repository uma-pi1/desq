package de.uni_mannheim.desq.journal.mining;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Properties;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;
import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.ItemState;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.journal.edfa.ExtendedDfa;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import de.uni_mannheim.desq.mining.MemoryDesqMiner;
import de.uni_mannheim.desq.mining.OldPostingList;
import de.uni_mannheim.desq.patex.PatEx;
import de.uni_mannheim.desq.util.PropertiesUtils;

@SuppressWarnings("deprecation")

// TODO: integrate into DesqDfs
public class DesqDfsWithPruning extends MemoryDesqMiner {

	// parameters for mining
	long sigma;
	String patternExpression;
	
	// Automaton
	Fst fst;
	ExtendedDfa eDfa;
	
	// helper variables
	int largestFrequentFid;
	int[] inputSequence;
	boolean reachedFinalState;
	int dfsLevel = 0;
	IntList outputSequence = new IntArrayList();
	Node currentNode;
	ArrayList<Iterator<ItemState>> itemStateIterators = new ArrayList<>();
	
	public DesqDfsWithPruning(DesqMinerContext ctx) {
		super(ctx);
		this.patternExpression = PropertiesUtils.get(ctx.properties, "patternExpression");
		this.sigma = PropertiesUtils.getLong(ctx.properties, "minSupport");
		
		// fst
		patternExpression = ".* [" + patternExpression.trim() + "]";
		PatEx p = new PatEx(patternExpression, ctx.dict);
		this.fst = p.translate();
		fst.minimize(); //TODO: move to translate
		
		// extendedDfa
		this.eDfa = new ExtendedDfa(fst, ctx.dict);
		
		this.largestFrequentFid = ctx.dict.getLargestFidAboveDfreq(sigma);
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
	public void addInputSequences(SequenceReader in) throws IOException {
		IntList inputSequence = new IntArrayList();
		while(in.readAsFids(inputSequence)) {
			if(eDfa.isRelevant(inputSequence, 0, 0)) {
				addInputSequence(inputSequence);
			}
		}
	}
	

	@Override
	public void mine() {
		Node root = new Node(null, 0);
		currentNode = root;
		for(int sid = 0; sid < inputSequences.size(); ++sid) {
			inputSequence = inputSequences.get(sid);
			incStep(sid, 0, fst.getInitialState().getId(), 0);
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
		currentNode = node;
		
		dfsLevel++;
		int support = 0;
		OldPostingList.Decompressor projectedDatabase = new OldPostingList.Decompressor(node.projectedDatabase);
	
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
				incStep(sid, pos, stateId, 0);

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
	
	private void incStep(int sid, int pos, int stateId, int level) {
		reachedFinalState |= fst.getState(stateId).isFinal();
		if(pos == inputSequence.length)
			return;
		int itemFid = inputSequence[pos];
		
		
		// get new iterator or reuse existing one
		Iterator<ItemState> itemStateIt;
		if (level >= itemStateIterators.size()) {
			itemStateIt = fst.getState(stateId).consume(itemFid);
			itemStateIterators.add(itemStateIt);
		} else {
			itemStateIt = fst.getState(stateId).consume(itemFid, itemStateIterators.get(level));
		}
		
        while(itemStateIt.hasNext()) {
			ItemState itemState = itemStateIt.next();
			int outputItemFid = itemState.itemFid;
					
			int toStateId = itemState.state.getId();
			if(outputItemFid == 0) { //EPS output
				incStep(sid, pos + 1, toStateId, level + 1);
			} else {
				if(largestFrequentFid >= outputItemFid) {
					currentNode.append(outputItemFid, sid, pos + 1, toStateId);
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
						OldPostingList.addCompressed(0, node.projectedDatabase);
					}

					node.lastSequenceId = sequenceId;
					node.prefixSupport++;

					OldPostingList.addCompressed(sequenceId + 1, node.projectedDatabase);
					OldPostingList.addCompressed(state + 1, node.projectedDatabase);
					OldPostingList.addCompressed(position + 1, node.projectedDatabase);

					node.statePosSet[state].set(position);
				} else if (!node.statePosSet[state].get(position)) {
					node.statePosSet[state].set(position);
					OldPostingList.addCompressed(state + 1, node.projectedDatabase);
					OldPostingList.addCompressed(position + 1, node.projectedDatabase);
				}
			}

			void clear() {
				projectedDatabase = null;
				statePosSet = null;
				children = null;
			}
		}

}
