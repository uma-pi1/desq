package de.uni_mannheim.desq.journal.mining;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.ItemState;
import de.uni_mannheim.desq.fst.State;
import de.uni_mannheim.desq.fst.Transition;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.journal.edfa.ExtendedDfa;
import de.uni_mannheim.desq.mining.DesqMinerContext;
import de.uni_mannheim.desq.mining.MemoryDesqMiner;
import de.uni_mannheim.desq.mining.OldPostingList;
import de.uni_mannheim.desq.patex.PatEx;
import de.uni_mannheim.desq.util.PropertiesUtils;

/**
 * DesqDfsTwoPass.java
 * 
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
@SuppressWarnings("deprecation")

// TODO: integrate into DesqDfs
public class DesqDfsTwoPass extends MemoryDesqMiner {

	// parameters for mining
	String patternExpression;
	long sigma;

	// Automaton
	Fst fst;
	ExtendedDfa eDfa;

	// helper variables
	int largestFrequentFid;
	int[] inputSequence;
	boolean reachedFinalState;
	IntList outputSequence;
	Node currentNode;
	
	// variables for twopass
	List<BitSet[]> posStateIndexList;
	List<int[]> finalPosList;
	IntList finalPos;
	IntList finalStateIds;
	BitSet initBitSet = new BitSet(1);
	ArrayList<Iterator<ItemState>> itemStateIterators = new ArrayList<>();
	
	public DesqDfsTwoPass(DesqMinerContext ctx) {
		super(ctx);
		this.patternExpression = ctx.conf.getString("patternExpression");
		this.sigma = ctx.conf.getLong("minSupport");
		this.largestFrequentFid = ctx.dict.getLargestFidAboveDfreq(sigma);
		
		// fst
		patternExpression = ".* [" + patternExpression.trim() + "]";
		PatEx p = new PatEx(patternExpression, ctx.dict);
		this.fst = p.translate();
		fst.minimize(); //TODO: move to translate
		
		// extendedDfa
		this.eDfa = new ExtendedDfa(fst, ctx.dict);
		
		outputSequence = new IntArrayList();
		
		posStateIndexList = new ArrayList<>();
		finalStateIds = new IntArrayList();
		for(State state : fst.getFinalStates()) {
			finalStateIds.add(state.getId());
		}
		finalPosList = new ArrayList<>();
		finalPos = new IntArrayList();
		initBitSet.set(0); //TODO: assumeds that initialStateId is 0
		
		// reverse fst
		fst.reverse(false);
	}
	
	public static Properties createProperties(String patternExpression, int sigma) {
		Properties properties = new Properties();
		PropertiesUtils.set(properties, "patternExpression", patternExpression);
		PropertiesUtils.set(properties, "minSupport", sigma);
		return properties;
	}
	
	public void clear() {
		inputSequences.clear();
		posStateIndexList.clear();
		finalPosList.clear();
        finalStateIds.clear();
		finalPos.clear();
	}

	@Override
	public void addInputSequences(SequenceReader in) throws IOException {
		IntList inputSequence = new IntArrayList();
		while(in.readAsFids(inputSequence)) {
			BitSet[] posStateIndex = new BitSet[inputSequence.size()+1];
			posStateIndex[0] = initBitSet;
			if(eDfa.computeReachability(inputSequence, 0, posStateIndex, finalPos)) {
				addInputSequence(inputSequence);
				posStateIndexList.add(posStateIndex);
				//TODO: directly do the first incStep to avoid copying finalPos
				finalPosList.add(finalPos.toIntArray());
				finalPos.clear();
			} 
		}
	}

	@Override
	public void mine() {
		Node root = new Node(null, 0);
		currentNode = root;
		for(int sid = 0; sid < inputSequences.size(); ++sid) {
			inputSequence = inputSequences.get(sid);
			for(int pos : finalPosList.get(sid)) {
				for(int stateId : finalStateIds) {
					if(posStateIndexList.get(sid)[pos+1].get(stateId)) {
						// We increment position here
						// to avoid adding 0 when reaching
						// initial state
						incStep(sid, pos+1, stateId, 0);
					}
				}
			}
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

			// increment support if atleast one snapshop corresponds to final
			// state
			if (reachedFinalState) {
				support++;
			}

		} while (projectedDatabase.nextPosting());

		// Output if P-frequent
		if (support >= sigma) {
			if (ctx.patternWriter != null) {
				outputSequence.add(node.suffixItemId);
				Node parent = node.parent;
				while (parent.parent != null) {
					outputSequence.add(parent.suffixItemId);
					parent = parent.parent;
				}
				ctx.patternWriter.write(outputSequence, support);
				outputSequence.clear();
				// System.out.println(outputSequence + " : " + support);
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
	}
	
	private void incStep(int sid, int pos, int stateId, int level) {
		if(pos == 0) {
			// We consumed entire input in reverse
			// we must have reached inital state
			// (twopass correctness)
			reachedFinalState = true;
			return;
		}
		
		// we use pos-1; because we incremented pos initially
		// see when incStep is called for the first time
		int itemFid = inputSequence[pos-1];
		
		
		
		
		
		BitSet[] posStateIndex = posStateIndexList.get(sid);
		for(Transition transition : fst.getState(stateId).getTransitions()) {
			int toStateId = transition.getToState().getId();
			if(transition.matches(itemFid) && posStateIndex[pos-1].get(toStateId)) {
				
				// create a new iterator or reuse existing one
				Iterator<ItemState> itemStateIt;
				if(level>=itemStateIterators.size()) {
					itemStateIt = transition.consume(itemFid);
					itemStateIterators.add(itemStateIt);
				} else {
					itemStateIt = transition.consume(itemFid, itemStateIterators.get(level));
				}
				while(itemStateIt.hasNext()) {
					int outputItemFid = itemStateIt.next().itemFid;
					if(outputItemFid == 0) { //EPS output
						incStep(sid, pos-1, toStateId, level + 1);
					} else {
						if(largestFrequentFid >= outputItemFid) {
							currentNode.append(outputItemFid, sid, pos-1, toStateId);
						}
					}
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
