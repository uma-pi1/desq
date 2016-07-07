package mining.interestingness;

import fst.OutputLabel;
import fst.XFst;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collector;

import mining.scores.DesqDfsScore;
import mining.scores.RankedScoreList;
import mining.statistics.collectors.DesqGlobalDataCollector;
import mining.statistics.collectors.DesqProjDbDataCollector;
import mining.statistics.collectors.SPMLocalStatisticFactory;
import mining.statistics.data.ProjDbStatData;
import mining.statistics.old.SPMStatisticsAggregator;

/**
 * DfsOnePass.java
 * 
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class DfsOnePassScored extends DesqDfsScored {

	// intial cFST state
	int initialState;

	// buffer for an input sequence
	int[] sequenceBuffer;

	// reusable hashset for computing ancestors
	private IntOpenHashSet tempAnc = new IntOpenHashSet();

	private boolean reachedFinalState;

	// depth of the search tree
	private int dfsLevel = 0;

	// Hash sets used for eps labels
	boolean[] currentStateSet;
	boolean[] nextStateSet;
	
	// score used to measure sequence performance
	DesqDfsScore score;
	
	// ranking class
	RankedScoreList rankedScoreList;
	
	// final Node
	Node finalNode;

	HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> projDbCollectors;
	HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?,?>, ?>> globalDataCollectors;
	HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> finalStateProjDbAccumulators;
	
	ProjDbStatData statisticsBaseData = new ProjDbStatData();
	
	public DfsOnePassScored(double sigma, XFst xfst, DesqDfsScore score, RankedScoreList rankedScoreList, HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?,?>, ?>> globalDataCollectors, boolean writeOutput) {
		super(sigma, xfst, writeOutput);
		initialState = xfst.getInitialState();
		
		currentStateSet = new boolean[xfst.numStates()];
		nextStateSet = new boolean[xfst.numStates()];
		
		this.score = score;
		this.rankedScoreList = rankedScoreList;
		this.projDbCollectors = score.getProjDbCollectors();
		this.globalDataCollectors = globalDataCollectors;
		this.finalStateProjDbAccumulators = new HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>>();
	}

	@Override
	protected void addInputSequence(int[] inputSequence) {
		inputSequences.add(inputSequence);
	}

	public void mine() throws IOException, InterruptedException {
		Node root = new Node(null, 0);	
		for(int sId = 0; sId < inputSequences.size(); ++sId) {
			sequenceBuffer = inputSequences.get(sId);
			incStep(sId, 0, initialState, root);
		}
		
		final IntIterator it = root.children.keySet().iterator();
		while (it.hasNext()) {
			int itemId = it.nextInt();
			Node child = root.children.get(itemId);
			
			if (score.getMaxScoreByPrefix(getCurrentSequence(child, dfsLevel + 1), globalDataCollectors, getStatisticData(child)) >= sigma) {
				expand(child);
				
			}
			child.clear();
		}
		root.clear();
	}

	private void expand(Node node) throws IOException, InterruptedException {
		
		dfsLevel++;

		int support = 0;
		PostingList.Decompressor projectedDatabase = new PostingList.Decompressor(node.projectedDatabase);
		finalStateProjDbAccumulators.clear();

		// For all sequences in projected database
		do {
			int sId = projectedDatabase.nextValue();
			sequenceBuffer = inputSequences.get(sId);
			reachedFinalState = false;

			// For all state@pos for a sequence
			do {
				int state = projectedDatabase.nextValue();
				int pos = projectedDatabase.nextValue();
				
				// for each T[pos@state]
				incStep(sId, pos, state, node);

			} while (projectedDatabase.hasNextValue());

			// increment support if atleast one snapshop corresponds to final state
			if (reachedFinalState) {
				if(finalStateProjDbAccumulators.size() == 0) {
					for (Entry<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> entry: projDbCollectors.entrySet()) {
						@SuppressWarnings("unchecked")
						DesqProjDbDataCollector<DesqProjDbDataCollector<?,?>, ?> coll = (DesqProjDbDataCollector<DesqProjDbDataCollector<?, ?>, ?>) entry.getValue();
						finalStateProjDbAccumulators.put(entry.getKey(), coll.supplier().get());
					}
				}
				
				statisticsBaseData.setPosition(-1);
				statisticsBaseData.setStateFST(-1);
				statisticsBaseData.setTransaction(sequenceBuffer);
				statisticsBaseData.setTransactionId(sId);
				
				for (Entry<String, DesqProjDbDataCollector<?, ?>> entry : finalStateProjDbAccumulators.entrySet()) {
					
					// at compile time it is not decided which type the accept function 
					@SuppressWarnings("unchecked")
					DesqProjDbDataCollector<DesqProjDbDataCollector<?,?>, ?> finalProjDBCollector = (DesqProjDbDataCollector<DesqProjDbDataCollector<?, ?>, ?>) finalStateProjDbAccumulators.get(entry.getKey());
					finalProjDBCollector.accumulator().accept(entry.getValue(), statisticsBaseData);
				}
				
				support++;
			}

		} while (projectedDatabase.nextPosting());

		// Output if at least one sequence is valid
		if (support > 0) {
			int[] outputSequence = getCurrentSequence(node, dfsLevel);
			if (score.getScoreByProjDb(outputSequence, globalDataCollectors, finalStateProjDbAccumulators) >= sigma) {
				numPatterns++;
				if (writeOutput) {
					// compute output sequence
//					int[] outputSequence = new int[dfsLevel];
//					int size = dfsLevel;
//					
//					outputSequence[--size] = node.suffixItemId;
//					Node parent = node.parent;
//					while(parent.parent != null) {
//						outputSequence[--size] = parent.suffixItemId;
//						parent = parent.parent;
//					}
					rankedScoreList.addNewOutputSequence(outputSequence, score.getScoreByProjDb(outputSequence, globalDataCollectors, finalStateProjDbAccumulators), support);
	//				writer.write(outputSequence, score.getScore(getCurrentSequence(node), getStatisticData(node)));
					//System.out.println(Arrays.toString(outputSequence) + " : " + support);
				}
			}
		}

		// Expand children with sufficient prefix support
		final IntIterator it = node.children.keySet().iterator();
		while (it.hasNext()) {
			int itemId = it.nextInt();
			Node child = node.children.get(itemId);
			
			if (score.getMaxScoreByPrefix(getCurrentSequence(child, dfsLevel + 1), globalDataCollectors, getStatisticData(child)) >= sigma) {
				expand(child);
			}
			child.clear();
		}
		node.clear();

		dfsLevel--;
	}
	
	private HashMap<String, DesqProjDbDataCollector<?, ?>> getStatisticData(Node node) {
		HashMap<String, DesqProjDbDataCollector<?, ?>> statData = new HashMap<String, DesqProjDbDataCollector<?, ?>>();
		
		for (Entry<String, DesqProjDbDataCollector<?, ?>> entry : node.localAccumulators.entrySet()) {
			statData.put(entry.getKey(), node.localAccumulators.get(entry.getKey()));
		}
		
		return statData;
	}

	// Simulates cFST until node is expanded by one item(s)
	private void incStep(int sId, int pos, int state, Node node) {

/*		reachedFinalState |= xfst.isFinalState(state);
		
		if (pos == sequenceBuffer.length) {
			return;
		}

		int itemId = sequenceBuffer[pos];

		if (xfst.hasOutgoingTransition(state, itemId)) {
			for (int tId = 0; tId < xfst.numTransitions(state); ++tId) {
				if (xfst.canStep(itemId, state, tId)) {
					int toState = xfst.getToState(state, tId);
					OutputLabel olabel = xfst.getOutputLabel(state, tId);

					switch (olabel.type) {
					case EPSILON:
						incStep(sId, pos + 1, toState, node);
						break;

					case CONSTANT:
						int outputItemId = olabel.item;
						if (flist[outputItemId] >= sigma) {
							node.append(outputItemId, sId, pos + 1, toState);
						}
						break;

					case SELF:
						if (flist[itemId] >= sigma) {
							node.append(itemId, sId, pos + 1, toState);
						}
						break;

					case SELFGENERALIZE:
						for (int id : getParents(itemId, olabel.item)) {
							if (flist[id] >= sigma) {
								node.append(id, sId, pos + 1, toState);
							}
						}
						break;

					default:
						break;
					}
				}
			}
		}*/
		reachedFinalState |= xfst.isFinalState(state);
		boolean eps = true;
		Arrays.fill(currentStateSet, false);
		currentStateSet[state] = true; 
		while (pos < sequenceBuffer.length) {
			int itemId = sequenceBuffer[pos];
			
			for (int s = 0; s < currentStateSet.length; s++) {
				if(currentStateSet[s] != true) {
					continue;
				}
				if (xfst.hasOutgoingTransition(s, itemId)) {
					for (int tId = 0; tId < xfst.numTransitions(s); ++tId) {
						if (xfst.canStep(itemId, s, tId)) {
							int toState = xfst.getToState(s, tId);
							OutputLabel olabel = xfst.getOutputLabel(s, tId);

							switch (olabel.type) {
							case EPSILON:
								// incStep(sId, pos + 1, toState, node);
								eps = true;
								nextStateSet[toState] = true;
								reachedFinalState |= xfst.isFinalState(toState);
								break;

							case CONSTANT:
								int outputItemId = olabel.item;
								if (score.getMaxScoreByItem(outputItemId, globalDataCollectors) >= sigma) {
									node.append(outputItemId, sId, pos + 1, toState);
									node.updateStatistics(outputItemId, sId, sequenceBuffer, pos, toState);
								}
								break;

							case SELF:
								if (score.getMaxScoreByItem(itemId, globalDataCollectors) >= sigma) {
									node.append(itemId, sId, pos + 1, toState);
									node.updateStatistics(itemId, sId, sequenceBuffer, pos, toState);
								}
								break;

							case SELFGENERALIZE:
								for (int id : getParents(itemId, olabel.item)) {
									if (score.getMaxScoreByItem(id, globalDataCollectors) >= sigma) {
										node.append(id, sId, pos + 1, toState);
										node.updateStatistics(id, sId, sequenceBuffer, pos, toState);
									}
								}
								break;

							default:
								break;
							}
						}
					}
				}
			}
			if (eps) {
				System.arraycopy(nextStateSet, 0, currentStateSet, 0, nextStateSet.length);
				Arrays.fill(nextStateSet, false);
				pos = pos + 1;
			}
		}

	}

	/**
	 * @param itemId
	 * @param rootItemId
	 * @return ancestors of itemId that are descendants of rootItemId
	 */
	private IntArrayList getParents(int itemId, int rootItemId) {
		IntArrayList stack = new IntArrayList();
		int top = 0;
		stack.add(itemId);
		tempAnc.add(itemId);
		while (top < stack.size()) {
			int currItemId = stack.getInt(top);
			for (int parentId : dictionary.getParents(currItemId)) {
				if (xfst.isReachable(rootItemId, parentId) && !tempAnc.contains(parentId)) {
					stack.add(parentId);
					tempAnc.add(parentId);
				}
			}
			top++;
		}
		tempAnc.clear();
		return stack;
	}
	
	private int[] getCurrentSequence(Node currentNode, int level) {
		int[] outputSequence = new int[level];
		int size = level;
		outputSequence[--size] = currentNode.suffixItemId;
		Node parent = currentNode.parent;
		while(parent.parent != null) {
			outputSequence[--size] = parent.suffixItemId;
			parent = parent.parent;
		}
		return outputSequence;
	}
	
	// Dfs tree
	private final class Node {
		int lastSequenceId = -1;
		int suffixItemId;

		Node parent;
		ByteArrayList projectedDatabase = new ByteArrayList();;
		BitSet[] statePosSet = new BitSet[xfst.numStates()];
		Int2ObjectOpenHashMap<Node> children = new Int2ObjectOpenHashMap<Node>();
		HashMap<String, DesqProjDbDataCollector<?, ?>> localAccumulators = new HashMap<String, DesqProjDbDataCollector<?, ?>>();

		Node(Node parent, int suffixItemId) {
			this.parent = parent;
			this.suffixItemId = suffixItemId;

			for (int i = 0; i < xfst.numStates(); ++i) {
				statePosSet[i] = new BitSet();
			}
			
			for (Entry<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> entry: projDbCollectors.entrySet()) {
				@SuppressWarnings("unchecked")
				DesqProjDbDataCollector<DesqProjDbDataCollector<?,?>, ?> coll = (DesqProjDbDataCollector<DesqProjDbDataCollector<?, ?>, ?>) entry.getValue();
				localAccumulators.put(entry.getKey(), coll.supplier().get());
			}
		}

		void flush() {
			for (int i = 0; i < xfst.numStates(); ++i) {
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
		
		
		void updateStatistics(int itemId, int sequenceId, int transaction[], int position, int state) {
			Node node = children.get(itemId);
			statisticsBaseData.setPosition(position);
			statisticsBaseData.setStateFST(state);
			statisticsBaseData.setTransaction(transaction);
			statisticsBaseData.setTransactionId(sequenceId);
			for (Entry<String, DesqProjDbDataCollector<?, ?>> entry : node.localAccumulators.entrySet()) {
				
				// at compile time it is not decided which type the accept function 
				@SuppressWarnings("unchecked")
				DesqProjDbDataCollector<DesqProjDbDataCollector<?,?>, ?> nodeProjDbCollector = (DesqProjDbDataCollector<DesqProjDbDataCollector<?, ?>, ?>) node.localAccumulators.get(entry.getKey());
				nodeProjDbCollector.accumulator().accept(entry.getValue(), statisticsBaseData);
			}
		}

		void clear() {
			projectedDatabase = null;
			statePosSet = null;
			children = null;
		}
	}

}
