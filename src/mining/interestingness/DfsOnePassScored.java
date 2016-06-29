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
import java.util.stream.Collector;

import mining.scores.RankedScoreList;
import mining.scores.SPMScore;
import mining.statistics.SPMLocalStatisticFactory;
import mining.statistics.SPMStatisticsData;
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
	SPMScore score;
	
	// ranking class
	RankedScoreList rankedScoreList;
	
	@SuppressWarnings("rawtypes")
	HashMap<String, Collector> collectors;
	
	SPMStatisticsData statisticsBaseData = new SPMStatisticsData();
	
	public DfsOnePassScored(double sigma, XFst xfst, SPMScore score, RankedScoreList rankedScoreList, 
							@SuppressWarnings("rawtypes") HashMap<String, Collector> collectors, boolean writeOutput) {
		super(sigma, xfst, writeOutput);
		initialState = xfst.getInitialState();
		
		currentStateSet = new boolean[xfst.numStates()];
		nextStateSet = new boolean[xfst.numStates()];
		
		this.score = score;
		this.rankedScoreList = rankedScoreList;
		this.collectors = collectors;
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
			
			if (score.getMaximumScore(getCurrentSequence(child, dfsLevel + 1), getStatisticData(child)) >= sigma) {
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
				support++;
			}

		} while (projectedDatabase.nextPosting());

		// Output if at least one sequence is valid
		if (support > 0) {
			int[] outputSequence = getCurrentSequence(node, dfsLevel);
			if(score.getScore(outputSequence, getStatisticData(node), support) >= sigma) {
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
					rankedScoreList.addNewOutputSequence(outputSequence, score.getScore(outputSequence, getStatisticData(node), support), support);
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
			
			if (score.getMaximumScore(getCurrentSequence(child, dfsLevel + 1), getStatisticData(child)) >= sigma) {
				expand(child);
			}
			child.clear();
		}
		node.clear();

		dfsLevel--;
	}
	
	@SuppressWarnings("unchecked")
	private HashMap<String, Object> getStatisticData(Node node) {
		HashMap<String, Object> statData = new HashMap<String, Object>();
		
		for (Entry<String, Object> entry : node.localAccumulators.entrySet()) {
			statData.put(entry.getKey(), collectors.get(entry.getKey()).finisher().apply(entry.getValue()));
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
								if (score.getItemScore(outputItemId) >= sigma) {
									node.append(outputItemId, sId, pos + 1, toState);
									node.updateStatistics(outputItemId, sId, sequenceBuffer, pos, toState);
								}
								break;

							case SELF:
								if (score.getItemScore(itemId) >= sigma) {
									node.append(itemId, sId, pos + 1, toState);
									node.updateStatistics(itemId, sId, sequenceBuffer, pos, toState);
								}
								break;

							case SELFGENERALIZE:
								for (int id : getParents(itemId, olabel.item)) {
									if (score.getItemScore(id) >= sigma) {
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
		int prefixSupport;
		Node parent;
		ByteArrayList projectedDatabase = new ByteArrayList();;
		BitSet[] statePosSet = new BitSet[xfst.numStates()];
		Int2ObjectOpenHashMap<Node> children = new Int2ObjectOpenHashMap<Node>();
		HashMap<String,Object> localAccumulators = new HashMap<String, Object>();

		Node(Node parent, int suffixItemId) {
			this.parent = parent;
			this.suffixItemId = suffixItemId;

			for (int i = 0; i < xfst.numStates(); ++i) {
				statePosSet[i] = new BitSet();
			}
			
			for (@SuppressWarnings("rawtypes") Entry<String, Collector> entry: collectors.entrySet()) {
				@SuppressWarnings("rawtypes")
				Collector coll = entry.getValue();
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
		
		@SuppressWarnings({ "unchecked", "rawtypes" })
		void updateStatistics(int itemId, int sequenceId, int transaction[], int position, int state) {
			Node node = children.get(itemId);
			statisticsBaseData.setPosition(position);
			statisticsBaseData.setStateFST(state);
			statisticsBaseData.setTransaction(transaction);
			statisticsBaseData.setTransactionId(sequenceId);
			for (Entry<String, Object> entry : node.localAccumulators.entrySet()) {
				Collector collector = collectors.get(entry.getKey());
				collector.accumulator().accept(entry.getValue(), statisticsBaseData);
			}
		}

		void clear() {
			projectedDatabase = null;
			statePosSet = null;
			children = null;
		}
	}

}
