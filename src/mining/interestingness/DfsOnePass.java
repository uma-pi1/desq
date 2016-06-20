package mining.interestingness;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import fst.OutputLabel;
import fst.XFst;

/**
 * DfsOnePass.java
 * 
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class DfsOnePass extends DesqDfs {

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
	IntOpenHashSet currentStateSet = new IntOpenHashSet();
	IntOpenHashSet nextStateSet = new IntOpenHashSet();
	
	public DfsOnePass(int sigma, XFst xfst, boolean writeOutput) {
		super(sigma, xfst, writeOutput);
		initialState = xfst.getInitialState();
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
			if (child.prefixSupport >= sigma) {
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

		// Output if P-frequent
		if (support >= sigma) {
			numPatterns++;
			if (writeOutput) {
				// compute output sequence
				int[] outputSequence = new int[dfsLevel];
				int size = dfsLevel;
				
				outputSequence[--size] = node.suffixItemId;
				Node parent = node.parent;
				while(parent.parent != null) {
					outputSequence[--size] = parent.suffixItemId;
					parent = parent.parent;
				}
				writer.write(outputSequence, support);
				//System.out.println(Arrays.toString(outputSequence) + " : " + support);
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
		currentStateSet.clear();
		currentStateSet.add(state);
		while (pos < sequenceBuffer.length) {
			int itemId = sequenceBuffer[pos];

			for (int s : currentStateSet) {
				if (xfst.hasOutgoingTransition(s, itemId)) {
					for (int tId = 0; tId < xfst.numTransitions(s); ++tId) {
						if (xfst.canStep(itemId, s, tId)) {
							int toState = xfst.getToState(s, tId);
							OutputLabel olabel = xfst.getOutputLabel(s, tId);

							switch (olabel.type) {
							case EPSILON:
								// incStep(sId, pos + 1, toState, node);
								eps = true;
								nextStateSet.add(toState);
								reachedFinalState |= xfst.isFinalState(toState);
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
				}
			}
			if (eps) {
				currentStateSet = nextStateSet.clone();
				nextStateSet.clear();
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

	// Dfs tree
	private final class Node {
		int prefixSupport = 0;
		int lastSequenceId = -1;
		int suffixItemId;
		Node parent;
		ByteArrayList projectedDatabase = new ByteArrayList();;
		BitSet[] statePosSet = new BitSet[xfst.numStates()];
		Int2ObjectOpenHashMap<Node> children = new Int2ObjectOpenHashMap<Node>();

		Node(Node parent, int suffixItemId) {
			this.parent = parent;
			this.suffixItemId = suffixItemId;

			for (int i = 0; i < xfst.numStates(); ++i) {
				statePosSet[i] = new BitSet();
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

		void clear() {
			projectedDatabase = null;
			statePosSet = null;
			children = null;
		}
	}

}
