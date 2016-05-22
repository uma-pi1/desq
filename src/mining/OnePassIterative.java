package mining;

//import java.util.Arrays;

import java.util.Arrays;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import fst.OutputLabel;
import fst.XFst;

public class OnePassIterative extends DesqCount {

	// Buffer to store output sequences
	IntArrayList buffer = new IntArrayList();

	// Hashset to store ancestors
	IntOpenHashSet tempAnc = new IntOpenHashSet();

	int pos = 0;

	int numStates = 0;

	int initialState = 0;

	class Node {
		int item;

		ObjectArrayList<Node> prefixes;

		Node(int item, ObjectArrayList<Node> suffixes) {
			this.item = item;
			this.prefixes = suffixes;
		}
	}

	ObjectArrayList<Node>[] statePrefix;
	int[] stateList;
	int stateListSize = 0;

	@SuppressWarnings("unchecked")
	public OnePassIterative(int sigma, XFst xfst, boolean writeOutput, boolean useFlist) {
		super(sigma, xfst, writeOutput, useFlist);
		numStates = xfst.numStates();
		statePrefix = (ObjectArrayList<Node>[]) new ObjectArrayList[numStates];
		stateList = new int[numStates];
		initialState = xfst.getInitialState();
	}

	private void reset() {
		pos = 0;
		for (int i = 0; i < numStates; ++i) {
			statePrefix[i] = null;
		}
		stateListSize = 0;
	}

	@Override
	protected void computeMatch() {

		statePrefix[initialState] = new ObjectArrayList<Node>();
		stateList[stateListSize++] = initialState;
		statePrefix[initialState].add(null);

		while (pos < sequence.length) {
			step();
			pos++;
		}
		reset();

	}

	private void step() {
		@SuppressWarnings("unchecked")
		ObjectArrayList<Node>[] nextStatePrefix = (ObjectArrayList<Node>[]) new ObjectArrayList[numStates];
		int[] nextStateList = new int[numStates];
		int nextStateListSize = 0;

		int itemId = sequence[pos];

		for (int i = 0; i < stateListSize; i++) {
			int fromState = stateList[i];

			if (xfst.hasOutgoingTransition(fromState, itemId)) {
				for (int tId = 0; tId < xfst.numTransitions(fromState); ++tId) {
					if (xfst.canStep(itemId, fromState, tId)) {
						int toState = xfst.getToState(fromState, tId);
						OutputLabel olabel = xfst.getOutputLabel(fromState, tId);

						boolean isFinal = xfst.isFinalState(toState);
						Node node;

						if (null == nextStatePrefix[toState]) {
							nextStatePrefix[toState] = new ObjectArrayList<Node>();
							nextStateList[nextStateListSize++] = toState;
						}

						switch (olabel.type) {
						case EPSILON:
							if (isFinal)
								computeOutput(statePrefix[fromState]);
							for (Node n : statePrefix[fromState])
								nextStatePrefix[toState].add(n);
							break;

						case CONSTANT:
							int outputItemId = olabel.item;
							if (!useFlist || flist[outputItemId] >= sigma) {
								node = new Node(outputItemId, statePrefix[fromState]);
								if (isFinal)
									computeOutput(node);
								nextStatePrefix[toState].add(node);
							}
							break;

						case SELF:
							if (!useFlist || flist[itemId] >= sigma) {
								node = new Node(itemId, statePrefix[fromState]);
								if (isFinal)
									computeOutput(node);

								nextStatePrefix[toState].add(node);
							}
							break;

						case SELFGENERALIZE:
							IntArrayList stack = new IntArrayList();
							int top = 0;
							int rootItemId = olabel.item;
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
							for (int id : stack) {
								if (!useFlist || flist[id] >= sigma) {
									node = new Node(id, statePrefix[fromState]);
									if (isFinal)
										computeOutput(node);
									nextStatePrefix[toState].add(node);
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
		this.stateList = nextStateList;
		this.stateListSize = nextStateListSize;
		this.statePrefix = nextStatePrefix;
	}

	private void computeOutput(ObjectArrayList<Node> suffixes) {
		for (Node node : suffixes)
			computeOutput(node);
	}

	private void outputBuffer() {

		if (!buffer.isEmpty()) {
			countSequence(reverse(buffer.toIntArray()));
			// System.out.println(buffer);
		}
	}

	private void computeOutput(Node node) {
		if (node == null) {
			outputBuffer();
			return;
		}

		buffer.add(node.item);
		for (Node n : node.prefixes) {
			computeOutput(n);
		}
		buffer.remove(buffer.size() - 1);
	}

	private int[] reverse(int[] a) {
		int i = 0;
		int j = a.length - 1;
		while (j > i) {
			a[i] ^= a[j];
			a[j] ^= a[i];
			a[i] ^= a[j];
			i++;
			j--;
		}
		// System.out.println(Arrays.toString(a));
		return a;
	}
}