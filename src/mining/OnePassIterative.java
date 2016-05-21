package mining;

//import java.util.Arrays;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import fst.OutputLabel;
import fst.XFst;

public class OnePassIterative extends DesqCount {
	
	// Buffer to store output sequences
	IntArrayList buffer = new IntArrayList();

	// Hashset to store ancestors
	IntOpenHashSet tempAnc = new IntOpenHashSet();
	
	int pos = 0;
	
	int numStates = 0;
	
	class Suffix {
		IntOpenHashSet items = new IntOpenHashSet();
		Suffix prefix = null;
	
		Suffix(boolean init) {
			if(init){
				items.add(-1);
			}
		}
	}
	
	Int2ObjectOpenHashMap<Suffix> stateSuffixMap;
			
	public OnePassIterative(int sigma, XFst xfst, boolean writeOutput, boolean useFlist) {
		super(sigma, xfst, writeOutput, useFlist);
		numStates = xfst.numStates();
		stateSuffixMap = new Int2ObjectOpenHashMap<OnePassIterative.Suffix>(numStates);
		reset();
	}
	
	private void reset() {
		pos = 0;
		stateSuffixMap.clear();
	}

	
	@Override
	protected void computeMatch() {
		buffer.clear();
		
		stateSuffixMap.put(xfst.getInitialState(), new Suffix(true));
		
		while(pos < sequence.length) {
			step();
			pos++;
		}
		for(int state : stateSuffixMap.keySet()) {
			if(xfst.isFinalState(state)){
				computeSequence(stateSuffixMap.get(state));
			}
		}
		reset();
		
	}
	
	private void step() {
		Int2ObjectOpenHashMap<Suffix> nextStateSuffixMap = new Int2ObjectOpenHashMap<Suffix>(numStates);

		int itemId = sequence[pos];

		for (int s : stateSuffixMap.keySet()) {
			Suffix prefix = stateSuffixMap.get(s);
			
			if(xfst.isFinalState(s)){
				computeSequence(prefix);
			}

			if (xfst.hasOutgoingTransition(s, itemId)) {
				for (int tId = 0; tId < xfst.numTransitions(s); ++tId) {
					if (xfst.canStep(itemId, s, tId)) {
						int toState = xfst.getToState(s, tId);
						OutputLabel olabel = xfst.getOutputLabel(s, tId);

						Suffix suffix = nextStateSuffixMap.get(toState);
						if (null == suffix) {
							suffix = new Suffix(false);
							nextStateSuffixMap.put(toState, suffix);
						}
						if(prefix.items.isEmpty()) {
							suffix.prefix = prefix.prefix;
						} else {
							suffix.prefix = prefix;
						}

						switch (olabel.type) {
						case EPSILON:
							break;

						case CONSTANT:
							int outputItemId = olabel.item;
							if (!useFlist || flist[outputItemId] >= sigma) {
								suffix.items.add(outputItemId);
							}
							break;

						case SELF:
							if (!useFlist || flist[itemId] >= sigma) {
								suffix.items.add(itemId);
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
									suffix.items.add(id);
								}
							}

							break;
						default:
							break;
						}
						//if(suffix.items.isEmpty())
						//	suffix = suffix.prefix;
					}

					
				}
			}
		}
		stateSuffixMap = nextStateSuffixMap;
		nextStateSuffixMap = null;
	}	

	private void computeSequence(Suffix suffix) {
		if(null == suffix.prefix) {
			if(!buffer.isEmpty()) {
				countSequence(reverse(buffer.toIntArray()));
			}
			return;
		}
		if(suffix.items.isEmpty())
			computeSequence(suffix.prefix);
		else {
		for (int itemId : suffix.items) {
			buffer.add(itemId);
			computeSequence(suffix.prefix);
			buffer.remove(buffer.size() - 1);
		}
		}

	}

	private int[] reverse(int[] a) {
		int i = 0; int j = a.length - 1;
		while (j > i) {
			a[i] ^= a[j]; a[j] ^= a[i]; a[i] ^= a[j];
			i++; j--;
		}
		//System.out.println(Arrays.toString(a));
		return a;
	}
}