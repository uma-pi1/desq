package mining;

import driver.DesqCountDriver;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import fst.OutputLabel;
import fst.XFst;

public class OnePassRecursive extends DesqCount {
	
	// Buffer to store output sequences
	IntArrayList buffer = new IntArrayList();

	// Hashset to store ancestors
	IntOpenHashSet tempAnc = new IntOpenHashSet();
	
	
	public OnePassRecursive(int sigma, XFst xfst, boolean writeOutput, boolean useFlist) {
		super(sigma, xfst, writeOutput, useFlist);
	}

	@Override
	protected void computeMatch() {
		DesqCountDriver.forwardPassTime.start();
		
		buffer.clear();
		step(0, xfst.getInitialState());
		
		DesqCountDriver.forwardPassTime.stop();
	}

	private void step(int pos, int state) {
		if (xfst.isFinalState(state)) {
			if (!buffer.isEmpty()) {
				// System.out.println(buffer);
				countSequence(buffer.toIntArray());
			}
		}
		if (pos == sequence.length) {
			return;
		}
		int itemId = sequence[pos];
		
		if(xfst.hasOutgoingTransition(state, itemId)) {

			for(int tId = 0; tId < xfst.numTransitions(state); ++tId) {
				if(xfst.canStep(itemId, state, tId)) {

					int toState = xfst.getToState(state, tId);
					OutputLabel olabel = xfst.getOutputLabel(state, tId);
					
					switch (olabel.type) {
					
					case EPSILON:
						step(pos + 1, toState);
						break;
					
					case CONSTANT:
						int outputItemId = olabel.item;
						if (!useFlist || flist[outputItemId] >= sigma) {
							buffer.add(outputItemId);
							step(pos + 1, toState);
							buffer.remove(buffer.size() - 1);
						}
						break;
					
					case SELF:
						if (!useFlist || flist[itemId] >= sigma) {
							buffer.add(itemId);
							step(pos + 1, toState);
							buffer.remove(buffer.size() - 1);
						}
						break;
					
					case SELFGENERALIZE:
						IntArrayList stack = new IntArrayList(); int top = 0;
						int rootItemId = olabel.item;
						stack.add(itemId); tempAnc.add(itemId);
						while (top < stack.size()) {
							int currItemId = stack.getInt(top);
							for(int parentId : dictionary.getParents(currItemId)) {
								if(xfst.isReachable(rootItemId, parentId) && !tempAnc.contains(parentId)) {
									stack.add(parentId);
									tempAnc.add(parentId);
								}
							}
							top++;
						}
						tempAnc.clear();
						for(int id : stack) {
							if (!useFlist || flist[id] >= sigma) {
								buffer.add(id);
								step(pos + 1, toState);
								buffer.remove(buffer.size() - 1);
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

}
