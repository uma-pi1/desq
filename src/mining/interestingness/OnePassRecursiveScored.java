package mining.interestingness;


import java.util.HashMap;

import driver.DesqConfig.Match;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import mining.scores.DesqCountScore;
import mining.scores.NotImplementedExcepetion;
import mining.scores.RankedScoreList;
import mining.statistics.collectors.DesqGlobalDataCollector;
import fst.OutputLabel;
import fst.XFst;

public class OnePassRecursiveScored extends DesqCountScored {
	
	// Buffer to store output sequences
	IntArrayList buffer = new IntArrayList();

	// Hashset to store ancestors
	IntOpenHashSet tempAnc = new IntOpenHashSet();
	boolean executeScoreBySequence = true;
	
	
	public OnePassRecursiveScored(double sigma, XFst xfst, DesqCountScore score, HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>> globalDataCollectors, RankedScoreList rankedScoreList, boolean writeOutput, Match match) {
		super(sigma, xfst, score, globalDataCollectors, rankedScoreList, writeOutput, match);
	}

	@Override
	protected void computeMatch() {
		buffer.clear();
		step(0, xfst.getInitialState());
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
					stepCounts++;
					int toState = xfst.getToState(state, tId);
					OutputLabel olabel = xfst.getOutputLabel(state, tId);
					
					switch (olabel.type) {
					
					case EPSILON:
						step(pos + 1, toState);
						break;
					
					case CONSTANT:
						int outputItemId = olabel.item;
						if (score.getMaxScoreByItem(outputItemId, globalDataCollectors) >= sigma) {
							buffer.add(outputItemId);
							if(score.getMaxScoreByPrefix(buffer.toIntArray(), 
									globalDataCollectors,
									sequence,
									sid,
									pos + 1,
									toState) >= sigma) {
								step(pos + 1, toState);
							}
							buffer.remove(buffer.size() - 1);
						}
						break;
					
					case SELF:
						if (score.getMaxScoreByItem(itemId, globalDataCollectors) >= sigma) {
							buffer.add(itemId);
							if(score.getMaxScoreByPrefix(buffer.toIntArray(), 
									globalDataCollectors,
									sequence,
									sid,
									pos + 1,
									toState) >= sigma) {
								step(pos + 1, toState);
							}
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
							if (score.getMaxScoreByItem(id, globalDataCollectors) >= sigma) {
								buffer.add(id);
								if(score.getMaxScoreByPrefix(buffer.toIntArray(), 
																globalDataCollectors,
																sequence,
																sid,
																pos + 1,
																toState) >= sigma) {
									step(pos + 1, toState);
								}
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
	
	private void countSequence(int[] sequence) {
		if(executeScoreBySequence) {
			try {
				if(score.getScoreBySequence(sequence, globalDataCollectors) >= sigma) {
					addSequenceToOutput(sequence, score.getScoreBySequence(sequence, globalDataCollectors));
				};
			} catch (NotImplementedExcepetion e)  {
				executeScoreBySequence = false;
			}
		}
		
		updateFinalSequenceStatistics(sequence);
	}

}
