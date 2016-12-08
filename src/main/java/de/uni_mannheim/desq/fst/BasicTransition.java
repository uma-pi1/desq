package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.BasicDictionary;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.RestrictedDictionary;
import de.uni_mannheim.desq.util.BitNonNegativeIntSet;
import de.uni_mannheim.desq.util.IntSetOptimizer;
import it.unimi.dsi.fastutil.ints.*;

import java.util.Iterator;

public final class BasicTransition extends Transition {
	public enum InputLabelType { SELF, SELF_DESCENDANTS }

    public enum OutputLabelType { SELF, SELF_ASCENDANTS, CONSTANT, EPSILON }

    // parameters
	final int inputLabel; 
	final InputLabelType inputLabelType;
	final int outputLabel; // only when outputlabel = constant or self_gen
	final OutputLabelType outputLabelType;
	final String inputLabelSid;
	final String outputLabelSid;

	// internal indexes
	final BasicDictionary dict;
	final IntSet inputFids;
	final BasicDictionary outputDict;
	final boolean isForest;

	private BasicTransition(BasicTransition other) {
		this.dict = other.dict;
		this.inputLabel = other.inputLabel;
		this.inputLabelType = other.inputLabelType;
		this.outputLabel = other.outputLabel;
		this.outputLabelType = other.outputLabelType;
		this.inputLabelSid = other.inputLabelSid;
		this.outputLabelSid = other.outputLabelSid;
		this.inputFids = other.inputFids;
		this.outputDict = other.outputDict;
		this.isForest = other.isForest;
	}
	
	// */* (no dots)
	public BasicTransition(int inputLabel, InputLabelType inputLabelType, 
			int outputLabel, OutputLabelType outputLabelType, 
			State toState, Dictionary dict) {
		// initialize
		this.dict = dict;
		this.inputLabel = inputLabel;
		this.inputLabelType = inputLabelType;
		this.outputLabel = outputLabel;
		this.outputLabelType = outputLabelType;
		this.inputLabelSid = inputLabel > 0 ? dict.sidOfFid(inputLabel): ".";
		this.outputLabelSid = outputLabel > 0 ? dict.sidOfFid(outputLabel) : null;
		this.toState = toState;

		// compute the set of fids that match this transition
		if (inputLabel == 0)
			inputFids = null;
		else switch (inputLabelType) {
		case SELF:
			inputFids = IntSets.singleton(inputLabel);
			break;
		case SELF_DESCENDANTS:
			// by default, use a bit set
			BitNonNegativeIntSet inputFidsBitSet = descendantFids(inputLabel, dict);
			if (outputLabelType == OutputLabelType.SELF_ASCENDANTS) {
				// store as bitset and reuse it for the output dictionary
				inputFidsBitSet.trim();
				inputFids = inputFidsBitSet;
			} else {
				// store optimized
				inputFids = IntSetOptimizer.optimize(inputFidsBitSet, true);
			}
			break;
		default: // unreachable
			 throw new IllegalStateException();
		}

		// compute the output dictionary
		if (outputLabelType == OutputLabelType.SELF_ASCENDANTS) {
			if (outputLabel == 0) {
				outputDict = dict;
			} else {
				if (inputLabel != outputLabel) // this is the only case we handle
					throw new IllegalStateException();
				outputDict = new RestrictedDictionary(dict, ((BitNonNegativeIntSet)inputFids).bitSet());
			}
		} else outputDict = null;

		// check whether every output item has at most one parent (to optimize iteration)
		isForest = outputDict == null ? dict.isForest() : outputDict.isForest();
	}	

	private static BitNonNegativeIntSet descendantFids(int fid, Dictionary dict) {
		BitNonNegativeIntSet descendants = new BitNonNegativeIntSet(dict.lastFid()+1);
		descendants.add(fid);
		dict.addDescendantFids(fid, descendants);
		return descendants;
	}

	private static final class ItemStateIterator implements Iterator<ItemState> {
		BasicTransition transition;
	    int nextFid;
		final ItemState itemState = new ItemState();
		IntIterator fidIterator;
		final IntCollection ascendants; // reusable; fidIterator goes over this set

		ItemStateIterator(boolean isForest) {
			ascendants = isForest ? new IntArrayList() : new IntAVLTreeSet();
		}

		@Override
		public boolean hasNext() {
            return nextFid >=0;
		}

		@Override
		public ItemState next() {
			assert nextFid >=0;

			switch (transition.outputLabelType) {
			case EPSILON:
				itemState.itemFid = 0;
				nextFid = -1;
				break;
			case CONSTANT:
				itemState.itemFid = transition.outputLabel;
				nextFid = -1;
				break;
			case SELF:
				itemState.itemFid = nextFid;
				nextFid = -1;
				break;
			case SELF_ASCENDANTS:
				itemState.itemFid = nextFid;
				if (fidIterator.hasNext())
					nextFid = fidIterator.nextInt();
				else
					nextFid = -1;
			}

			return itemState;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}
	
	@Override
	public boolean matches(int itemFid) {
		return inputLabel==0 || inputFids.contains(itemFid);
	}

	@Override
	public boolean matchesAll() {
		return inputLabel == 0 || inputFids.size() == dict.size();
	}

	@Override
	public boolean matchesAllWithFrequentOutput(int largestFrequentItemFid) {
		if (!hasOutput() || !matchesAll()) return false;
		switch (outputLabelType) {
		case SELF:
			// check if all items frequent
			return dict.lastFid() <= largestFrequentItemFid;
		case SELF_ASCENDANTS:
			assert dict.size() == outputDict.size(); // because all items are matched, we produce all ancestors
			return dict.largestRootFid() <= largestFrequentItemFid; // then every item has a frequent root ancestor
		case CONSTANT:
			// check if only one output
			return dict.size() == 1 && dict.firstFid() == outputLabel;
		case EPSILON:
		default:
			return false;
		}
	}

	@Override
	public IntIterator matchedFidIterator() {
		if (inputLabel == 0) {
			return dict.fidIterator();
		} else {
			return inputFids.iterator();
		}
	}

	public boolean hasOutput() {
		return !outputLabelType.equals(OutputLabelType.EPSILON);
	}

	@Override
	public boolean matchesWithFrequentOutput(int inputFid, int largestFrequentItemFid) {
		if (!hasOutput()) {
			return false;
		}
		switch (outputLabelType) {
		case SELF:
			return inputFid <= largestFrequentItemFid;
		case SELF_ASCENDANTS:
			if (outputLabel != 0) {
				return outputLabel <= largestFrequentItemFid; // we generalize up to there
			} else {
				return dict.hasAscendantWithFidBelow(inputFid, largestFrequentItemFid);
			}
		case CONSTANT:
			return outputLabel <= largestFrequentItemFid;
		case EPSILON:
		default:
			throw new IllegalStateException("unreachable");
		}
	}

	@Override
	public Iterator<ItemState> consume(int itemFid, Iterator<ItemState> it) {
		ItemStateIterator resultIt;
		if (it != null && it instanceof ItemStateIterator)
            resultIt = (ItemStateIterator) it;
		else
			resultIt = new ItemStateIterator(isForest);
		
		if (inputLabel==0 || inputFids.contains(itemFid)) {
			resultIt.transition = this;
		    resultIt.nextFid = itemFid;
			resultIt.itemState.state = toState;
			if (outputLabelType == OutputLabelType.SELF_ASCENDANTS) {
				resultIt.ascendants.clear();
				outputDict.addAscendantFids(itemFid, resultIt.ascendants);
				resultIt.fidIterator = resultIt.ascendants.iterator();
			}
		} else {
			resultIt.nextFid = -1;
		}

		return resultIt;
	}

	@Override
	public BasicTransition shallowCopy() {
		return new BasicTransition(this);
	}

	@Override
	public String toPatternExpression() {
		String patternExpression;
		if(inputLabel == 0)
			patternExpression = ".";
		else {
			patternExpression = "\"" + inputLabelSid + "\"";
			if(inputLabelType == InputLabelType.SELF)
				patternExpression += "=";
		}
		switch(outputLabelType) {
			case SELF:
				patternExpression = "("+patternExpression+")";
				break;
			case SELF_ASCENDANTS:
				patternExpression = "("+patternExpression+"^"+")";
				break;
			case CONSTANT:
				patternExpression = "("+patternExpression+"=^"+")";
				break;
			case EPSILON:
				break;

		}
		return patternExpression;
	}

	@Override
	public String labelString() {
		StringBuilder sb = new StringBuilder();
		if(inputLabel == 0)
			sb.append(".");
		else {
			sb.append(inputLabelSid);
			if(inputLabelType == InputLabelType.SELF) 
				sb.append("=");
		}
		sb.append(":");
		switch(outputLabelType) {
		case SELF:
			sb.append("$");
			break;
		case SELF_ASCENDANTS:
			sb.append("$-" + (outputLabelSid != null ? outputLabelSid : ""));
			break;
		case CONSTANT:
			sb.append(outputLabelSid);
			break;
		case EPSILON:
			sb.append("ε");
			
		}
		return sb.toString();
	}

	@Override
	public String toString() {
		return labelString() + " -> " + toState.id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + inputLabel;
		result = prime * result + ((inputLabelType == null) ? 0 : inputLabelType.hashCode());
		result = prime * result + outputLabel;
		result = prime * result + ((outputLabelType == null) ? 0 : outputLabelType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BasicTransition other = (BasicTransition) obj;
		if (inputLabel != other.inputLabel)
			return false;
		if (inputLabelType != other.inputLabelType)
			return false;
		if (outputLabel != other.outputLabel)
			return false;
		return outputLabelType == other.outputLabelType;
	}

	@Override
	public boolean isDotEps() {
//		if(inputLabel == 0 && outputLabelType == OutputLabelType.EPSILON)
//			return true;
//		else
//			return false;
//
		return (inputLabel == 0 && outputLabelType == OutputLabelType.EPSILON);
	}
}
