package de.uni_mannheim.desq.fst;

import java.util.Iterator;

import de.uni_mannheim.desq.dictionary.Dictionary;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;

public class BasicTransition extends Transition {
	public enum InputLabelType { SELF, SELF_DESCENDANTS };
	public enum OutputLabelType { SELF, SELF_ASCENDANTS, CONSTANT, EPSILON };

	// parameters
	final int inputLabel; 
	final InputLabelType inputLabelType;
	final int outputLabel; // only when outputlabel = constant or self_gen
	final OutputLabelType outputLabelType;

	// internal indexes
	final IntSet inputFids;
	final Dictionary outputDict;

	private BasicTransition(BasicTransition other) {
		this.inputLabel = other.inputLabel;
		this.inputLabelType = other.inputLabelType;
		this.outputLabel = other.outputLabel;
		this.outputLabelType = other.outputLabelType;
		this.inputFids = other.inputFids;
		this.outputDict = other.outputDict;
	}
	
	// */* (no dots)
	public BasicTransition(int inputLabel, InputLabelType inputLabelType, 
			int outputLabel, OutputLabelType outputLabelType, 
			State toState, Dictionary dict) {
		this.inputLabel = inputLabel;
		this.inputLabelType = inputLabelType;
		this.outputLabel = outputLabel;
		this.outputLabelType = outputLabelType;
		this.toState = toState;

		if (inputLabel == 0)
			inputFids = null;
		else switch (inputLabelType) {
		case SELF:
			inputFids = IntSets.singleton(inputLabel);
			break;
		case SELF_DESCENDANTS:
			inputFids = dict.descendantsFids(inputLabel);
			break;
		default:
			 inputFids = null;
		}
		
		if (outputLabelType == OutputLabelType.SELF_ASCENDANTS) {
			if (outputLabel == 0) 
				outputDict = dict;
			else
				outputDict = dict.restrictedCopy(dict.descendantsFids(outputLabel));
		} else outputDict = null;
	}	

	
	private static final class ItemStateIterator implements Iterator<ItemState> {
		BasicTransition transition;
	    int nextFid;
		final ItemState itemState = new ItemState();
		IntIterator fidIterator;
		IntSet ascendants = new IntAVLTreeSet(); // reusable; fidIterator goes over this set
		
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
	public Iterator<ItemState> consume(int itemFid) {
		return consume(itemFid, null);
	}

	@Override
	public Iterator<ItemState> consume(int itemFid, Iterator<ItemState> it) {
		ItemStateIterator resultIt = null;
		if (it != null && it instanceof ItemStateIterator) 
			resultIt = (ItemStateIterator)it;
		else
			resultIt = new ItemStateIterator();
		
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
	public Transition shallowCopy() {
		return new BasicTransition(this);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if(inputLabel == 0)
			sb.append(".");
		else {
			sb.append(inputLabel);
			if(inputLabelType == InputLabelType.SELF) 
				sb.append("=");
		}
		sb.append(":");
		switch(outputLabelType) {
		case SELF:
			sb.append("$");
			break;
		case SELF_ASCENDANTS:
			sb.append("$-" + outputLabel);
			break;
		case CONSTANT:
			sb.append(outputLabel);
			break;
		case EPSILON:
			sb.append("EPS");
			
		}
		return sb.toString();
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
		if (outputLabelType != other.outputLabelType)
			return false;
		return true;
	}
	
	

}
