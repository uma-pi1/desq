package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.BasicDictionary;
import it.unimi.dsi.fastutil.ints.*;

import java.util.Iterator;

/** A transition in an {@link de.uni_mannheim.desq.fst.Fst}. */
public abstract class Transition {
	/** The target state of the transition */
	State toState;

	/** The dictionary underlying the FST */
	protected final BasicDictionary dict;

	protected Transition(final BasicDictionary dict, final State toState) {
		this.dict = dict;
		this.toState = toState;
	}

	// -- steps -------------------------------------------------------------------------------------------------------

	/** Returns an iterator over the (output item, state)-pairs produced by this transition when consuming
	 * the given input item. The iterator instance is taken from <code>itCache</code>, which must not be null. */
	public abstract Iterator<ItemState> consume(int inputFid, ItemStateIteratorCache itCache);


	// -- information about the transition ----------------------------------------------------------------------------

	/** Whether or not this transition produces an output item when it fires. */
	public abstract boolean hasOutput();

	/** Returns true if this transition matches every input and never produces output (equivalent to
	 * <code>matchesAll() && !hasOutput()</code>).*/
	public abstract boolean isUncapturedDot();

	/** Whether {@link #consume(int, ItemStateIteratorCache)} has a non-empty result for the given input item. */
	public abstract boolean matches(int inputFid);

	/** Whether {@link #consume(int, ItemStateIteratorCache)} has a non-empty result for every input item. */
	public abstract boolean matchesAll();

	/** Whether {@link #consume(int, ItemStateIteratorCache)} has at least one result for the given input item, which
	 * either contains no output item (epsilon output) or a frequent output item (fid larger
	 * than <code>frequentItemFid</code>). */
	public abstract boolean fires(int inputFid, int largestFrequentItemFid);

	/** Whether {@link #consume(int, ItemStateIteratorCache)} has at least one result for every input item, which
	 * either contains no output item (epsilon output) or a frequent output item (fid larger
	 * than <code>frequentItemFid</code>). */
	public abstract boolean firesAll(int largestFrequentItemFid);

	/** Returns an iterator over all fids matched by this transition, i.e., for which {@link #matches(int)}
	 * returns <code>true</code>. */
	public abstract IntIterator matchedFidIterator();

	/** Returns an iterator over all fids for which this transition fires, i.e., for which {@link #fires(int,int)}
	 * returns <code>true</code>. */
	public IntIterator firedFidIterator(int largestFrequentItemFid) {
		return new AbstractIntIterator() {
			IntIterator matchedFidIt = matchedFidIterator();
			int nextFid = move();

			@Override
			public int nextInt() {
				assert hasNext();
				final int result = nextFid;
				move();
				return result;
			}

			@Override
			public boolean hasNext() {
				return nextFid >= 0;
			}

			private int move() {
				while (matchedFidIt.hasNext()) {
					nextFid = matchedFidIt.nextInt();
					if (fires(nextFid, largestFrequentItemFid))
						return nextFid;
				}
				return (nextFid = -1);
			}
		};
	}

	/** Returns true if when this transition can produce the given fid as an output item. In other words, returns
	 * whether there exists an input item for which {@link #consume(int, ItemStateIteratorCache)} produces the
	 * given output item.
	 */
	public abstract boolean canProduce(int outputFid);


	// -- utility methods ---------------------------------------------------------------------------------------------

	/** Returns a shallow copy of this transition, which may share all data but {@link #toState}. */
	public abstract Transition shallowCopy();

	/** Changes the target state of this transition */
	public void setToState(State state) {
		this.toState = state;
	}

	/** Returns the target state of this transition */
	public State getToState() {
		return toState;
	}

	/** Returns an item expression for this transition. */
	public abstract String itemExpression();

	@Override
	public String toString() {
		return itemExpression() + " -> " + toState.id;
	}

	/** Hash code of this transition (ignoring target state) */
	@Override
    public int hashCode() {
    	return itemExpression().hashCode();
	}

	/** Compares this transition with the given object. The target state is ignored. */
	public boolean equals(Object o) {
    	if (!(o instanceof Transition)) {
    		return false;
		}
		return itemExpression().equals(((Transition) o).itemExpression());
	}

	// -- iterator classes --------------------------------------------------------------------------------------------

	/** An iterator over a single item */
	final static class SingleItemStateIterator implements Iterator<ItemState> {
		ItemState itemState = new ItemState();
		boolean hasNext = true;

		public SingleItemStateIterator(final State toState) {
			itemState.itemFid = 0; // never outputs
			itemState.state = toState;
		}

		@Override
		public boolean hasNext() {
			return hasNext;
		}

		@Override
		public ItemState next() {
			assert hasNext;
			hasNext = false;
			return itemState;

		}
	}

	/** An iterator over an item and its ascendants */
	final static class AscendantsItemStateIterator implements Iterator<ItemState> {
		int nextFid;
		final ItemState itemState = new ItemState();
		IntIterator fidIterator;
		final IntCollection ascendants; // reusable; fidIterator goes over this set

		AscendantsItemStateIterator(boolean isForest) {
			ascendants = isForest ? new IntArrayList() : new IntAVLTreeSet();
		}

		@Override
		public boolean hasNext() {
			return nextFid >=0;
		}

		@Override
		public ItemState next() {
			assert nextFid >=0;
			itemState.itemFid = nextFid;
			if (fidIterator.hasNext())
				nextFid = fidIterator.nextInt();
			else
				nextFid = -1;
			return itemState;
		}

		public void reset(BasicDictionary dict, int itemFid, State toState) {
			nextFid = itemFid;
			itemState.state = toState;
			ascendants.clear();
			dict.addAscendantFids(itemFid, ascendants);
			fidIterator = ascendants.iterator();
		}
	}

	/** A cache of iterators for reuse. */
    public static final class ItemStateIteratorCache {
        final SingleItemStateIterator single = new SingleItemStateIterator(null);
        final AscendantsItemStateIterator ascendants;

		public ItemStateIteratorCache(boolean isForest) {
			ascendants = new AscendantsItemStateIterator(isForest);
		}
	}
}
