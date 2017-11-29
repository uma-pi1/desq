package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.BasicDictionary;
import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSet;

/** A captured negated item, optionally with descendants: either (A) or (A=) */
final class TransitionUncapturedNegatedItem extends Transition {
    private final int itemFid;
    private final boolean skipDescendants;
    private final String itemLabel;

    // helper
    final IntSet skippedFids;

    /** Matches every item, except the given item */
    public TransitionUncapturedNegatedItem(final BasicDictionary dict, final State toState, final int fid,
                                           final String itemLabel, boolean skipDescendants) {
        super(dict,toState);
        this.itemFid = fid;
        this.itemLabel = itemLabel;
        this.skipDescendants = skipDescendants;
        this.skippedFids = TransitionUncapturedItem.getMatchedFids(dict, fid, skipDescendants);
    }

    /** Shallow copy */
    private TransitionUncapturedNegatedItem(TransitionUncapturedNegatedItem other) {
        super(other.dict, other.toState);
        this.itemFid = other.itemFid;
        this.itemLabel = other.itemLabel;
        this.skipDescendants = other.skipDescendants;
        this.skippedFids = other.skippedFids;
    }

    @Override
    public boolean hasOutput() {
        return false;
    }

    @Override
    public boolean matches(int fid) {
        return !skippedFids.contains(fid);
    }

    @Override
    public boolean matchesAll() {
        return skippedFids.isEmpty();
    }

    @Override
    public boolean fires(int inputFid, int largestFrequentItemFid) {
        return matches(inputFid);
    }

    @Override
    public boolean firesAll(int largestFrequentItemFid) {
        return matchesAll();
    }

    @Override
    public IntIterator matchedFidIterator() {
        return new AbstractIntIterator() {
            IntIterator allFidIt = dict.fidIterator();
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
                while (allFidIt.hasNext()) {
                    nextFid = allFidIt.nextInt();
                    if (!skippedFids.contains(nextFid))
                        return nextFid;
                }
                return (nextFid = -1);
            }
        };
    }

    @Override
    public IntIterator firedFidIterator(final int largestFrequentItemFid) {
        return matchedFidIterator();
    }

    @Override
    public boolean canProduce(int outputFid) {
        return false;
    }


    @Override
    public SingleItemStateIterator consume(final int fid, final ItemStateIteratorCache itCache) {
        final SingleItemStateIterator it = itCache.single;
        it.hasNext = !skippedFids.contains(fid);
        it.itemState.itemFid = 0;
        it.itemState.state = toState;
        return it;
    }

    @Override
    public TransitionUncapturedNegatedItem shallowCopy() {
        return new TransitionUncapturedNegatedItem(this);
    }

    @Override
    public String itemExpression() {
        return "-" + itemLabel + (skipDescendants ? "" : "=");
    }

    @Override
    public boolean isUncapturedDot() {
        return false;
    }
}
