package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.BasicDictionary;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;
import it.unimi.dsi.fastutil.ints.IntSet;

/** A captured item, descendants are generalized to that item: (A=^) */
final class TransitionCapturedConstant extends Transition {
    private final int itemFid;
    private final String itemLabel;

    // helper
    private final IntSet matchedFids;

    /** Matches all descendents of given item */
    public TransitionCapturedConstant(final BasicDictionary dict, final State toState, final int fid,
                                      final String itemLabel) {
        super(dict,toState);
        this.itemFid = fid;
        this.itemLabel = itemLabel;
        this.matchedFids = TransitionUncapturedItem.getMatchedFids(dict, fid, true);
    }

    /** Shallow copy */
    private TransitionCapturedConstant(TransitionCapturedConstant other) {
        super(other.dict, other.toState);
        this.itemFid = other.itemFid;
        this.itemLabel = other.itemLabel;
        this.matchedFids = other.matchedFids;
    }

    @Override
    public boolean hasOutput() {
        return true;
    }

    @Override
    public boolean matches(int fid) {
        return matchedFids.contains(fid);
    }

    @Override
    public boolean matchesAll() {
        return matchedFids.size() == dict.size();
    }

    @Override
    public boolean fires(int inputFid, int largestFrequentItemFid) {
        return itemFid <= largestFrequentItemFid && matches(inputFid);
    }

    @Override
    public boolean firesAll(int largestFrequentItemFid) {
        return itemFid <= largestFrequentItemFid && matchesAll();
    }

    @Override
    public IntIterator matchedFidIterator() {
        return matchedFids.iterator();
    }

    @Override
    public IntIterator firedFidIterator(int largestFrequentItemFid) {
        if (itemFid <= largestFrequentItemFid) {
            return matchedFids.iterator();
        } else {
            return IntIterators.EMPTY_ITERATOR;
        }
    }

    @Override
    public boolean canProduce(int outputFid) {
        return itemFid == outputFid;
    }

    @Override
    public SingleItemStateIterator consume(final int fid, final ItemStateIteratorCache itCache) {
        final SingleItemStateIterator it = itCache.single;
        it.hasNext = matchedFids.contains(fid);
        it.itemState.itemFid = itemFid;
        it.itemState.state = toState;
        return it;
    }

    @Override
    public TransitionCapturedConstant shallowCopy() {
        return new TransitionCapturedConstant(this);
    }

    @Override
    public String itemExpression() {
        return "(" + itemLabel + "=^)";
    }

    @Override
    public boolean isUncapturedDot() {
        return false;
    }
}
