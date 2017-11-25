package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.BasicDictionary;
import de.uni_mannheim.desq.util.IntBitSet;
import de.uni_mannheim.desq.util.IntSetOptimizer;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;

/** An uncaptured item, optionally with descendants: either A or A= */
class TransitionUncapturedItem extends Transition {
    private final int itemFid;
    private final String itemLabel;
    private final boolean matchDescendants;

    // helper
    private final IntSet matchedFids;

    /** Matches all descendents of given item */
    public TransitionUncapturedItem(final BasicDictionary dict, final State toState, final int fid,
                                    final String itemLabel, boolean matchDescendants) {
        super(dict,toState);
        this.itemFid = fid;
        this.itemLabel = itemLabel;
        this.matchDescendants = matchDescendants;
        this.matchedFids = getMatchedFids(dict, fid, matchDescendants);
    }

    /** Shallow copy */
    private TransitionUncapturedItem(TransitionUncapturedItem other) {
        super(other.dict, other.toState);
        this.itemFid = other.itemFid;
        this.itemLabel = other.itemLabel;
        this.matchDescendants = other.matchDescendants;
        this.matchedFids = other.matchedFids;
    }

    static IntSet getMatchedFids(BasicDictionary dict, int fid, boolean matchDescendants) {
        if (matchDescendants && !dict.isLeaf(fid)) {
            IntBitSet descendants = new IntBitSet(dict.lastFid()+1);
            dict.addDescendantFids(fid, descendants);
            descendants.add(fid);
            return IntSetOptimizer.optimize(descendants, true);
        } else {
            return IntSets.singleton(fid);
        }
    }

    @Override
    public boolean hasOutput() {
        return false;
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
        return matches(inputFid);
    }

    @Override
    public boolean firesAll(int largestFrequentItemFid) {
        return matchesAll();
    }

    @Override
    public IntIterator matchedFidIterator() {
        return matchedFids.iterator();
    }

    @Override
    public IntIterator firedFidIterator(int largestFrequentItemFid) {
        return matchedFidIterator();
    }

    @Override
    public boolean canProduce(int outputFid) {
        return false;
    }


    @Override
    public SingleItemStateIterator consume(final int fid, final ItemStateIteratorCache itCache) {
        final SingleItemStateIterator it = itCache.single;
        it.hasNext = matchedFids.contains(fid);
        it.itemState.itemFid = 0;
        it.itemState.state = toState;
        return it;
    }

    @Override
    public TransitionUncapturedItem shallowCopy() {
        return new TransitionUncapturedItem(this);
    }

    @Override
    public String itemExpression() {
        return itemLabel + (matchDescendants ? "" : "=)");
    }

    @Override
    public boolean isUncapturedDot() {
        return false;
    }
}
