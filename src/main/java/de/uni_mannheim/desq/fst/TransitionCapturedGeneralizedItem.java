package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.BasicDictionary;
import de.uni_mannheim.desq.dictionary.RestrictedDictionary;
import de.uni_mannheim.desq.util.IntBitSet;
import it.unimi.dsi.fastutil.ints.IntIterator;

/** A captured item, descendants are generalized up to that item: (A^) */
final class TransitionCapturedGeneralizedItem extends Transition {
    private final int itemFid;
    private final String itemLabel;

    // helper
    private final RestrictedDictionary matchedDict;

    /** Matches all descendents of given item */
    public TransitionCapturedGeneralizedItem(final BasicDictionary dict, final State toState, final int fid,
                                             final String itemLabel) {
        super(dict,toState);
        this.itemFid = fid;
        this.itemLabel = itemLabel;
        IntBitSet matchedFids = new IntBitSet(dict.lastFid()+1);
        dict.addDescendantFids(fid, matchedFids);
        matchedFids.add(fid);
        matchedDict = new RestrictedDictionary(dict, matchedFids.bitSet());
    }

    /** Shallow copy */
    private TransitionCapturedGeneralizedItem(TransitionCapturedGeneralizedItem other) {
        super(other.dict, other.toState);
        this.itemFid = other.itemFid;
        this.itemLabel = other.itemLabel;
        this.matchedDict = other.matchedDict;
    }

    @Override
    public boolean hasOutput() {
        return true;
    }

    @Override
    public boolean matches(int fid) {
        return matchedDict.containsFid(fid);
    }

    @Override
    public boolean matchesAll() {
        return matchedDict.size() == dict.size();
    }

    @Override
    public boolean fires(int inputFid, int largestFrequentItemFid) {
        return matchedDict.hasAscendantWithFidBelow(inputFid, largestFrequentItemFid);
    }

    @Override
    public boolean firesAll(int largestFrequentItemFid) {
        return matchesAll() && matchedDict.largestRootFid() <= largestFrequentItemFid;
    }

    @Override
    public IntIterator matchedFidIterator() {
        return matchedDict.fidIterator();
    }

    @Override
    public AscendantsItemStateIterator consume(final int fid, final ItemStateIteratorCache itCache) {
        AscendantsItemStateIterator it = itCache.ascendants;
        if (matchedDict.containsFid(fid)) {
            it.reset(matchedDict, fid, toState);
        } else {
            it.nextFid = -1;
        }
        return it;
    }

    @Override
    public TransitionCapturedGeneralizedItem shallowCopy() {
        return new TransitionCapturedGeneralizedItem(this);
    }

    @Override
    public String itemExpression() {
        return "(" + itemLabel + "^)";
    }

    @Override
    public boolean isUncapturedDot() {
        return false;
    }
}
