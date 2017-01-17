package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.BasicDictionary;
import it.unimi.dsi.fastutil.ints.IntIterator;

/** A captured dot: (.) */
final class TransitionCapturedDot extends Transition {
    public TransitionCapturedDot(final BasicDictionary dict, final State toState) {
        super(dict,toState);
    }

    @Override
    public boolean hasOutput() {
        return true;
    }

    @Override
    public boolean matches(int fid) {
        return true;
    }

    @Override
    public boolean matchesAll() {
        return true;
    }

    @Override
    public boolean fires(int inputFid, int largestFrequentItemFid) {
        return inputFid <= largestFrequentItemFid;
    }

    @Override
    public boolean firesAll(int largestFrequentItemFid) {
        return dict.lastFid() <= largestFrequentItemFid;
    }

    @Override
    public IntIterator matchedFidIterator() {
        return dict.fidIterator();
    }

    @Override
    public IntIterator firedFidIterator(int largestItemFid) {
        return dict.fidIterator(largestItemFid);
    }

    @Override
    public boolean canProduce(int outputFid) {
        return true;
    }

    @Override
    public SingleItemStateIterator consume(final int fid, final ItemStateIteratorCache itCache) {
        final SingleItemStateIterator it = itCache.single;
        it.itemState.itemFid = fid;
        it.itemState.state = toState;
        it.hasNext = true;
        return it;
    }

    @Override
    public TransitionCapturedDot shallowCopy() {
        return new TransitionCapturedDot(dict, toState);
    }

    @Override
    public String itemExpression() {
        return "(.)";
    }

    @Override
    public boolean isUncapturedDot() {
        return false;
    }
}
