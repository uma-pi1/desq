package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.BasicDictionary;
import it.unimi.dsi.fastutil.ints.IntIterator;

/** An uncaptured dot: . */
final class TransitionUncapturedDot extends Transition {
    public TransitionUncapturedDot(final BasicDictionary dict, final State toState) {
        super(dict,toState);
    }

    @Override
    public boolean hasOutput() {
        return false;
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
        return true;
    }

    @Override
    public boolean firesAll(int largestFrequentItemFid) {
        return true;
    }

    @Override
    public IntIterator matchedFidIterator() {
        return dict.fidIterator();
    }

    @Override
    public IntIterator firedFidIterator(int largestFrequentItemFid) {
        return matchedFidIterator();
    }

    @Override
    public SingleItemStateIterator consume(final int fid, final ItemStateIteratorCache itCache) {
        final SingleItemStateIterator it = itCache.single;
        it.itemState.itemFid = 0;
        it.itemState.state = toState;
        it.hasNext = true;
        return it;
    }

    @Override
    public TransitionUncapturedDot shallowCopy() {
        return new TransitionUncapturedDot(dict, toState);
    }

    @Override
    public String itemExpression() {
        return ".";
    }

    @Override
    public boolean isUncapturedDot() {
        return true;
    }
}
