package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.BasicDictionary;
import it.unimi.dsi.fastutil.ints.IntIterator;

/** A captured dot with generalization: (.^) */
final class TransitionCapturedGeneralizedDot extends Transition {
    public TransitionCapturedGeneralizedDot(final BasicDictionary dict, final State toState) {
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
        return dict.hasAscendantWithFidBelow(inputFid, largestFrequentItemFid);
    }

    @Override
    public boolean firesAll(int largestFrequentItemFid) {
        return dict.largestRootFid() <= largestFrequentItemFid;
    }

    @Override
    public IntIterator matchedFidIterator() {
        return dict.fidIterator();
    }

    @Override
    public AscendantsItemStateIterator consume(final int fid, final ItemStateIteratorCache itCache) {
        AscendantsItemStateIterator it = itCache.ascendants;
        it.reset(dict, fid, toState);
        return it;
    }

    @Override
    public TransitionCapturedGeneralizedDot shallowCopy() {
        return new TransitionCapturedGeneralizedDot(dict, toState);
    }

    @Override
    public String itemExpression() {
        return "(.^)";
    }

    @Override
    public boolean isUncapturedDot() {
        return false;
    }
}
