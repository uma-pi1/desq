package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.BasicDictionary;
import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSet;

/** A captured item, optionally with descendants: either (A) or (A=) */
final class TransitionCapturedItem extends Transition {
    private final int itemFid;
    private final boolean matchDescendants;
    private final String itemLabel;

    // helper
    final IntSet matchedFids;

    /** Matches all descendents of given item */
    public TransitionCapturedItem(final BasicDictionary dict, final State toState, final int fid,
                                  final String itemLabel, boolean matchDescendants) {
        super(dict,toState);
        this.itemFid = fid;
        this.itemLabel = itemLabel;
        this.matchDescendants = matchDescendants;
        this.matchedFids = TransitionUncapturedItem.getMatchedFids(dict, fid, matchDescendants);
    }

    /** Shallow copy */
    private TransitionCapturedItem(TransitionCapturedItem other) {
        super(other.dict, other.toState);
        this.itemFid = other.itemFid;
        this.itemLabel = other.itemLabel;
        this.matchDescendants = other.matchDescendants;
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
        return inputFid <= largestFrequentItemFid && matches(inputFid);
    }

    @Override
    public boolean firesAll(int largestFrequentItemFid) {
        return largestFrequentItemFid <= dict.lastFid() && matchesAll();
    }

    @Override
    public IntIterator matchedFidIterator() {
        return matchedFids.iterator();
    }

    @Override
    public IntIterator firedFidIterator(final int largestFrequentItemFid) {
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
                    if (nextFid <= largestFrequentItemFid)
                        return nextFid;
                }
                return (nextFid = -1);
            }
        };
    }


    @Override
    public SingleItemStateIterator consume(final int fid, final ItemStateIteratorCache itCache) {
        final SingleItemStateIterator it = itCache.single;
        it.hasNext = matchedFids.contains(fid);
        it.itemState.itemFid = fid;
        it.itemState.state = toState;
        return it;
    }

    @Override
    public TransitionCapturedItem shallowCopy() {
        return new TransitionCapturedItem(this);
    }

    @Override
    public String itemExpression() {
        return "(" + itemLabel + (matchDescendants ? ")" : "=");
    }

    @Override
    public boolean isUncapturedDot() {
        return false;
    }
}
