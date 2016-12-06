package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.util.IntSetUtils;
import it.unimi.dsi.fastutil.ints.Int2ShortOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ShortOpenHashMap;

import java.util.BitSet;

/** Computes DFA states as needed. Less memory than eager DFA, but (significantly) slower.
 *
 * Created by rgemulla on 06.12.2016.
 */
public class LazyDfaState extends DfaState {
    BitSet firedTransitions = new BitSet(); // use as cache

    public LazyDfaState(Dfa dfa, BitSet fstStates) {
        super(dfa, fstStates);
        reachableDfaStates.add(null); // stores default transition
        indexByFid = new Int2ShortOpenHashMap();
        indexByFiredTransitions = new Object2ShortOpenHashMap<BitSet>();
    }

    @Override
    public DfaState consume(int fid) {
        // get the index of the next DFA state (if computed already)
        short index = indexByFid.get(fid);

        // compute the next state if not done before
        // note: if index == 0 and one of the next two conditions does not hold, we always use the default transition
        if (index == 0 && transitionLabels.length>0 && !indexByFid.containsKey(fid)) {
            // figure out which transitions fire for the given fid
            firedTransitions.clear();
            for (int t = 0; t < transitionLabels.length; t++) { // iterate over transitions
                Transition transition = transitionByLabel[t];
                if (transition.fires(fid, dfa.largestFrequentItemFid)) {
                    firedTransitions.set(t);
                }
            }

            // now add the transition
            if (firedTransitions.isEmpty()) {
                // just the default transition fires
                indexByFid.put(fid, index);
            } else {
                // else get the DFA state for the set of fired transitions
                index = indexByFiredTransitions.getShort(firedTransitions);
                if (index == 0) {
                    // DFA state not yet encountered here; compute it
                    BitSet toStates = new BitSet();
                    DfaState defaultToState = reachableDfaStates.get(0);
                    if (defaultToState != null)
                        toStates.or(defaultToState.fstStates); // always fires
                    for (int t = firedTransitions.nextSetBit(0);
                         t >= 0;
                         t = firedTransitions.nextSetBit(t + 1)) {
                        toStates.or(toStatesByLabel[t]);
                    }

                    reachableDfaStates.add(getDfaState(toStates));
                    if (reachableDfaStates.size() > Short.MAX_VALUE)
                        throw new IllegalStateException("Only up to 32767 to-states supported");
                    index = (short) (reachableDfaStates.size() - 1);
                    indexByFiredTransitions.put(IntSetUtils.copyOf(firedTransitions), index);
                }
                indexByFid.put(fid, index);
            }
        }

        return reachableDfaStates.get( index );
    }

    @Override
    void construct() {
        //  just collect the transitions of the initial state
        dfa.initial = this;
        dfa.states.clear();
        dfa.states.put(fstStates, this);
        BitSet defaultTransition = new BitSet(dfa.fst.numStates());
        collectTransitions(defaultTransition, null);
        if (!defaultTransition.isEmpty()) {
            reachableDfaStates.set(0, getDfaState(defaultTransition));
        }
    }

    /** Returns the DFA state for the given set of FST states. If this DFA state has not been created, creates it and
     * adds it to the DFA. */
    private DfaState getDfaState(BitSet fstStates) {
        DfaState dfaState = dfa.states.get(fstStates);
        if (dfaState == null) {
            dfaState = new LazyDfaState(dfa, fstStates);
            dfa.states.put(fstStates, dfaState);
            BitSet defaultTransition = new BitSet(dfa.fst.numStates());
            dfaState.collectTransitions(defaultTransition, null);
            if (!defaultTransition.isEmpty()) {
                dfaState.reachableDfaStates.set(0, getDfaState(defaultTransition));
            }
            // TODO: share indexByFid and indexByFiredTransitions between states with same set of FST transition labels
        }
        return dfaState;
    }
}
