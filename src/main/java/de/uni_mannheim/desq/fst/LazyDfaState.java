package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.util.CollectionUtils;

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
    }

    @Override
    public DfaState consume(int fid) {
        // get the index of the next DFA state
        int index = indexByFid.getInt(fid);

        // if we haven't seen this item, compute the next state
        if (index < 0) {
            // figure out which transitions fire for the given fid
            assert transitionLabels != null; // otherwise index would be 0
            firedTransitions.clear();
            for (int t = 0; t < transitionLabels.length; t++) { // iterate over transitions
                Transition transition = transitionByLabel[t];
                if (transition.fires(fid, dfa.largestFrequentItemFid)) {
                    firedTransitions.set(t);
                }
            }

            // if we haven't see this combination of fired transitions before, remoember it
            if (!firedTransitions.isEmpty()) {
                index = indexByFiredTransitions.getInt(firedTransitions);
                if (index == 0) {
                    // combination not yet encountered here; mark it for indexing
                    firedTransitionsByIndex.add(CollectionUtils.copyOf(firedTransitions));
                    index = firedTransitionsByIndex.size() - 1;
                    indexByFiredTransitions.put(firedTransitionsByIndex.get(index), index);
                }
            } else {
                // default transition
                index = 0;
            }

            // add the index
            indexByFid.set(fid, index);
        }

        // if we computed the fired transitions of an item for which we did not determine the reachable states,
        // do so now (this can happen above or by another state that shares index structures)
        while (index >= reachableDfaStates.size()) {
            assert transitionLabels != null; // otherwise index would be 0
            BitSet firedTransitions = firedTransitionsByIndex.get(reachableDfaStates.size());
            BitSet toStates = computeToStates(firedTransitions);
            reachableDfaStates.add(getDfaState(toStates));
        }

        // now index has the position of the next state
        return reachableDfaStates.get( index );
    }

    @Override
    void construct() {
        // set the initial state
        dfa.initial = this;
        dfa.states.clear();
        dfa.stateByTransitions.clear();
        dfa.states.put(fstStates, this);

        //  and collect the transitions of the initial state
        BitSet defaultTransition = new BitSet(dfa.fst.numStates());
        collectTransitions(defaultTransition, (short)-1, null);
        String key = transitionLabels != null ? String.join(" ", transitionLabels) : "null";
        dfa.stateByTransitions.put(key, this);
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
            dfaState.collectTransitions(defaultTransition, (short)-1, null);

            String key = dfaState.transitionLabels != null ? String.join(" ", dfaState.transitionLabels) : "null";
            DfaState similarState = dfa.stateByTransitions.get(key);
            if (similarState != null) {
                dfaState.indexByFid = similarState.indexByFid;
                dfaState.indexByFiredTransitions = similarState.indexByFiredTransitions;
                dfaState.firedTransitionsByIndex = similarState.firedTransitionsByIndex;
            } else {
                dfa.stateByTransitions.put(key, dfaState);
            }

            if (!defaultTransition.isEmpty()) {
                dfaState.reachableDfaStates.set(0, getDfaState(defaultTransition));
            }

        }
        return dfaState;
    }
}
