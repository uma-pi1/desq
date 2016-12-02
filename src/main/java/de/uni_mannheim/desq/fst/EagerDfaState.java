package de.uni_mannheim.desq.fst;

import java.util.BitSet;

/**
 * Created by rgemulla on 02.12.2016.
 */
public final class EagerDfaState extends DfaState {
    /** Next state for each input item (index = item fid) */
    EagerDfaState[] transitionTable;


    public EagerDfaState(BitSet fstStates, Fst fst, int size) {
        transitionTable = new EagerDfaState[size + 1];
        initialize(fstStates, fst);
    }

    public void setTransition(int itemFid, EagerDfaState toState) {
        transitionTable[itemFid] = toState;
    }

    public EagerDfaState consume(int itemFid) {
        return transitionTable[itemFid];
    }
}
