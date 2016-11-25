package de.uni_mannheim.desq.fst;

import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.BitSet;

/** A state of a {@link Dfa}.
 *
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public final class DfaState {
    /** Next state for each input item (index = item fid) */
    DfaState[] transitionTable;

    /** FST states correponding to this DFA state (index = FST state number) */
    BitSet fstStates;

    /** True if one of the corresponding FST states is final */
    boolean isFinal = false;

    /** True if one of the corresponding FST states is final-complete */
    boolean isFinalComplete = false;

    public DfaState(IntSet stateIdSet, Fst fst, int size) {
        initTransitionTable(size);
        setFstStates(stateIdSet, fst);
    }

    private void initTransitionTable(int size) {
        transitionTable = new DfaState[size + 1];
    }

    public void addToTransitionTable(int itemFid, DfaState toState) {
        transitionTable[itemFid] = toState;
    }

    public DfaState consume(int itemFid) {
        return transitionTable[itemFid];
    }

    private void setFstStates(IntSet stateIdSet, Fst fst) {
        isFinal = isFinalComplete = false;
        this.fstStates = new BitSet();
        for (int stateId : stateIdSet) {
            fstStates.set(stateId);
            State state = fst.getState(stateId);
            if (state.isFinal()) {
                isFinal = true;
            }
            if (state.isFinalComplete()) {
                isFinalComplete = true;
            }
        }
    }

    public boolean isFinal() {
        return isFinal;
    }

    public boolean isFinalComplete() {
        return isFinalComplete;
    }

    public BitSet getFstStates() {
        return fstStates;
    }
}
