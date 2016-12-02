package de.uni_mannheim.desq.fst;

import java.util.BitSet;

/** A state of a {@link Dfa}.
 *
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public abstract class DfaState {
    /** FST states correponding to this DFA state (index = FST state number) */
    BitSet fstStates;

    /** True if one of the corresponding FST states is final */
    boolean isFinal = false;

    /** True if one of the corresponding FST states is final-complete */
    boolean isFinalComplete = false;

    protected void initialize(BitSet fstStates, Fst fst) {
        isFinal = isFinalComplete = false;
        this.fstStates = fstStates;
        for (int stateId = fstStates.nextSetBit(0);
             stateId >= 0;
             stateId = fstStates.nextSetBit(stateId+1)) {
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

    public abstract DfaState consume(int itemFid);

    /** Call when state is guaranteed to not be achanged anymore; may trigger internal optimizations. */
    public void freeze() { }
}
