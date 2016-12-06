package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.Dictionary;

import java.util.BitSet;

/** A state of a {@link Dfa}.
 *
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public abstract class DfaState {
    /** The DFA to which this state belongs. */
    Dfa dfa;

    /** FST states correponding to this DFA state (index = FST state number) */
    BitSet fstStates;

    /** True if one of the corresponding FST states is final */
    boolean isFinal = false;

    /** True if one of the corresponding FST states is final-complete */
    boolean isFinalComplete = false;

    public DfaState(Dfa dfa, BitSet fstStates) {
        this.dfa = dfa;
        this.fstStates = fstStates;

        // compute information about this state
        Fst fst = dfa.fst;
        isFinal = isFinalComplete = false;
        for (int stateId = fstStates.nextSetBit(0);
             stateId >= 0;
             stateId = fstStates.nextSetBit(stateId+1)) {
            State state = fst.getState(stateId);
            if (state.isFinal()) {
                isFinal = true;
            }
            if (state.isFinalComplete()) {
                isFinalComplete = true;
                break; // both isFinal and isFinalComplete are true now
            }
        }
    }

    public Dfa getDfa() {
        return dfa;
    }

    public BitSet getFstStates() {
        return fstStates;
    }

    public boolean isFinal() {
        return isFinal;
    }

    public boolean isFinalComplete() {
        return isFinalComplete;
    }

    public abstract DfaState consume(int itemFid);

    /** Constructs the DFA from its initial state. Must only be one the initial state. */
     abstract void construct(Dictionary dict, int largestFrequentItemFid, boolean processFinalCompleteStates);
}
