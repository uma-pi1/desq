package de.uni_mannheim.desq.fst;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.BitSet;

/**
 * Created by rgemulla on 02.12.2016.
 */
public final class EagerDfaState extends DfaState {
    /** From item id to next state */
    Int2ObjectMap<EagerDfaState> transitions = new Int2ObjectOpenHashMap<>();

    /** Next state for all items not in {@link #transitions} or null if those items are not matched. */
    EagerDfaState defaultTransition = null;

    public EagerDfaState(BitSet fstStates, Fst fst) {
        initialize(fstStates, fst);
    }

    public void setTransition(int itemFid, EagerDfaState toState) {
        transitions.put(itemFid, toState);
    }

    public void setDefaultTransition(EagerDfaState toState) {
        this.defaultTransition = toState;
    }

    public EagerDfaState consume(int itemFid) {
        EagerDfaState toState = transitions.get(itemFid);
        return toState != null ? toState : defaultTransition;
    }

    @Override
    public void freeze() {
        if (transitions.size()==0) {
            // this avoid hashing for lookups (mainly beneficial for . transistions)
            transitions = Int2ObjectMaps.EMPTY_MAP;
        }
    }
}
