package de.uni_mannheim.desq.fst;

import it.unimi.dsi.fastutil.ints.Int2ShortMap;
import it.unimi.dsi.fastutil.ints.Int2ShortMaps;
import it.unimi.dsi.fastutil.ints.Int2ShortOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ShortMap;
import it.unimi.dsi.fastutil.objects.Object2ShortOpenHashMap;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Created by rgemulla on 02.12.2016.
 */
public final class EagerDfaState extends DfaState {
    /** From item id to position+1 of next state in {@link #toStates} */
    Int2ShortMap transitions = new Int2ShortOpenHashMap();

    /** All states reachable from this state */
    List<EagerDfaState> toStates = new ArrayList<>();

    /** Index of to states (from state to position+1 in list) */
    Object2ShortMap<EagerDfaState> toStatesIndex = new Object2ShortOpenHashMap<>();

    /** Next state for all items not in {@link #transitions} or null if those items are not matched. */
    EagerDfaState defaultTransition = null;

    public EagerDfaState(BitSet fstStates, Fst fst) {
        toStates.add(null); // position 0 not used
        initialize(fstStates, fst);
    }

    public void setTransition(int itemFid, EagerDfaState toState) {
        short toStatePos = toStatesIndex.getShort(toState);
        if (toStatePos == 0) { // not present
            toStates.add(toState);
            if (toStates.size() > Short.MAX_VALUE)
                throw new IllegalStateException("Only up to 32767 to-states supported");
            toStatePos = (short)(toStates.size()-1);
            toStatesIndex.put(toState, toStatePos);
        }
        transitions.put(itemFid, toStatePos);
    }

    public void setDefaultTransition(EagerDfaState toState) {
        this.defaultTransition = toState;
    }

    public EagerDfaState consume(int itemFid) {
        short toStatePos = transitions.get(itemFid);
        return toStatePos > 0 ? toStates.get(toStatePos) : defaultTransition;
    }

    @Override
    public void freeze() {
        if (transitions.size()==0) {
            // this avoid hashing for lookups (mainly beneficial for . transistions)
            transitions = Int2ShortMaps.EMPTY_MAP;
        }
    }
}
