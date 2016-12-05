package de.uni_mannheim.desq.fst;

import it.unimi.dsi.fastutil.ints.Int2ShortMap;
import it.unimi.dsi.fastutil.ints.Int2ShortMaps;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Created by rgemulla on 02.12.2016.
 */
public final class EagerDfaState extends DfaState {
    /** From item id to position of next state in {@link #toStates} or 0 if not present */
    Int2ShortMap transitions = Int2ShortMaps.EMPTY_MAP;

    /** All states reachable from this state (first element not used and wired to null) */
    List<EagerDfaState> toStates = new ArrayList<>();

    /** Next state for all items not in {@link #transitions} or null if those items are not matched. */
    EagerDfaState defaultTransition = null;

    public EagerDfaState(BitSet fstStates, Fst fst) {
        toStates.add(null); // position 0 not used
        initialize(fstStates, fst);
    }

    public EagerDfaState consume(int itemFid) {
        short toStatePos = transitions.get(itemFid);
        return toStatePos > 0 ? toStates.get(toStatePos) : defaultTransition;
    }
}
