package de.uni_mannheim.desq.fst;

import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.Object2ShortMap;
import it.unimi.dsi.fastutil.objects.Object2ShortMaps;

import java.util.*;

/** A state of a {@link Dfa}.
 *
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 * @author Rainer Gemulla [rgemulla@uni-mannheim.de]
 */
public abstract class DfaState {
    private static final String[] EMPTY_STRING_ARRAY = new String[] {};
    private static final BitSet[] EMPTY_BITSET_ARRAY = new BitSet[] {};

    /** The DFA to which this state belongs. */
    Dfa dfa;

    /** FST states corresponding to this DFA state (index = FST state number) */
    BitSet fstStates;

    /** True if one of the corresponding FST states is final */
    boolean isFinal = false;

    /** True if one of the corresponding FST states is final-complete */
    boolean isFinalComplete = false;

    /** All DFA states reachable from this state. Position 0 is special (see {@link #indexByFid}) and may be null. */
    List<DfaState> reachableDfaStates = new ArrayList<>();

    /** For each active item, the index of the next DFA state in {@link #reachableDfaStates}. For eager DFAs, all items
     * stored in this map have at least an index of 1; for all other items, <code>reachableDfsStates[0]</code> is used.
     * For lazy DFAs, 0-indexes may also be stored. */
    Int2ShortMap indexByFid = Int2ShortMaps.EMPTY_MAP;

    /** The distinct labels of the outgoing transitions of the {@link #fstStates} corresponding to this DFA state.
     * Sorted lexicigraphically. */
    String[] transitionLabels = EMPTY_STRING_ARRAY;

    /** For each label in {@link #transitionLabels}, the set of FST states that can be reached for an item that matches
     * the corresponding transition. Parallel array to {@link #transitionLabels}. */
    BitSet[] toStatesByLabel = EMPTY_BITSET_ARRAY;

    /** For each label in {@link #transitionLabels}, an arbitrary FST transition with this label.
     * Parallel array to {@link #transitionLabels}. */
    Transition[] transitionByLabel = null;

    /** For each combination of transitions in {@link #transitionLabels} that can be fired jointly by an
     * item, the position of the next DFA state in {@link #reachableDfaStates}. */
    Object2ShortMap<BitSet> indexByFiredTransitions = Object2ShortMaps.EMPTY_MAP;


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

    /** Returns next state of <code>null</code> if none. */
    public abstract DfaState consume(int itemFid);

    /** Constructs the DFA from its initial state. Must only be called on the initial state. */
     abstract void construct();

    /** Iterate over all outgoing transitions in {@link #fstStates} and compute reachable FST states per
     * distinct	transition label. Updates {@link #transitionLabels}, {@link #toStatesByLabel}, and
     * {@link #transitionByLabel}.
     *
     * If <code>firedItemsByLabel</code> is non-null and we encounter a label that is not stored in
     * it, we also compute the set of items that fire the transition and store it. This is only used for eager DFAs. */
    void collectTransitions(BitSet defaultTransition, Map<String, IntList> firedItemsByLabel /* not used if null */) {
        // map from transition label to reachable FST states (excluding fires-all transitions)
        SortedMap<String, BitSet> toStatesByLabel = new TreeMap<>();
        SortedMap<String, Transition> transitionByLabel = new TreeMap<>();
        for (int stateId = fstStates.nextSetBit(0); // iterate over states
             stateId >= 0;
             stateId = fstStates.nextSetBit(stateId + 1)) {

            // ignore outgoing transitions from final complete states
            State state = dfa.fst.getState(stateId);
            if (state.isFinalComplete())
                continue;

            // iterate over transitions
            for (Transition t : state.getTransitions()) {
                if (t.firesAll(dfa.largestFrequentItemFid)) {
                    // this is an optmization which often helps when the pattern expression contains .
                    defaultTransition.set(t.getToState().getId());
                } else {
                    // otherwise we remember the transition
                    String label = t.toPatternExpression();
                    BitSet toStates = toStatesByLabel.computeIfAbsent(label, k -> new BitSet(dfa.fst.numStates()));
                    transitionByLabel.put(label, t);
                    toStates.set(t.getToState().getId());

                    // if it was a new label, compute the fired items
                    if (firedItemsByLabel!= null && !firedItemsByLabel.containsKey(label)) {
                        IntArrayList firedItems = new IntArrayList(dfa.dict.lastFid() + 1);
                        IntIterator it = t.matchedFidIterator();
                        while (it.hasNext()) {
                            int fid = it.nextInt();
                            boolean matches = !t.hasOutput() || t.matchesWithFrequentOutput(fid, dfa.largestFrequentItemFid);
                            if (matches) {
                                firedItems.add(fid);
                            }
                        }
                        firedItems.trim();
                        firedItemsByLabel.put(label, firedItems);
                        // System.out.println(label + " fires for " + firedItems.size() + " items");
                    }
                }
            }
        }

        // we are done, update the member variables
        if (!toStatesByLabel.isEmpty()) {
            // remember the remaining transitions
            this.transitionLabels = toStatesByLabel.keySet().toArray(new String[]{}); // sorted (since sorted map)
            this.toStatesByLabel = toStatesByLabel.values().toArray(new BitSet[]{}); // sorted conformingly
            this.transitionByLabel = transitionByLabel.values().toArray(new Transition[]{}); // sorted conformingly
        }

    }
}
