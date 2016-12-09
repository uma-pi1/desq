package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.util.IntByteArrayList;
import de.uni_mannheim.desq.util.IntConstantList;
import de.uni_mannheim.desq.util.IntShortArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.*;

/** A state of a {@link Dfa}.
 *
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 * @author Rainer Gemulla [rgemulla@uni-mannheim.de]
 */
public abstract class DfaState {
    /** The DFA to which this state belongs. */
    Dfa dfa;

    /** FST states corresponding to this DFA state (index = FST state number) */
    BitSet fstStates;

    /** True if one of the corresponding FST states is final */
    boolean isFinal = false;

    /** True if one of the corresponding FST states is final-complete */
    boolean isFinalComplete = false;

    /** All DFA states reachable from this state. Position 0 is special: It stores the next state for all items
     * that are not matched by a transition in {@link #transitionByLabel} and may be <code>null</code>. */
    ArrayList<DfaState> reachableDfaStates = new ArrayList<>();

    /** For each item (fid), the index of the next DFA state in {@link #reachableDfaStates}. If
     * {@link #transitionByLabel} is empty, this list is not used and set to <code>null</code>. In this case,
     * the next state is given by <code>reachableDfaStates.get(0)</code>.
     *
     * Shared by all states with the same outgoing transition labels.
     */
    IntList indexByFid = null;

    /** The distinct labels of the outgoing transitions of the {@link #fstStates} corresponding to this DFA state.
     * Only transitions that do not fire on all items are relevant and stored here. Sorted lexicigraphically.
     * Can be <code>null</code> (no such transitions).
     *
     * Shared by all states with the same outgoing transition labels.
     */
    String[] transitionLabels = null;

    /** For each label in {@link #transitionLabels}, the set of FST states that can be reached for an item that matches
     * the corresponding transition. Parallel array to {@link #transitionLabels}. Can be <code>null</code> (no
     * such transitions).
     */
    BitSet[] toStatesByLabel = null;

    /** For each label in {@link #transitionLabels}, an arbitrary FST transition with this label.
     * Parallel array to {@link #transitionLabels}.
     *
     * Shared by all states with the same outgoing transition labels.
     */
    Transition[] transitionByLabel = null;

    /** For each combination of transitions in {@link #transitionLabels} that can be fired jointly by an
     * item, the position of the next DFA state in {@link #reachableDfaStates}. Can be <code>null</code> (no
     * such transitions).
     *
     * Shared by all states with the same outgoing transition labels.
     */
    Object2IntOpenHashMap<BitSet> indexByFiredTransitions = null;

    /** Inverse of {@link #indexByFiredTransitions}. Can be null.
     *
     * Shared by all states with the same outgoing transition labels.
     */
    List<BitSet> firedTransitionsByIndex = null;


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
    void collectTransitions(BitSet defaultTransition,
                            short indexByFidDefaultValue,
                            Map<String, IntList> firedItemsByLabel /* not used if null */) {
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
        int n = dfa.dict.lastFid()+1;
        if (!toStatesByLabel.isEmpty()) {
            // remember the remaining transitions
            this.transitionLabels = toStatesByLabel.keySet().toArray(new String[]{}); // sorted (since sorted map)
            this.toStatesByLabel = toStatesByLabel.values().toArray(new BitSet[]{}); // sorted conformingly
            this.transitionByLabel = transitionByLabel.values().toArray(new Transition[]{}); // sorted conformingly
            this.indexByFiredTransitions = new Object2IntOpenHashMap<>();
            this.firedTransitionsByIndex = new ArrayList<>();
            this.firedTransitionsByIndex.add(null); // unused / placeholser

            // and initialize the index
            if (transitionLabels.length <=7) {
                indexByFid = new IntByteArrayList(n);
            } else if (transitionLabels.length <= 15) {
                indexByFid = new IntShortArrayList(n);
            } else {
                indexByFid = new IntArrayList(n);
            }

            for (int i=0; i<n; i++) {
                indexByFid.add(indexByFidDefaultValue);
            }
        } else {
            indexByFid = new IntConstantList(n, 0); // always use default transition
        }
    }

    BitSet computeToStates(BitSet firedTransitions) {
        BitSet toStates = new BitSet();
        DfaState defaultToState = reachableDfaStates.get(0);
        if (defaultToState != null)
            toStates.or(defaultToState.fstStates); // always fires
        for (int t = firedTransitions.nextSetBit(0);
             t >= 0;
             t = firedTransitions.nextSetBit(t + 1)) {
            toStates.or(toStatesByLabel[t]);
        }
        return toStates;
    }
}
