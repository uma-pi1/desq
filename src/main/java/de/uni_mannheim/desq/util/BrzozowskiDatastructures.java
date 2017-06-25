package de.uni_mannheim.desq.util;

import de.uni_mannheim.desq.mining.OneNFA;
import de.uni_mannheim.desq.mining.OutputLabel;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.Object2IntAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.BitSet;

/**
 * Data structures for determinizing an OutputNFA once.
 * Can be reused for mutliple OutputNFAs, using clear()
 */
public class BrzozowskiDatastructures {

    /** For each state in the new NFA, this stores the included states of the original NFA */
    ObjectArrayList<IntSet> includedStates = new ObjectArrayList<>();

    /** Stores the outgoing edges of each state */
    ObjectArrayList<Object2IntAVLTreeMap<OutputLabel>> outgoingEdges = new ObjectArrayList<>();

    /** A map of all states by their included original states. We use this to check whether we already have a state
     * that includes a given set of original states */
    Object2IntOpenHashMap<IntSet> statesByIncludedOriginalStates = new Object2IntOpenHashMap<>();

    /** Stores the maximum pivot item for each state (=maximum item on path from state s to the final state) */
    IntArrayList maxPivot = new IntArrayList();

    /** The set of final states in this NFA */
    IntSet isFinal = new IntOpenHashSet();

    /** A reference to the original NFA */
    OneNFA nfa;

    public BrzozowskiDatastructures(OneNFA nfa) {
        this.nfa = nfa;
    }

    public void clear() {
        includedStates.clear();
        outgoingEdges.clear();
        statesByIncludedOriginalStates.clear();
        maxPivot.clear();
        isFinal.clear();
    }

    /** Add a new state that includes original states <code>inclStates</code> (original states = states of the original NFA) */
    public int addNewState(IntSet inclStates) {
        int numState = numStates();
        includedStates.add(inclStates);
        outgoingEdges.add(new Object2IntAVLTreeMap<>()); // TODO: probably better not to use tree maps here
        statesByIncludedOriginalStates.put(inclStates, numState);

        // maxPivot of this state is the maxPivot of all included states
        // also: check whether there is an initial state among the included states
        int maxPivot = -1;
        boolean containsInitial = false;
        for(int s : inclStates) {
            maxPivot = Math.max(maxPivot, nfa.getMaxPivot(s));
            containsInitial |= nfa.isInitial(s);
        }
        this.maxPivot.add(maxPivot);
        if(containsInitial)
            isFinal.add(numState);

        return numState;
    }

    /** Add an edge from state <code>from</code> to state <code>to</code> with label <code>label</code> */
    public void addEdge(int from, OutputLabel label, int to) {
        assert from!=to: "Can't add self-loop edges: " + from + "->" + to + " with label " + label;
        getOutgoingEdges(from).put(label, to);
    }


    public int getOrCreateState(IntSet inclStates) {
        int state = statesByIncludedOriginalStates.getOrDefault(inclStates, -1);
        if(state == -1) {
            state = addNewState(inclStates);
        }
        return state;
    }

    /** The number of states in this NFA */
    public int numStates() {
        return includedStates.size();
    }

    /** Returns the states of the original NFA that are included in state <code>state</code> */
    public IntSet getIncludedOriginalStates(int state) {
        return includedStates.get(state);
    }

    /** Returns the outgoing edges of state <code>state</code> */
    public Object2IntAVLTreeMap<OutputLabel> getOutgoingEdges(int state) {
        return outgoingEdges.get(state);
    }

    /** Returns the id of a the state that includes original states <code>inclStates</code>, if such a state exists.
     * Otherwise, it returns -1 */
    public int checkForExistingState(IntSet inclStates) {
        return statesByIncludedOriginalStates.getOrDefault(inclStates, -1);
    }

    public boolean isFinal(int state) {
        return isFinal.contains(state);
    }


    private BitSet processedWithPivotSeen = new BitSet();
    private BitSet processedWithNoPivotSeen = new BitSet();

    /** Returns all pivots of this NFA */
    public IntArrayList findPivots(IntList seq) {
        IntArrayList pivots = new IntArrayList();
        int currentPivot = maxPivot.getInt(0);
        while (currentPivot > 0 && currentPivot != Integer.MAX_VALUE) {
            processedWithPivotSeen.clear();
            processedWithNoPivotSeen.clear();
            pivots.add(currentPivot);
            findNextPivot(0, currentPivot, false);

            currentPivot = maxPivot.getInt(0);
        }
        return pivots;
    }

    /**
     * Finds the next pivot item.
     * Drops all occurences of the <code>currentPivot</code>, and updates the <code>maxPivot</code> entries accordingly.
     */
    private void findNextPivot(int state, int currentPivot, boolean haveSeenPivot) {
        // we do not need to process one state twice
        if((!haveSeenPivot && (processedWithNoPivotSeen.get(state) || processedWithPivotSeen.get(state))) ||
            (haveSeenPivot && processedWithPivotSeen.get(state))) {
            return;
        }
        // mark that we have processed this state
        if(haveSeenPivot) {
            processedWithPivotSeen.set(state);
        } else {
            processedWithNoPivotSeen.set(state);
        }

        OutputLabel ol;
        int toState;
        int newPivot = -3;
        maxPivot.set(state, -1); // TODO: is this necessary?

        // TODO: add this state to the extracted NFA

        // process all outgoing edges of this state: follow all paths that are relevant for the current pivot
        for(Object2IntMap.Entry<OutputLabel> tr : getOutgoingEdges(state).object2IntEntrySet()) {
            ol = tr.getKey();
            toState = tr.getIntValue();
            if(haveSeenPivot || // we have already seen the pivot on this path, so we add all subpaths to the NFA
                    maxPivot.getInt(toState) == currentPivot || // the to-state is labeled with the pivot
                    ol.getMaxOutputItem() == currentPivot || ol.getJustDroppedPivot() == currentPivot) { // the transition contains (or contained) the pivot

                // drop pivot item from transition if necessary
                if(ol.getMaxOutputItem() == currentPivot)
                    ol.dropMaxOutputItem();

                // follow this path
                if(!ol.isEmpty()) // we don't follow any transitions that are empty already
                    findNextPivot(toState, currentPivot, haveSeenPivot || ol.getJustDroppedPivot() == currentPivot);
            }

            // determine the maxPivot item coming out of this path
            int maxPivotThisTr = Math.max(ol.getMaxOutputItem(), maxPivot.getInt(toState));
            if(ol.isEmpty())
                maxPivotThisTr = -2; // ignore the value of this path if the path is irrelevant
            if(maxPivot.getInt(toState) < 0 && !isFinal(toState))
                maxPivotThisTr = -2; // ignore the value if the to-state is a dead end for this pivot (except if its final)

            newPivot = Math.max(maxPivotThisTr, newPivot);
        }
        // update the max pivot of the current state
        maxPivot.set(state, newPivot);
    }
}
