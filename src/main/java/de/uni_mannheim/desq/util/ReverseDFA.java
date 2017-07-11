package de.uni_mannheim.desq.util;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.mining.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.*;

import java.util.BitSet;

/**
 * Data structures for determinizing an OutputNFA once.
 * Can be reused for mutliple OutputNFAs, using clear()
 */
public class ReverseDFA {

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
    QPGrid grid;

    // -------- for extracting NFAs -------------------
    /** Stores whether we have already processed a state with having seen a pivot so far */
    private BitSet processedWithPivotSeen = new BitSet();

    /** Stores whether we have already processed a state _without_ having seen a pivot so far */
    private BitSet processedWithNoPivotSeen = new BitSet();

    /** Stores the max. pivot items of the previous search. We use this when extracting NFAs because we already start modifying the original maxPivots */
    private int[] previousMaxPivots;

    /** Stores the (backward) NFA extracted for the current pivot item */
    ExtractedNFA extractedNFA = new ExtractedNFA(true);

    /** Stores the (forward) DFA for the current pivot item */
    ExtractedNFA forwardDFA = new ExtractedNFA(false);

    /** A list of output labels from which we need to drop the pivot item */
    ReferenceSet<IntArrayList> outputItemsWithPivotItem = new ReferenceOpenHashSet<>();

    public ReverseDFA(QPGrid grid) {
        this.grid = grid;
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
            maxPivot = Math.max(maxPivot, grid.getMaxPivot(s));
            containsInitial |= grid.isInitial(s);
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



    /** Finds the pivot items from this DFA and extracts one DFA per pivot item */
    public void constructNFAs(ObjectArrayList<Sequence> serializedNFAs, Fst fst) {
        previousMaxPivots = new int[numStates()];
        int currentPivot = maxPivot.getInt(0);

        // for each found pivot item p, we run through the grid once to do 2 things:
        //   1) determine the next pivot item
        //   2) extract a DFA for the current pivot item p
        while (currentPivot > 0 && currentPivot != Integer.MAX_VALUE) {
            // copy the current maxPivot's (because we start modifying maxPivot, but need the old values
            System.arraycopy(maxPivot.elements(), 0, previousMaxPivots, 0, numStates());

            // clear variables for this pivot search
            extractedNFA.clear();
            processedWithPivotSeen.clear();
            processedWithNoPivotSeen.clear();
            outputItemsWithPivotItem.clear();

            // create a state in the extracted DFA
            IntSet startStates = new IntOpenHashSet();
            startStates.add(0);
            int extractedState = extractedNFA.getOrCreateState(startStates);

            // run through the DFA recursively to find next pivot item and to extract DFA for current pivot
            extractStep(0, extractedState, currentPivot, false);

            // reverse and determinize the extracted NFA for the current pivot
            forwardDFA.clear();
            forwardDFA = extractedNFA.reverseAndDeterminize(forwardDFA);

            // export PDFs
//            extractedNFA.exportPDF(seq + "-piv" + lastPivot + "-extracted.pdf");
//            forwardDFA.exportPDF(seq + "-piv" + lastPivot + "-forward.pdf");

            // serialize the reversed and determinized DFA
            WeightedSequence send = forwardDFA.serialize(fst, currentPivot);
            send.add(currentPivot); // we append the pivot item to the end
            serializedNFAs.add(send);

            // drop the pivot item from the output labels
            // (need to do this after we extract+serialize the DFA for the current pivot)
            for(IntArrayList outputItems : outputItemsWithPivotItem) {
                if(outputItems.size() > 0 && outputItems.getInt(outputItems.size()-1) == currentPivot) // can probably be removed
                    outputItems.size(outputItems.size()-1);
            }
            outputItemsWithPivotItem.clear();

            // the next pivot item is the maxPivot at the starting state
            currentPivot = maxPivot.getInt(0);
        }
    }


    /**
     * Extracts relevant parts for one state in this DFA. Does multiple things:
     * 1) Marks occurences of the current pivot item in outgoing transitions of this state for deletion
     * 2) Updates the maxPivot entries of this state (to find the next pivot item)
     * 3) Extracts relevant parts for the DFA of the current pivot item
     */
    private void extractStep(int state, int extractedState, int currentPivot, boolean haveSeenPivot) {
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

        // process all outgoing edges of this state: follow all paths that are relevant for the current pivot
        for(Object2IntMap.Entry<OutputLabel> tr : getOutgoingEdges(state).object2IntEntrySet()) {
            ol = tr.getKey();
            toState = tr.getIntValue();
            boolean pivotInThisTr = false;
            if(haveSeenPivot || // we have already seen the pivot on this path, so we add all subpaths to the NFA
                    previousMaxPivots[toState] == currentPivot || // the to-state was labeled with the pivot item last round
                    ol.getMaxOutputItem() == currentPivot) { // the transition contains (or contained) the pivot

                // if the the pivot item occurs in this transition, we delete it
                // (we first mark it for deletion and delete it after we extracted the DFA for the current pivot)
                if(ol.getMaxOutputItem() == currentPivot) {
                    outputItemsWithPivotItem.add(ol.getOutputItems());
                    pivotInThisTr = true;
                }

                // extract this transition and follow it if it contains output items
                if(!ol.isEmpty()) {
                    // copy this transition to the NFA for the current pivot item
                    IntSet toStateSet = new IntOpenHashSet();
                    toStateSet.add(toState); // TODO: this is stupid. improve
                    int extractedTo = extractedNFA.getOrCreateState(toStateSet);
                    extractedNFA.addEdge(extractedState, ol, extractedTo, isFinal(toState) && (haveSeenPivot || pivotInThisTr));

                    // follow the transition
                    extractStep(toState, extractedTo, currentPivot, haveSeenPivot || pivotInThisTr);
                }
            }

            // determine the maxPivot item coming out of this path
            int maxPivotThisTr = Math.max(ol.getMaxOutputItemAfterPivot(currentPivot), maxPivot.getInt(toState));
            if(ol.isEmpty() || ol.getMaxOutputItemAfterPivot(currentPivot) == -1)
                maxPivotThisTr = -2; // ignore the value of this path if the path is irrelevant
            if(maxPivot.getInt(toState) < 0 && !isFinal(toState))
                maxPivotThisTr = -2; // ignore the value if the to-state is a dead end for this pivot (except if its final)

            newPivot = Math.max(maxPivotThisTr, newPivot);
        }
        // update the max pivot of the current state
        maxPivot.set(state, newPivot);
    }
}
