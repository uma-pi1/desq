package de.uni_mannheim.desq.util;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.graphviz.AutomatonVisualizer;
import de.uni_mannheim.desq.mining.*;
import de.uni_mannheim.desq.mining.distributed.ExtractedNfa;
import de.uni_mannheim.desq.mining.distributed.OutputLabel;
import de.uni_mannheim.desq.mining.distributed.QPGrid;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;
import it.unimi.dsi.fastutil.objects.*;
import org.apache.commons.io.FilenameUtils;

import java.util.BitSet;

/**
 * Data structures for determinizing an OutputNfa once.
 * Can be reused for mutliple OutputNFAs, using clear()
 */
public class ReverseDfa {

    /** For each state in the new NFA, this stores the included states of the original NFA */
    ObjectArrayList<IntSet> includedStates = new ObjectArrayList<>();

    /** Stores the outgoing edges of each state */
    ObjectArrayList<Object2IntOpenHashMap<OutputLabel>> outgoingEdges = new ObjectArrayList<>();

    /** Stores the outgoing edges after we have collected all of them */
    ObjectArrayList<IntArrayList> outgoingToStates = new ObjectArrayList<>();
    ObjectArrayList<ObjectArrayList<OutputLabel>> outgoingLabels = new ObjectArrayList<>();

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
    ExtractedNfa extractedNfa = new ExtractedNfa(true);

    /** Stores the (forward) DFA for the current pivot item */
    ExtractedNfa forwardDFA = new ExtractedNfa(false);

    /** A list of output labels from which we need to drop the pivot item */
    ObjectList<IntArrayList> outputItemsWithPivotItem = new ObjectArrayList<>();

    /** A list of edges to drop after we have extracted the NFA for the current pivot item */
    LongSortedSet edgesToDelete = new LongAVLTreeSet();

    public ReverseDfa(QPGrid grid) {
        this.grid = grid;
    }

    public void clear() {
        includedStates.clear();
//        outgoingEdges.clear(); // we reuse the objects here, so we don't clear
//        outgoingToStates.clear(); // we reuse the objects
//        outgoingLabels.clear();
        statesByIncludedOriginalStates.clear();
        maxPivot.clear();
        isFinal.clear();
        edgesToDelete.clear();
    }

    /** Add a new state that includes original states <code>inclStates</code> (original states = states of the original NFA) */
    public int addNewState(IntSet inclStates) {
        int numState = numStates();
        includedStates.add(inclStates);
        if(outgoingEdges.size() > numState)
            // reuse old object
            outgoingEdges.get(numState).clear();
        else
            outgoingEdges.add(new Object2IntOpenHashMap<>());


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
    public Object2IntOpenHashMap<OutputLabel> getOutgoingEdges(int state) {
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

        // First, we move the outgoing edges into two parallel array lists
        outgoingLabels.ensureCapacity(numStates());
        outgoingToStates.ensureCapacity(numStates());
        for(int s=0; s<numStates(); s++) {
            if(outgoingLabels.size() > s) {
                outgoingLabels.get(s).clear();
                outgoingToStates.get(s).clear();
            } else {
                outgoingLabels.add(new ObjectArrayList<>());
                outgoingToStates.add(new IntArrayList());
            }
            outgoingLabels.get(s).ensureCapacity(outgoingEdges.get(s).size());
            outgoingToStates.get(s).ensureCapacity(outgoingEdges.get(s).size());
            for(Object2IntMap.Entry<OutputLabel> entry : outgoingEdges.get(s).object2IntEntrySet()) {
                outgoingLabels.get(s).add(entry.getKey());
                outgoingToStates.get(s).add(entry.getIntValue());
            }
        }


        // for each found pivot item p, we run through the grid once to do 2 things:
        //   1) determine the next pivot item
        //   2) extract a DFA for the current pivot item p
        while (currentPivot > 0 && currentPivot != Integer.MAX_VALUE) {
            // copy the current maxPivot's (because we start modifying maxPivot, but need the old values
            System.arraycopy(maxPivot.elements(), 0, previousMaxPivots, 0, numStates());

            // clear variables for this pivot search
            extractedNfa.clear();
            processedWithPivotSeen.clear();
            processedWithNoPivotSeen.clear();
            outputItemsWithPivotItem.clear();

            // create a state in the extracted DFA
            IntSet startStates = new IntOpenHashSet();
            startStates.add(0);
            int extractedState = extractedNfa.getOrCreateState(startStates);

            // run through the DFA recursively to find next pivot item and to extract DFA for current pivot
            extractStep(0, extractedState, currentPivot, false);

            // reverse and determinize the extracted NFA for the current pivot
            forwardDFA.clear();
            forwardDFA = extractedNfa.reverseAndDeterminize(forwardDFA);

            // export PDFs
//            extractedNfa.exportPDF(seq + "-piv" + currentPivot + "-extracted.pdf");
//            forwardDFA.exportPDF(seq + "-piv" + currentPivot + "-forward.pdf");
//            exportCurrentToPDF(seq + "-reverse-after-" + currentPivot + ".pdf");

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


            // delete edges
            for(long stateEdge : edgesToDelete) {
                int e = -PrimitiveUtils.getLeft(stateEdge);
                int state = PrimitiveUtils.getRight(stateEdge);
                int numEdges = outgoingLabels.get(state).size();

                // if the edge we want to delete is not the last one, we move the last edge to the one we want to delete
                if(e != numEdges-1) {
                    outgoingLabels.get(state).set(e, outgoingLabels.get(state).get(numEdges-1));
                    outgoingToStates.get(state).set(e, outgoingToStates.get(state).getInt(numEdges-1));
                }

                // delete the last edge
                outgoingLabels.get(state).size(numEdges-1);
                outgoingToStates.get(state).size(numEdges-1);
            }
            edgesToDelete.clear();

            // the next pivot item is the maxPivot at the starting state
            currentPivot = maxPivot.getInt(0);

        }
    }

    private void printMaxPiv() {
        for(int i=0; i<maxPivot.size(); i++) {
            System.out.print(i + ":" + maxPivot.getInt(i) + " ");
        }
        System.out.println("");
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
        for(int e = 0; e<outgoingLabels.get(state).size(); e++) { // e ~ edge number
            ol = outgoingLabels.get(state).get(e);
            toState = outgoingToStates.get(state).getInt(e);
            boolean pivotInThisTr = false;

            if((haveSeenPivot || // we have already seen the pivot on this path, so we add all subpaths to the NFA
               previousMaxPivots[toState] == currentPivot || // the to-state was labeled with the pivot item last round
               ol.getMaxOutputItem() == currentPivot) // the transition contains the pivot
               && (previousMaxPivots[toState] >= 0 || isFinal(toState)) ) {  // we don't follow this path if it is marked as a dead end // TODO: is not necessary anymore when we delete edges

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
                    int extractedTo = extractedNfa.getOrCreateState(toStateSet);
                    extractedNfa.addEdge(extractedState, ol, extractedTo, isFinal(toState) && (haveSeenPivot || pivotInThisTr));

                    // follow the transition
                    extractStep(toState, extractedTo, currentPivot, haveSeenPivot || pivotInThisTr);
                }
            }

            // determine the maxPivot item coming out of this path
            int maxPivotThisTr = Math.max(ol.getMaxOutputItemAfterPivot(currentPivot), maxPivot.getInt(toState));

            // we delete this edge if one of the following is true:
            // 1) the label of this outgoing transition is empty
            // 2) the label of this outgoing transition is empty after removing the current pivot item
            // 3) the to-state has no valid outgoing edges and is not a final state
            if(ol.isEmpty() || ol.getMaxOutputItemAfterPivot(currentPivot) == -1 || (maxPivot.getInt(toState) < 0 && !isFinal(toState))) {
                maxPivotThisTr = -2; // ignore the value of this path if the path is irrelevant

                // we further mark this edge for deletion
                edgesToDelete.add(PrimitiveUtils.combine(-e, state));
            }

            newPivot = Math.max(maxPivotThisTr, newPivot);
        }
        // update the max pivot of the current state
        maxPivot.set(state, newPivot);
    }

    public void exportCurrentToPDF(String file) {
        AutomatonVisualizer automatonVisualizer = new AutomatonVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
        automatonVisualizer.beginGraph();
        for (int s = 0; s < numStates(); s++) {
            for(int e = 0; e< outgoingLabels.get(s).size(); e++) { // e ~ edge number
                OutputLabel ol = outgoingLabels.get(s).get(e);
                int toState = outgoingToStates.get(s).getInt(e);
                String label;
                label = (ol == null ? " " : ol.getOutputItems().toString() + "(" + ol.getInputItem() + ")");
                if(!ol.isEmpty())
                    automatonVisualizer.add(String.valueOf(s), label, String.valueOf(toState));
            }

            if (isFinal(s))
                automatonVisualizer.addFinalState(String.valueOf(s));
        }
        automatonVisualizer.endGraph();
    }
}
