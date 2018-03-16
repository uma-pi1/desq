package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.graphviz.AutomatonVisualizer;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.*;
import org.apache.commons.io.FilenameUtils;

/**
 * Created by alex on 10/07/2017.
 */
public class ExtractedNFA {
    /** For each state in the new NFA, this stores the included states of the original NFA */
    ObjectArrayList<IntSet> includedStates = new ObjectArrayList<>();

    /** Stores the outgoing edges of each state */
    ObjectArrayList<Object2ObjectAVLTreeMap<OutputLabel,IntSet>> outgoingEdges = new ObjectArrayList<>();

    /** Parallel arrays to store incoming labels and the corresponding "from" state */
    ObjectArrayList<ObjectArrayList<OutputLabel>> incomingLabels = new ObjectArrayList<>();
    ObjectArrayList<IntArrayList> incomingStates = new ObjectArrayList<>();

    /** A map of all states by their included original states. We use this to check whether we already have a state
     * that includes a given set of original states */
    Object2IntOpenHashMap<IntSet> statesByIncludedOriginalStates = new Object2IntOpenHashMap<>();

    /** The set of final states in this NFA */
    IntSet isFinal = new IntOpenHashSet();

    /** A flag whether to construct backward edges (not necessary for the forward NFA) */
    boolean constructBackwardEdges;

    // --------------- serialization ---------------------------
    /** Serialization order: (internal state id) -> (serialized number) */
    IntArrayList serializedStateNumbers = new IntArrayList();

    /** The number of serialized states */
    int numSerializedStates;

    /** The number of serialized final states */
    int numSerializedFinalStates;

    /** Pointer to the FST (used to encode larger sets of output items using transitions of the FST) */
    Fst fst;

    /** The pivot item of this NFA (also used in serialization) */
    int pivot;

    public ExtractedNFA(boolean constructBackwardEdges) {
        this.constructBackwardEdges = constructBackwardEdges;
    }

    public void clear() {
        includedStates.clear();
        outgoingEdges.clear();
        statesByIncludedOriginalStates.clear();
        isFinal.clear();
        incomingLabels.clear();
        incomingStates.clear();
        serializedStateNumbers.clear();
    }

    /** Add a new state that includes original states <code>inclStates</code> (original states = states of the original NFA) */
    private int addNewState(IntSet inclStates) {
        int numState = numStates();
        includedStates.add(inclStates);
        outgoingEdges.add(new Object2ObjectAVLTreeMap<>()); // TODO: probably better not to use tree maps here
        if(constructBackwardEdges) {
            incomingLabels.add(new ObjectArrayList<>());
            incomingStates.add(new IntArrayList());
        }
        statesByIncludedOriginalStates.put(inclStates, numState);
        serializedStateNumbers.add(-1);

        return numState;
    }

    /** Add an edge from state <code>from</code> to state <code>to</code> with label <code>label</code> */
    public void addEdge(int from, OutputLabel label, int to, boolean targetFinal) {
        assert from != to : "Can't add self-loop edges: " + from + "->" + to + " with label " + label;

        if(targetFinal)
            isFinal.add(to);

        IntSet toStates = outgoingEdges.get(from).getOrDefault(label, null);
        if(toStates == null) {
            toStates = new IntOpenHashSet();
            outgoingEdges.get(from).put(label, toStates);
        }
        toStates.add(to);

        if(constructBackwardEdges) {
            incomingLabels.get(to).add(label);
            incomingStates.get(to).add(from);
        }
    }

    public int getOrCreateState(IntSet inclStates) {
        int state = statesByIncludedOriginalStates.getOrDefault(inclStates, -1);
        if (state == -1) {
            state = addNewState(inclStates);
        }
        return state;
    }

    public int numStates() {
        return includedStates.size();
    }

    private boolean isFinal(int s) {
        return isFinal.contains(s);
    }

    /** Returns the states of the original NFA that are included in state <code>state</code> */
    public IntSet getIncludedOriginalStates(int state) {
        return includedStates.get(state);
    }

    /** Reverses and determinizes this NFA into the given ExtractedNFA */
    public ExtractedNFA reverseAndDeterminize(ExtractedNFA forwardDFA) {
        forwardDFA.clear();

        int currentState = forwardDFA.addNewState(isFinal);
        Object2ObjectOpenHashMap<OutputLabel, IntSet> newOutgoingEdges = new Object2ObjectOpenHashMap<>();

        do {
            newOutgoingEdges.clear();

            // collect the incoming edges of all included states
            for(int includedState : forwardDFA.getIncludedOriginalStates(currentState)) {
                // for every state in forwardDFA.getIncludedOriginalStates
                // add edges to the collecting map newOutgoingEdges
                for (int i = 0; i < incomingLabels.get(includedState).size(); i++) {
                    OutputLabel label = incomingLabels.get(includedState).get(i);
                    int to = incomingStates.get(includedState).getInt(i);
                    IntSet toStates = newOutgoingEdges.getOrDefault(label, null);
                    if (toStates == null) {
                        toStates = new IntAVLTreeSet();
                        toStates.add(to);
                        newOutgoingEdges.put(label, toStates);
                    } else {
                        toStates.add(to);
                    }
                }
            }

                // for each outgoing edges, add an edge
            for(Object2ObjectMap.Entry<OutputLabel, IntSet> outgoingEdge : newOutgoingEdges.object2ObjectEntrySet()) {
                int toState = forwardDFA.getOrCreateState(outgoingEdge.getValue());
                forwardDFA.addEdge(currentState, outgoingEdge.getKey(), toState, outgoingEdge.getValue().contains(0));
            }

            currentState++;
        } while (currentState < forwardDFA.numStates());


        return forwardDFA;

    }

    /** Serializes this NFA */
    public WeightedSequence serialize(Fst fst, int pivot) {
        WeightedSequence send = new WeightedSequence();
        this.fst = fst;
        this.pivot = pivot;

        numSerializedStates = 0;
        numSerializedFinalStates = 0;

        // run serialization in DFS manner
        serializeStep(0, send); // 0 is always root

        // if we have only one final state and it's the last one, we don't need to send the marker
        if(numSerializedFinalStates == 1 && send.getInt(send.size()-1) == OutputNFA.FINAL) {
            send.size(send.size()-1);
        }

        return send;
    }

    /** One DFS step for serializing this NFA */
    private void serializeStep(int state, WeightedSequence send) {
        OutputLabel ol;
        IntSet toStates;
        int toState;

        // keep track of the number of serialized states
        numSerializedStates++; // we start state numbering at 1 for the serialization
        serializedStateNumbers.set(state, numSerializedStates);

        if(isFinal(state)) {
            send.add(OutputNFA.FINAL);
            numSerializedFinalStates++;
        }

        boolean firstTransition = true;
        for(Object2ObjectMap.Entry<OutputLabel,IntSet> entry : outgoingEdges.get(state).object2ObjectEntrySet()) {
            ol = entry.getKey();
            toStates = entry.getValue();
            // in serialization, we assume deterministic automaton, so there is only one to-state
            toState = toStates.iterator().nextInt();


            // if this is the first transition we process at this state, we don't need to write and structural information.
            // we just follow the first path
            if(firstTransition) {
                firstTransition = false;
            } else {
                send.add(-(serializedStateNumbers.getInt(state)+fst.numberDistinctItemEx()+1));
            }

            // serialize the output label
            if(ol.outputItems.size() > 1 && ol.outputItems.getInt(1) <= pivot) { // multiple output items, so we encode them using the transition
                // TODO: the current serialization format doesn't allow us to send two output items directly. We need to think about this.
                if(ol.outputItems.size() == 2 || ol.outputItems.getInt(2) > pivot) {
                    // we have only two output items, so we encode them directly
                    send.add(-1); // we use this as marker for two following output items
                    send.add(ol.outputItems.getInt(0));
                    send.add(ol.outputItems.getInt(1));
                } else {
                    send.add(-(fst.getItemExId(ol.tr)+1));
                    send.add(ol.inputItem); // TODO: we can generalize this for the pivot
                }
            } else { // there is only one (relevant) output item, and we know it's the first in the list
                send.add(ol.outputItems.getInt(0));
            }

            // serialize the to-state, either by processing it or by noting down it's number
            if(serializedStateNumbers.getInt(toState) == -1) {
                serializeStep(toState, send);
            } else {
                send.add(-(serializedStateNumbers.getInt(toState)+fst.numberDistinctItemEx()+1));
            }
        }
    }

    /** Export this NFA to PDF */
    public void exportPDF(String file) {
        AutomatonVisualizer automatonVisualizer = new AutomatonVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
        automatonVisualizer.beginGraph();
        for (int s = 0; s < numStates(); s++) {
            for (Object2ObjectMap.Entry<OutputLabel,IntSet> trEntry : outgoingEdges.get(s).object2ObjectEntrySet()) {
                OutputLabel ol = trEntry.getKey();
                String label;
                label = (ol == null ? " " : ol.outputItems.toString() + "(" + ol.inputItem + ")");
                for(int toState : trEntry.getValue()) {
                    if (!ol.isEmpty()) {
                        automatonVisualizer.add(String.valueOf(s), label, String.valueOf(toState));
                    }
                }
            }

            if (isFinal(s))
                automatonVisualizer.addFinalState(String.valueOf(s));
        }
        automatonVisualizer.endGraph();
    }
}
