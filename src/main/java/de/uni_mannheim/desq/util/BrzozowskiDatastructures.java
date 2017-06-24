package de.uni_mannheim.desq.util;

import de.uni_mannheim.desq.mining.OutputLabel;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.Object2IntAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

/**
 * Data structures for determinizing an OutputNFA once.
 * Can be reused for mutliple OutputNFAs, using clear()
 */
public class BrzozowskiDatastructures {

    ObjectArrayList<IntSet> includedStates = new ObjectArrayList<>();
    ObjectArrayList<Object2IntAVLTreeMap<OutputLabel>> outgoingEdges = new ObjectArrayList<>();
    Object2IntOpenHashMap<IntSet> statesByIncludedOldStates = new Object2IntOpenHashMap<>();


    public void clear() {
        includedStates.clear();
        outgoingEdges.clear();
        statesByIncludedOldStates.clear();
    }

    public int addNewState(IntSet inclStates) {
        int numState = numStates();
        includedStates.add(inclStates);
        outgoingEdges.add(new Object2IntAVLTreeMap<>());
        statesByIncludedOldStates.put(inclStates, numState);
        return numState;
    }

    public void addEdge(int from, OutputLabel label, int to) {
        getOutgoingEdges(from).put(label, to);
    }

    public int numStates() {
        return includedStates.size();
    }

    public IntSet getIncludedOriginalStates(int state) {
        return includedStates.get(state);
    }

    public Object2IntAVLTreeMap<OutputLabel> getOutgoingEdges(int state) {
        return outgoingEdges.get(state);
    }

    public int checkForExistingState(IntSet inclStates) {
       return statesByIncludedOldStates.getOrDefault(inclStates, -1);
    }

    public boolean isFinal(int state) {
        return getIncludedOriginalStates(state).contains(0);
    }

}
