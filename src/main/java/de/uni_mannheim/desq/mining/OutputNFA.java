package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.graphviz.FstVisualizer;
import it.unimi.dsi.fastutil.ints.IntBidirectionalIterator;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.apache.commons.io.FilenameUtils;

import java.util.BitSet;

/**
 * A NFA that produces the output sequences of one input sequence.
 *
 * Per input sequence, we send one NFA to each partition, if the input sequence produces at least one
 * output sequence at that partition. Before sending the NFA, we trim it so we don't send paths that are
 * not relevant for that partition. We do the trimming while serializing.
 */
public class OutputNFA {
    PathState root;
    int numPathStates = 0;
    int numPaths = 0;
    int numSerializedStates = 0;
    protected int numSerializedFinalStates = 0;

    int pivot;

    // special integers for serialization
    final static public int FINAL = 0;
    final static public int END_FINAL = Integer.MIN_VALUE;
    final static public int END = Integer.MAX_VALUE;

    /** List of states in this NFA */
    public ObjectList<PathState> pathStates = new ObjectArrayList<>();

    /** List of leaf states in this NFA */
    public IntLinkedOpenHashSet leafs = new IntLinkedOpenHashSet();

    public Fst fst;

    public OutputNFA(int pivot, Fst fst) {
        root = new PathState(this, null, 0);
        this.pivot = pivot;
        this.fst = fst;
    }

    /** Get the nth PathState of this NFA */
    public PathState getPathStateByNumber(int n) {
        return pathStates.get(n);
    }

    public PathState getMergeTarget(int n) {
        PathState state = getPathStateByNumber(n);
        if(state.id != state.mergedInto)
            return getPathStateByNumber(state.mergedInto);
        else
            return state;
    }

    /**
     * Add a path to this NFA.
     * Trims the outputItems sets of the OutputLabls in the path for the pivot of this NFA.
     * Returns the largest fid smaller than the pivot
     *
     * @param path
     */
    protected int addOutLabelPathAndReturnNextPivot(OutputLabel[] path) {
        numPaths++;
        PathState currentState;
        int nextLargest = -1, nextLargestInThisSet;
        boolean seenPivotAsSingle = false;
        OutputLabel ol;

        // Run through the transitions of this path and add them to this NFA
        currentState = this.root;
        for(int i=0; i<path.length; i++) {
            ol = path[i];

            // drop all output items larger than the pivot
            while(ol.outputItems.getInt(ol.outputItems.size()-1) > pivot) {
                ol.outputItems.removeInt(ol.outputItems.size()-1);
            }

            // find next largest item
            if(!seenPivotAsSingle) { // as soon as we know there won't be another round we don't need to do this anymore
                nextLargestInThisSet = ol.outputItems.getInt(ol.outputItems.size()-1);
                // if the largest item is the pivot, we get the next-largest (if there is one)
                if (nextLargestInThisSet == pivot) {
                    if (ol.outputItems.size() > 1) {
                        nextLargestInThisSet = ol.outputItems.getInt(ol.outputItems.size()-2);
                    } else {
                        // if this set has only the pivot item, it will be empty next run. so we note
                        //    that we need to stop after this run
                        seenPivotAsSingle = true;
                    }
                }
                nextLargest = Math.max(nextLargest, nextLargestInThisSet);
            }

            currentState = currentState.followTransition(ol, this);
        }

        // we ran through the path, so the current state is a final state.
        currentState.setFinal();

        if(seenPivotAsSingle)
            return -1; // meaning, we have an empty set for any pivot smaller the current pivot, so we stop
        else
            return nextLargest;
    }

    public WeightedSequence mergeAndSerialize() {
        // merge
//        swMerge.start();
//        counterTrimCalls++;
        followGroup(leafs, true);
//        swMerge.stop();

        // serialize
//        swSerialize.start();
        WeightedSequence send = new WeightedSequence(0);
        numSerializedStates = 0;
        root.serializeDfs(send);

        // if we have only one final state and it's the last one, we don't need to send the marker
        if(numSerializedFinalStates == 1 && send.getInt(send.size()-1) == OutputNFA.FINAL) {
            send.size(send.size()-1);
        }
//        swSerialize.stop();
//        maxNumStates = Math.max(numSerializedStates, maxNumStates);

        return send;
    }

    /**
     * Process a group of states. That means, we try to merge subsets of the passed group of states. Each of those groups,
     * we process recursively.
     * @param stateIds
     */
    private void followGroup(IntLinkedOpenHashSet stateIds, boolean definitelyMergeable) {
//        if(stateIds.size() > maxFollowGroupSetSize) maxFollowGroupSetSize = stateIds.size();
//        counterFollowGroupCalls++;
        IntBidirectionalIterator it = stateIds.iterator();
        int currentTargetStateId;
        int sId;
        PathState target, merge;

        // we use this bitset to mark states that we have already merged in this iteration.
        // we use that to make sure we don't process them more than once
        BitSet alreadyMerged = new BitSet();

        // as we go over the passed set of states, we collect predecessor groups, which we will process afterwards
        IntLinkedOpenHashSet predecessors = new IntLinkedOpenHashSet();

        int i = 0, j;
        while(it.hasNext()) {
            // Retrieve the next state of the group. We will use this one as a merge target for all the following states
            // in the list
            currentTargetStateId = it.nextInt();
            i++;
            if(alreadyMerged.get(i)) // we don't process this state if we have merged it into another state already
                continue;
            target = getMergeTarget(currentTargetStateId);

            // begin a new set of predecessors
            predecessors.clear();
            j = i;

            // Now compare this state to all the following states. If we find merge-compatible states, we merge them into this one
            while(it.hasNext()) {
                sId = it.nextInt();
                j++;

                if(alreadyMerged.get(j))  // don't process if we already merged this state into another one
                    continue;

                merge = getMergeTarget(sId); // we retrieve the mapped number here. In case the state has already been merged somewhere else, we don't want to modify a merged state.

                if(merge.id == target.id) { // if the two states we are looking at are the same ones, we don't need to make any changes.
                    alreadyMerged.set(j);
                } else if(definitelyMergeable || merge.isMergeableInto(target)) { // check whether the two states are mergeable

                    // we write down that we merged this state into the target.
                    merge.mergedInto = target.id;

                    // also rewrite the original entry if the state we just merged was merged before
                    if(merge.id != sId)
                        getPathStateByNumber(sId).mergedInto = target.id;

                    // mark this state as merged, so we don't process it multiple times
                    alreadyMerged.set(j);

                    // if we have a group of predecessors (more than one), we process them as group
                    if(target.predecessor != null && merge.predecessor != null) {
                        if (target.predecessor.id != merge.predecessor.id) {
                            predecessors.add(merge.predecessor.id);
                        }
                    }
                }
            }

            // rewind the iterator
            it.back(j-i);

            // if we have multiple predecessors, process them as group (later on)
            if(predecessors.size()>0) {
                predecessors.add(target.predecessor.id);
                followGroup(predecessors, false);
            }
        }
    }

    /**
     * Export the PathStates of this NFA with their transitions to PDF
     * @param file
     */
    public void exportGraphViz(String file) {
        FstVisualizer fstVisualizer = new FstVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
        fstVisualizer.beginGraph();
        for(PathState s : pathStates) {
            if (s.mergedInto == s.id) {
                for (Object2ObjectMap.Entry<OutputLabel, PathState> trEntry : s.outTransitions.object2ObjectEntrySet()) {
                    OutputLabel ol = trEntry.getKey();
                    String label;
                    label = ol.outputItems.toString();
                    fstVisualizer.add(String.valueOf(s.id), label, String.valueOf(trEntry.getValue().mergedInto));
                }
                if (s.isFinal)
                    fstVisualizer.addFinalState(String.valueOf(s.id));
            }
        }
        fstVisualizer.endGraph();
    }
}

