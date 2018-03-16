package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.ItemState;
import de.uni_mannheim.desq.fst.Transition;
import de.uni_mannheim.desq.fst.graphviz.AutomatonVisualizer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.apache.commons.io.FilenameUtils;

import java.util.BitSet;
import java.util.Iterator;

/**
 * A class to decode serialized NFAs from by-path to by-state representation.
 * Reuses internal data structures between multiple calls to {@link #convertPathToStateSerialization(Sequence, int)}
 */
class NFADecoder {
    ObjectList<IntArrayList> outgoing = new ObjectArrayList<>();
    int numReadStates, item, inputItem, outputItem, currentState = 0, toState, itExId, sPos, outgoingIntegers, len;
    BitSet finalStates = new BitSet();
    IntList currentOutgoing;
    IntList writtenAtPos = new IntArrayList();
    Fst fst;
    int minItemExValue;
    Transition.ItemStateIteratorCache itCache;


    public NFADecoder(Fst fst, Transition.ItemStateIteratorCache itCache) {
        this.fst = fst;
        minItemExValue = -(fst.numberDistinctItemEx()+1);
        this.itCache = itCache;
    }

    /** Converts the NFA serialized in serializedNFA to a state-based representation **/
    public Sequence convertPathToStateSerialization(Sequence serializedNFA, int partitionPivot) {
        numReadStates = 0;
        finalStates.clear();
        outgoingIntegers = 0;
        currentState = 0;
        // prep outgoing array for root state
        prepOutgoing(0);

        for(int pos = 0; pos<serializedNFA.size(); pos++) {
            item = serializedNFA.getInt(pos);
            if(item == OutputNFA.FINAL) {
                // mark current state as final
                finalStates.set(currentState);
            } else {
                // this is a transition

                // we might have an explicit toState
                if(item < minItemExValue) {
                    currentState = -(item+1+fst.numberDistinctItemEx()+1);
                    pos++;
                    item = serializedNFA.getInt(pos);
                }

                // from here on, it's the outgoing transition. with either one positive output item, or a negative item ex id + input item
                if(item > 0) { // it's one single output item
                    sPos = pos+1;
                } else if(item == -1) { // it's two output items
                    sPos = pos+3;
                } else { // it's a transition id with input item
                    sPos = pos+2;
                }

                // look-ahead for a explicit toState
                if(sPos < serializedNFA.size() && serializedNFA.getInt(sPos) < minItemExValue) {
                    // there is an explicit toState
                    toState = -(serializedNFA.getInt(sPos)+1+fst.numberDistinctItemEx()+1);
                    sPos++;
                } else {
                    // there is no explicit toState. so we generate an implicit one
                    numReadStates++;
                    toState = numReadStates;
                    prepOutgoing(toState);
                }

                // store the toState and the output items
                currentOutgoing = outgoing.get(currentState);
                currentOutgoing.add(-toState);
                outgoingIntegers++;
                if(item > 0) {
                    // there is only one output item, we add it and are done
                    currentOutgoing.add(item);
                    outgoingIntegers++;
                } else if(item == -1) {
                    currentOutgoing.add(serializedNFA.getInt(pos+1));
                    currentOutgoing.add(serializedNFA.getInt(pos+2));
                    outgoingIntegers += 2;
                } else {
                    // there are multiple output items. we produce all output items and add ones relevant for this partition
                    inputItem = serializedNFA.getInt(pos+1);
                    itExId = -(item+1);
                    Iterator<ItemState> outIt = fst.getPrototypeTransitionByItemExId(itExId)
                            .consume(inputItem, itCache);
                    while(outIt.hasNext()) {
                        outputItem = outIt.next().itemFid;
                        if(partitionPivot >= outputItem) {
                            currentOutgoing.add(outputItem);
                            outgoingIntegers++;
                        }
                    }
                }

                // advance to the new state and set the new reading position correctly
                currentState = toState;
                pos = sPos-1;
            }
        }

        // if there is no final marker in the sequence, we assume the last state of the sequence is final
        if(finalStates.cardinality() == 0)
            finalStates.set(currentState);

        // Create the new serialization. For that, we reuse the array backing the input NFA.
        // We keep track of where we serialized which state. In a second run over the serialization, we replace
        // the state numbers by their index in the array.
        serializedNFA.size(outgoingIntegers+numReadStates+1);
        int[] byState = serializedNFA.elements();
        writtenAtPos.size(numReadStates+1);

        for(int stateNo=0, pos=0; stateNo<=numReadStates; stateNo++) {
            // we write state stateNo at position pos
            writtenAtPos.set(stateNo,pos);

            // write outgoing data
            if(stateNo < outgoing.size()) { // if this state has outgoing transitions
                assert !outgoing.get(stateNo).contains(-stateNo);
                len = outgoing.get(stateNo).size();
                System.arraycopy(outgoing.get(stateNo).elements(), 0, byState, pos, len);
            } else {
                len = 0;
            }
            pos += len; // accounting for the outgoing data

            // write the end marker (final or non-final)
            if(finalStates.get(stateNo))
                byState[pos] = OutputNFA.END_FINAL;
            else
                byState[pos] = OutputNFA.END;

            pos++; // accounting for the state end marker
        }

        // replace state numbers by their index in the array
        for(int pos = 0; pos<serializedNFA.size(); pos++) {
            if(byState[pos] < 0 && byState[pos] != OutputNFA.END_FINAL) {
                byState[pos] =  -writtenAtPos.get(-byState[pos]);
            }
        }

        return serializedNFA;
    }

    /** Prepare the outgoing IntList for the state with number stateNo: make sure outgoing array is large enough
     * and item array exists. If it already exists, clear the previously used array. */
    private void prepOutgoing(int stateNo) {
        if(stateNo >= outgoing.size()) {
            outgoing.add(new IntArrayList());
        } else {
            outgoing.get(stateNo).clear();
        }
    }


    /** Exports the fst using graphviz (type based on extension, e.g., "gv" (source file), "pdf", ...) */
    public void exportGraphViz(String file) {
        AutomatonVisualizer automatonVisualizer = new AutomatonVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
        automatonVisualizer.beginGraph();
        IntList outItems = new IntArrayList();
        int toState = -1;
        for(int i=0; i<numReadStates+1; i++) {
            if(i < outgoing.size()) {
                for(int j=0; j<outgoing.get(i).size(); j++) {
                    int item = outgoing.get(i).getInt(j);
                    if(item < 0) {
                        // a new to state, so write the previous edge (if there was a previous one) and change to new toState
                        if(!outItems.isEmpty()) {
                            automatonVisualizer.add(String.valueOf(i), outItems.toString(), String.valueOf(toState));
                        }
                        toState = -item;
                        outItems.clear();
                    } else {
                        // note output item
                        outItems.add(item);
                    }
                }
                // write last edge of the last state
                if(!outItems.isEmpty()) {
                    automatonVisualizer.add(String.valueOf(i), outItems.toString(), String.valueOf(toState));
                    outItems.clear();
                }
            }
            // mark final
            if(finalStates.get(i))
                automatonVisualizer.addFinalState(String.valueOf(i));
        }
        automatonVisualizer.endGraph();
    }
}
