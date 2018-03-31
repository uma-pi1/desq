package de.uni_mannheim.desq.mining.distributed;

import de.uni_mannheim.desq.fst.Fst;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

/**
 * Created by alex on 09/02/2017.
 */
public class NfaList {
    private Int2IntOpenHashMap nfasByPivot = new Int2IntOpenHashMap();
    private ObjectArrayList<OutputNfa> nfas = new ObjectArrayList<>();

    public void clear() {
        nfasByPivot.clear();
    }

    public OutputNfa getNFAForPivot(int pivot, Fst fst, boolean useHybrid) {
        if(nfasByPivot.containsKey(pivot)) {
            // we have the NFA already, return it
            return nfas.get(nfasByPivot.get(pivot));
        } else {
            // we need to make a new NFA. either by reusing an old one or creating a new one
            if (nfas.size() > nfasByPivot.size()) {
                // we can reuse an old object
                OutputNfa nfa = nfas.get(nfasByPivot.size());
                nfa.clearAndPrepForPivot(pivot);
                nfasByPivot.put(pivot, nfasByPivot.size());
                return nfa;
            } else {
                // we have to create a new object
                OutputNfa nfa = new OutputNfa(pivot, fst, useHybrid);
                nfas.add(nfa);
                nfasByPivot.put(pivot, nfas.size() - 1);
                return nfa;
            }
        }
    }

    public ObjectList<OutputNfa> getNFAs() {
        return nfas.subList(0, nfasByPivot.size());
    }

    public int size() {
        return nfasByPivot.size();
    }
}
