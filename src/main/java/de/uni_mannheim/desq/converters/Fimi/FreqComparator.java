package de.uni_mannheim.desq.converters.Fimi;

import java.util.Comparator;
import java.util.TreeMap;

/**
 * Created by ryan on 29.01.17.
 */
public class FreqComparator implements Comparator<String> {
    TreeMap<String, Integer> hmap;
    public FreqComparator(TreeMap<String, Integer> h){
        hmap=h;
    }

    @Override
    public int compare(String o1, String o2) {
        int priority1 = hmap.get(o1);
        int priority2 = hmap.get(o2);
        return (priority1-priority2);
    }
}
