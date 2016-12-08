package de.uni_mannheim.desq.util;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rgemulla on 08.12.2016.
 */
public class IntListOptimizer {
    private Map<IntList, IntList> cache = new HashMap<>();
    private boolean allowReuse;

    public IntListOptimizer(boolean allowReuse) {
        this.allowReuse = allowReuse;

    }

    public IntList optimize(IntList list) {
        // see if we have this set already
        IntList optimizedList = cache.get(list);
        if (optimizedList == null) {
            // if not, create and cache it
            optimizedList = optimize(list, allowReuse);
            cache.put(optimizedList, optimizedList);
        }
        return optimizedList;
    }

    /** Returns a read-optimized list with the same elements as the given list. The returned list is optimized for
     * memory consumption and read speed and must not be modified.
     *
     * @param list the list to optimized
     * @param allowReuse whether we can return <code>list</code> if already optimal
     */
    public static IntList optimize(IntList list, boolean allowReuse) {
        // handle empty lists
        if (list.isEmpty()) {
            return IntLists.EMPTY_LIST;
        }

        //  handle singleton lists
        if (list.size() == 1) {
            if (allowReuse && list instanceof IntLists.Singleton) {
                return list;
            } else {
                return IntLists.singleton(list.get(0));
            }
        }

        // otherwise use arrays
        if (allowReuse && list instanceof IntArrayList) {
            ((IntArrayList)list).trim();
            return list;
        } else {
            return new IntArrayList(list);
        }
    }
}
