package de.uni_mannheim.desq.fst.distributed;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import de.uni_mannheim.desq.fst.Dfa;

/**
 * Cache helping to construct DFAs only once per executor JVM.
 *
 * We evict entries from the cache with reference-based eviction (entry in the cache is going to be garbage-collected
 * if there are no strong or soft references to it anymore).
 */
public class DfaCache {

    private static LoadingCache<DfaCacheKey, Dfa> dfaCache = CacheBuilder.newBuilder()
            .weakValues()
            .build(new CacheLoader<DfaCacheKey, Dfa>() {
                public Dfa load(DfaCacheKey key) throws Exception {
                    return key.getCallableForDfaConstruction().call();
                }
            });

    /** Returns a DFA that is either constructed and put into the cache or loaded from the cache. */
    public static Dfa getDfa(DfaCacheKey key) {
        return dfaCache.getUnchecked(key);
    }

}
