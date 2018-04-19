package de.uni_mannheim.desq.fst.distributed;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import de.uni_mannheim.desq.fst.Fst;

/**
 * Cache helping to construct FSTs only once per executor JVM.
 *
 * We evict entries from the cache with reference-based eviction (entry in the cache is going to be garbage-collected
 * if there are no strong or soft references to it anymore).
 */
public class FstCache {

    private static LoadingCache<FstCacheKey, Fst> fstCache = CacheBuilder.newBuilder()
            .weakValues()
            .build(new CacheLoader<FstCacheKey, Fst>() {
                public Fst load(FstCacheKey key) throws Exception {
                    return key.getCallableForFstConstruction().call();
                }
            });

    /** Returns an FST that is either constructed and put into the cache or loaded from the cache. */
    public static Fst getFst(FstCacheKey key) {
        return fstCache.getUnchecked(key);
    }

}
