package de.uni_mannheim.desq.fst.distributed;

import com.google.common.base.Objects;
import de.uni_mannheim.desq.fst.Fst;

import java.util.concurrent.Callable;

/** Custom key object for {@link FstCache}. */
public class FstCacheKey {

    private String patternExpression;
    private Callable<Fst> callableForFstConstruction;

    public FstCacheKey(String patternExpression, Callable<Fst> callableForFstConstruction) {
        this.patternExpression = patternExpression;
        this.callableForFstConstruction = callableForFstConstruction;
    }

    public String getPatternExpression() {
        return this.patternExpression;
    }

    public Callable<Fst> getCallableForFstConstruction() {
        return this.callableForFstConstruction;
    }

    /** Only hashes the pattern expression. */
    @Override
    public int hashCode() {
        return Objects.hashCode(patternExpression);
    }

    /** Only compares the pattern expression. */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FstCacheKey that = (FstCacheKey) o;
        return Objects.equal(patternExpression, that.patternExpression);
    }

}
