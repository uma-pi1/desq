package de.uni_mannheim.desq.fst.distributed;

import com.google.common.base.Objects;
import de.uni_mannheim.desq.fst.Dfa;

import java.util.concurrent.Callable;

/** Custom key object for {@link DfaCache}. */
public class DfaCacheKey {

    private String patternExpression;
    private Callable<Dfa> callableForDfaConstruction;

    public DfaCacheKey(String patternExpression, Callable<Dfa> callableForDfaConstruction) {
        this.patternExpression = patternExpression;
        this.callableForDfaConstruction = callableForDfaConstruction;
    }

    public String getPatternExpression() {
        return this.patternExpression;
    }

    public Callable<Dfa> getCallableForDfaConstruction() {
        return this.callableForDfaConstruction;
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
        DfaCacheKey that = (DfaCacheKey) o;
        return Objects.equal(patternExpression, that.patternExpression);
    }

}
