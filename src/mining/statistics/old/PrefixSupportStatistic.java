package mining.statistics.old;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class PrefixSupportStatistic implements Supplier<Integer>, BiConsumer<PrefixSupportStatistic, Integer> {
	int support = 0;
	
	PrefixSupportStatistic() {
		
	}
	
	PrefixSupportStatistic(int support) {
		this.support = support;
	}

	@Override
	public Integer get() {
		return 0;
	}

	@Override
	public void accept(PrefixSupportStatistic t, Integer u) {
		t.support = t.support + u;
	}
}
