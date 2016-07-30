package mining.statistics.collectors;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

import mining.statistics.data.DesqTransactionData;

public class GlobalTransactionsCounter implements DesqGlobalDataCollector<GlobalTransactionsCounter, Integer>, 
												Supplier<GlobalTransactionsCounter>,
												BiConsumer<GlobalTransactionsCounter, DesqTransactionData> {
	// Data of the accumulator, BiConsumer 
	int totalNoTransactions;
	int previousTransactionId;
	public static final String ID = "GLOBAL_TRANSACTIONS_COUNTER";
	
	// Constructor for the supplier
	public GlobalTransactionsCounter() {
		this.previousTransactionId = -1;
	}
	
	// Constructor for the collector
	public GlobalTransactionsCounter(int totalNoTransactions) {
		this.totalNoTransactions = totalNoTransactions;
	}
	
	// Collector Method
	@Override
	public Supplier<GlobalTransactionsCounter> supplier() {
	  return GlobalTransactionsCounter::new;
	}

	// Collector Method
	@Override
	public BiConsumer<GlobalTransactionsCounter, DesqTransactionData> accumulator() {
		return (acc, elem) -> acc.accept(acc, elem);
	}
	
	// Collector Method
	@Override
	public BinaryOperator<GlobalTransactionsCounter> combiner() {
		return (acc1,acc2) -> new GlobalTransactionsCounter(acc1.totalNoTransactions + acc2.totalNoTransactions);
	}

	// Collector Method
	@Override
	public Function<GlobalTransactionsCounter, Integer> finisher() {
		return (acc) -> new Integer(acc.totalNoTransactions);
	}

	// Collector Method
	@Override
	public Set<Characteristics> characteristics() {
		return Collections.emptySet();
	}
	
	
	// Supplier Method
	@Override
	public GlobalTransactionsCounter get() {
		return new GlobalTransactionsCounter(0);
	}

	
	// BiConsumer Method
	@Override
	public void accept(GlobalTransactionsCounter t, DesqTransactionData u) {
		if(u.getTransactionId() != this.previousTransactionId) {
			t.totalNoTransactions = t.totalNoTransactions + 1;
			t.previousTransactionId = u.getTransactionId();
		} else {
			// do nothing
		}
	}
}
