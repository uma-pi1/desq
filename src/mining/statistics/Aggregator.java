package mining.statistics;

public interface Aggregator<Input, Partial, Final> {
	
	public Partial initialize(Input t);
	public Partial merge(Partial p1, Partial p2);
	public Final terminate(Partial p);
	
}
