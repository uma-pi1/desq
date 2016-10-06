package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.io.SequenceReader;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public abstract class DesqMiner {
	// if null, patterns are mined but not collected
	protected final DesqMinerContext ctx;
	
	protected DesqMiner(DesqMinerContext ctx) {
		this.ctx = ctx;
	}
	
	/** Adds a new input sequence (composed of fids). The provided sequence must 
	 * not be buffered by this miner. */
	protected abstract void addInputSequence(IntList inputSequence);
	
	public void addInputSequences(SequenceReader in) throws IOException {
		IntList inputSequence = new IntArrayList();
		while (in.readAsFids(inputSequence)) {
			addInputSequence(inputSequence);
		}
	}

	/** Mines all added input sequences */
	public abstract void mine();

	public static String patternExpressionFor(int gamma, int lambda, boolean generalize) {
		String capturedItem = "(." + (generalize ? "^" : "") + ")";
		return capturedItem + "[.{0," + gamma + "}" + capturedItem + "]{0," + (lambda-1) + "}";
	}

	/** Creates a miner for the specified context. To determine which miner to create, the "minerClass" property
	 * needs to be set. */
	public static DesqMiner create(DesqMinerContext ctx) {
		String minerClass = ctx.conf.getString("desq.mining.miner.class", null);
		if (minerClass==null) {
			throw new IllegalArgumentException("desq.mining.miner.class property not set");
		}
		try {
			DesqMiner miner = (DesqMiner)Class.forName(minerClass)
					.getConstructor(DesqMinerContext.class).newInstance(ctx);
			return miner;
		} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | ClassNotFoundException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}
}
