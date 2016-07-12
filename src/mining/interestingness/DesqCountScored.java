package mining.interestingness;

import fst.XFst;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenCustomHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenCustomHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import mining.scores.DesqCountScore;
import mining.scores.NotImplementedExcepetion;
import mining.scores.RankedScoreList;
import mining.statistics.collectors.DesqGlobalDataCollector;
import mining.statistics.collectors.DesqProjDbDataCollector;
import mining.statistics.collectors.DesqResultDataCollector;
import mining.statistics.data.DesqSequenceData;
import mining.statistics.data.DesqTransactionData;
import mining.statistics.data.ProjDbStatData;
import utils.Dictionary;
import utils.IntArrayStrategy;
import utils.PrimitiveUtils;
import writer.SequentialWriter;
import driver.DesqConfig.Match;


/**
 * DesqCount.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public abstract class DesqCountScored {

	protected Dictionary dictionary = Dictionary.getInstance();

	protected Object2DoubleOpenCustomHashMap<int[]> outputSequences = new Object2DoubleOpenCustomHashMap<int[]>(new IntArrayStrategy());
//	
	protected ArrayList<int[]> inputSequences = new ArrayList<int[]>();
	
//	protected ArrayList<int[]> outputSequences = new ArrayList<int[]>();

	protected double sigma;

	protected XFst xfst;

	protected boolean writeOutput = true;
	
	protected int[] sequence;

	protected int sid = -1;
	
	protected int numPatterns = 0;
	
	protected int[] flist = Dictionary.getInstance().getFlist();

	protected SequentialWriter writer = SequentialWriter.getInstance();
	
	protected long gpt = 0L;
	
	protected long gptUnique = 0L;
	
	protected long globalGpt = 0L;
	
	protected  long globalGptUnique = 0L;
	
	protected  long totalMatchedSequences = 0L;
	
	protected  long totalMatches = 0L;

	protected Match match;
	
	protected DesqCountScore score;
	
	protected HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?,?>, ?>> globalDataCollectors;
	protected HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> projDbCollectors;
	protected HashMap<String, DesqResultDataCollector<? extends DesqResultDataCollector<?, ?>, ?>> resultDataCollectors;
	
	protected int stepCounts = 0;
	
	private RankedScoreList rankedScoreList;
	private boolean executeResultDataCollection;
	
	private ProjDbStatData projDbStatData = new ProjDbStatData();
	private DesqSequenceData sequenceData = new DesqSequenceData();
	private DesqTransactionData transactionData = new DesqTransactionData();
	
	protected Object2ObjectOpenCustomHashMap<int[], HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> > minedSequenceSet = new Object2ObjectOpenCustomHashMap<int[],HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>>>(new IntArrayStrategy());
	
	// Methods
	
	public DesqCountScored(double sigma, XFst dfa, DesqCountScore score,  HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>> globalDataCollectors, RankedScoreList rankedScoreList, boolean writeOutput,  Match match) {
		this.sigma = sigma;
		this.xfst = dfa;
		this.writeOutput = writeOutput;
		this.match = match;
		this.score = score;
		
		this.projDbCollectors = score.getProjDbCollectors();
		this.globalDataCollectors = globalDataCollectors;
		try {
			this.resultDataCollectors = score.getResultDataCollectors();
		} catch (NotImplementedExcepetion e) {
			this.resultDataCollectors = null;
		}
		
		this.rankedScoreList = rankedScoreList;
	}
	
	
	public void scan(String file) throws Exception {
		FileInputStream fstream = new FileInputStream(file);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		IntArrayList buffer = new IntArrayList();
		int transactionId = 0;
		
		String line;
		while ((line = br.readLine()) != null) {
			if (!line.isEmpty()) {
				String[] str = line.split(" ");
				sequence = new int[str.length];
				for (int i = 0; i < str.length; ++i) {
					sequence[i] = Integer.parseInt(str[i]);
				}
				
				inputSequences.add(sequence);
				
				transactionData.setTransaction(sequence);
				transactionData.setTransactionId(transactionId++);
				
				for (Entry<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>> entry: globalDataCollectors.entrySet()) {
					@SuppressWarnings("unchecked")
					DesqGlobalDataCollector<DesqGlobalDataCollector<?,?>, ?> coll = (DesqGlobalDataCollector<DesqGlobalDataCollector<?, ?>, ?>) entry.getValue();
					coll.accumulator().accept(coll, transactionData);
				}
			}
		}
		br.close();
		
		for(int sId = 0; sId < inputSequences.size(); ++sId) {
			sequence = inputSequences.get(sId);
			
			sid = sid + 1;
			gpt = 0;
			gptUnique = 0;
			buffer.clear();
			//computeMatch(buffer, 0, dfa.getInitialState());
			computeMatch();
			globalGpt += gpt;
			globalGptUnique += gptUnique;
			
			if(gpt > 0)
				totalMatchedSequences++;
		}
		
		// evaluate sequences by projected database statistics
		try {
			for (Map.Entry<int[], HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>>> entry : minedSequenceSet.entrySet()) {
				HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> finalCollectors = entry.getValue();
				if (score.getScoreByProjDb(entry.getKey(), globalDataCollectors, finalCollectors) >= sigma) {
					addSequenceToOutput(entry.getKey(), score.getScoreByProjDb(entry.getKey(), globalDataCollectors, finalCollectors));				
				}
			}
		} catch(NotImplementedExcepetion e) {
			// do nothing...
		}
		
		if(resultDataCollectors != null) {
			// evaluate sequences by result statistics, remove sequences 
			for (Entry<int[], Double> entry : outputSequences.entrySet()) {
				if (score.getScoreByResultSet(entry.getKey(), globalDataCollectors, resultDataCollectors) >= sigma) {
					outputSequences.put(entry.getKey(), score.getScoreByResultSet(entry.getKey(), globalDataCollectors, resultDataCollectors));		
				} else {
					outputSequences.remove(entry.getKey());
				}
			}
		}
		
		// output sequences
		totalMatches = outputSequences.size();
		
		for (Entry<int[], Double> entry : outputSequences.entrySet()) {
			rankedScoreList.addNewOutputSequence(entry.getKey(), entry.getValue());
		}
		
		System.out.println(stepCounts);
		
	}
	
	protected abstract void computeMatch();

//	protected void countSequence(int[] sequence) {
//		gpt++;
//		Long supSid = outputSequences.get(sequence);
//		if (supSid == null) {
//			outputSequences.put(sequence, PrimitiveUtils.combine(1, sid));
//			gptUnique++;
//			return;
//		}
//		if (PrimitiveUtils.getRight(supSid) != sid) {
//			int newCount = PrimitiveUtils.getLeft(supSid) + 1;
//			outputSequences.put(sequence, PrimitiveUtils.combine(newCount, sid));
//			gptUnique++;
//		}
//	}
	
	protected void updateFinalSequenceStatistics(int[] sequence) {
		HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> finalStateProjDbAccumulators = minedSequenceSet.get(sequence);
		if (finalStateProjDbAccumulators == null) {
			finalStateProjDbAccumulators = new HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>>();
			for (Entry<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> entry: projDbCollectors.entrySet()) {
				@SuppressWarnings("unchecked")
				DesqProjDbDataCollector<DesqProjDbDataCollector<?,?>, ?> coll = (DesqProjDbDataCollector<DesqProjDbDataCollector<?, ?>, ?>) entry.getValue();
				finalStateProjDbAccumulators.put(entry.getKey(), coll.supplier().get());
			}

		} 
		
		projDbStatData.setPosition(-1);
		projDbStatData.setStateFST(-1);
		projDbStatData.setTransaction(this.sequence);
		projDbStatData.setTransactionId(this.sid);
		projDbStatData.setInFinalState(true);
		
		for (Entry<String, DesqProjDbDataCollector<?, ?>> entry : finalStateProjDbAccumulators.entrySet()) {
			// at compile time it is not decided which type the accept function 
			@SuppressWarnings("unchecked")
			DesqProjDbDataCollector<DesqProjDbDataCollector<?,?>, ?> finalProjDBCollector = (DesqProjDbDataCollector<DesqProjDbDataCollector<?, ?>, ?>) finalStateProjDbAccumulators.get(entry.getKey());
			finalProjDBCollector.accumulator().accept(entry.getValue(), projDbStatData);
		}
		minedSequenceSet.put(sequence, finalStateProjDbAccumulators);
	}
	
	protected void addSequenceToOutput(int[] outputSeq, double score) {
		outputSequences.put(outputSeq, score);
		numPatterns++;
		
		if(executeResultDataCollection ) {
			try {
				sequenceData.setSequence(outputSeq);
				
				for (Entry<String, DesqResultDataCollector<? extends DesqResultDataCollector<?, ?>, ?>> resultCollectorEntry: resultDataCollectors.entrySet()) {
					@SuppressWarnings("unchecked")
					DesqResultDataCollector<DesqResultDataCollector<?,?>, ?> resultCollector = (DesqResultDataCollector<DesqResultDataCollector<?, ?>, ?>) resultCollectorEntry.getValue();
					resultCollector.accumulator().accept(resultCollector, sequenceData);
				}
			} catch(NotImplementedExcepetion e) {
				executeResultDataCollection = false;
			}
		}
		
		
	}

	public long getGlobalGpt() {
		return globalGpt;
	}


	public long getTotalMatchedSequences() {
		return totalMatchedSequences;
	}


	public long getGlobalGptUnique() {
		return globalGptUnique;
	}


	public int noOutputPatterns() {
		return numPatterns;
	}


	public long getTotalMatches() {
		return totalMatches;
	}
	


}
