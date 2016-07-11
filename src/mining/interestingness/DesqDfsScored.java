package mining.interestingness;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import mining.statistics.collectors.DesqGlobalDataCollector;
import mining.statistics.data.DesqSequenceData;
import mining.statistics.data.DesqTransactionData;
import utils.Dictionary;
import writer.SequentialWriter;
import fst.XFst;

/**
 * DesqDfs.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public abstract class DesqDfsScored {

	protected Dictionary dictionary = Dictionary.getInstance();
	
	protected ArrayList<int[]> inputSequences = new ArrayList<int[]>();
	
	protected static int sId;
	
	protected XFst xfst;
	
	protected double sigma;
	
	protected SequentialWriter writer = SequentialWriter.getInstance();
	
	protected int[] flist = Dictionary.getInstance().getFlist();
	
	protected int numPatterns = 0;
	
	protected boolean writeOutput = true;
	
	protected HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?,?>, ?>> globalDataCollectors;
	
	private DesqTransactionData transactionData;
	
	// Methods
	
	public DesqDfsScored() {}
	
	public DesqDfsScored(double sigma, XFst xfst, HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?,?>, ?>> globalDataCollectors, boolean writeOutput) {
		this.sigma = sigma;
		this.xfst = xfst;
		this.writeOutput = writeOutput;
		this.transactionData = new DesqTransactionData();
	}
	
	public void clear() {
		inputSequences.clear();
	}
	
	public void scan(String file) throws Exception {
		FileInputStream fstream = new FileInputStream(file);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line;
		int transactionId = 0;
		
		while((line = br.readLine()) != null) {
			if(!line.isEmpty()) {
				String[] rawSequence = line.split(" ");
				int[] inputSequence = new int[rawSequence.length];
				int i = 0;
				for (String s : rawSequence) {
					try {
						inputSequence[i++] = Integer.parseInt(s);
					} catch (NumberFormatException e) {
				        System.out.println(rawSequence + " " + inputSequence.length); 
				    }
				}
				addInputSequence(inputSequence);
				
				transactionData.setTransaction(inputSequence);
				transactionData.setTransactionId(transactionId++);
				
				for (Entry<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>> entry: globalDataCollectors.entrySet()) {
					@SuppressWarnings("unchecked")
					DesqGlobalDataCollector<DesqGlobalDataCollector<?,?>, ?> coll = (DesqGlobalDataCollector<DesqGlobalDataCollector<?, ?>, ?>) entry.getValue();
					coll.accumulator().accept(coll, transactionData);
				}
				
			}
		}
		br.close();
	}
	
	protected abstract void addInputSequence(int[] inputSequence);

	public abstract void mine() throws IOException, InterruptedException;
	
	public int noPatterns(){
		return numPatterns;
	}
	
}
