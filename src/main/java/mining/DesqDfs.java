package mining;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import fst.XFst;
import utils.Dictionary;
import writer.DelWriter;

/**
 * DesqDfs.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public abstract class DesqDfs {

	protected Dictionary dictionary = Dictionary.getInstance();
	
	protected ArrayList<int[]> inputSequences = new ArrayList<>();
	
	protected static int sId;
	
	protected XFst xfst;
	
	protected int sigma;
	
	protected DelWriter writer = DelWriter.getInstance();
	
	protected int[] flist = Dictionary.getInstance().getFlist();
	
	protected int numPatterns = 0;
	
	protected boolean writeOutput = true;
	
	// Methods
	
	public DesqDfs() {}
	
	public DesqDfs(int sigma, XFst xfst, boolean writeOutput) {
		this.sigma = sigma;
		this.xfst = xfst;
		this.writeOutput = writeOutput;
	}
	
	public void clear() {
		inputSequences.clear();
	}
	
	public void scan(String file) throws Exception {
		FileInputStream fstream = new FileInputStream(file);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line;
		
		while((line = br.readLine()) != null) {
			if(!line.isEmpty()) {
				String[] rawSequence = line.split("\\s* \\s*");
				int[] inputSequence = new int[rawSequence.length];
				int i = 0;
				for (String s : rawSequence) {
					inputSequence[i++] = Integer.parseInt(s);
				}
				addInputSequence(inputSequence);
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
