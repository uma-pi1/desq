package mining.statistics.old;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.function.Supplier;
import java.util.stream.Collector;

import utils.Dictionary;

public class GlobalInformationGainStatistic {
	
	private double[] gainList;
	
	
	public GlobalInformationGainStatistic(String dbFile) throws Exception {
		int[] flist = Dictionary.getInstance().getFlist();
		calculateInformationGainIndex(flist, retrieveNumberOfEvents(dbFile));
	}
	
	public double getInformationGain(int item) {		
		return gainList[item];
	}
	
	private void calculateInformationGainIndex(int[] flist, int events) {
		gainList = new double[flist.length];
		
		for (int i = 0; i < flist.length; i++) {
			gainList[i] = -1 * (Math.log(((double)flist[i]) / ((double) events)) / Math.log(flist.length));
		}	
	}
	
	private int retrieveNumberOfEvents(String dbFile) throws Exception {
		FileInputStream fstream;
		fstream = new FileInputStream(dbFile);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		int events = 0;
		
		String[] sequence;
//		Pattern pattern = Pattern.compile("\\s* \\s*");
		
		while ((strLine = br.readLine()) != null) {
			if (!strLine.isEmpty()) {
//				sequence = pattern.split(strLine);
				sequence = strLine.split(" ");
				events = events + sequence.length;
			}
		}
		br.close();
		System.out.println(events);
		return events;
	}
}
