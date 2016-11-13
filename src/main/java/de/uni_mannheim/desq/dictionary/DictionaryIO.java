package de.uni_mannheim.desq.dictionary;

import java.io.*;

/* I/O method for Dictionary */
@Deprecated // use Avro now
public final class DictionaryIO {
	/** Adds an item to the specified dictionary by deconding a line from del file format. */
	public static void addItemFromDelLine(Dictionary dict, String line, boolean withStatistics) {
		// create the item
		String[] columns = line.split("\t");
		int fid;
		String[] parents;
		if (withStatistics) {
			String sid = columns[0];
			fid = Integer.parseInt(columns[3]);
			long cfreq = Long.parseLong(columns[1]);
			long dfreq = Long.parseLong(columns[2]);
			dict.addItem(fid, fid, sid, dfreq, cfreq);
			parents = columns[4].split(",");
		} else {
			String sid = columns[0];
			fid = Integer.parseInt(columns[1]);
			dict.addItem(fid, fid, sid);
			parents = columns[2].split(",");
		}
		
		// add parents
		for (String parentString : parents) {
			int parentFid = Integer.parseInt(parentString);
			if (parentFid == 0) continue; // indicates no parent
			dict.addParent(fid, parentFid);
		}
	}
	
	/** Load dictionary from del file format.
	 * 
	 * with statistics:
	 * sid <TAB> cFreq <TAB> dFreq <TAB> gid (equals fid) <TAB> comma-separated parent ids
	 * 
	 * without statistics:
	 * sid <TAB> gid <TAB> comma-separated parent ids
	 * 
	 */
	public static Dictionary loadFromDel(InputStream in, boolean withStatistics) throws IOException {
		Dictionary dict = new Dictionary();
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line;
		while ((line = br.readLine()) != null) {
			addItemFromDelLine(dict, line, withStatistics);
		}
		br.close();
		return dict;
	}
	
	/** Conversion from old del format to new formats */
	public static void main(String[] args) throws IOException {
		String base = "data-local/nyt-1991-dict";
		Dictionary dict = loadFromDel(new FileInputStream(base+".del"), true);
		dict.write(base+".avro.gz");
		dict.write(base+".json");
	}
}
