package writer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;



public class LogWriter {
	
	private static LogWriter instance = null;

	private String outputPath;

	// -- Methods

	protected LogWriter() {

	}

	public static LogWriter getInstance() {
		if (instance == null) {
			instance = new LogWriter();
		}
		return instance;
	}
	
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
		File outFile = new File(outputPath);
		File parentFile = outFile.getParentFile();
		if(!parentFile.exists() && !parentFile.mkdirs()){
		    throw new IllegalStateException("Couldn't create dir: " + parentFile);
		}
	}


	public void write(String s) throws IOException, InterruptedException {
		BufferedWriter br = new BufferedWriter(new FileWriter(outputPath, true));
		br.write(s);
		br.write("\n");
		br.close();
	}

}
