package driver;

import java.io.IOException;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import tools.EncodeData;
import driver.DesqConfig.Method;

public class Driver {

	private static final Logger logger = Logger.getLogger(Driver.class.getSimpleName());

	
	private String[] args = null;
	
	private Options options = new Options();
	
	public DesqConfig conf;
	
	boolean encode = false;
	boolean mine = false;
	
	public Driver(String[] args) {
		conf = new DesqConfig();
		this.args = args;
		
		options.addOption("encode", false, "Encode input data");
		options.addOption("i", true, "Path/to/input/sequences/");
		options.addOption("h", true, "Path/to/hierarchy-file");
		options.addOption("e", true, "Path/to/encoded-input/");
		
		options.addOption("mine", false, "Mine sequences");
		options.addOption("o", true, "Path/to/output/sequences/");
		options.addOption("s", true, "Minimum support threshold");
		options.addOption("p", true, "Pattern expression");
		
		options.addOption("method", true, null);
		
		options.addOption("help", false, "Show help");
	}
	
	public void parseArgs() throws IOException {
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		
		try {
			cmd = parser.parse(options, args);
			if(cmd.hasOption("help")) {
				help();
			}
			
			if(cmd.hasOption("encode")) {
				encode = true;
				mine = false;
				if(cmd.hasOption("i")) {
					conf.setInputSequencesPath(cmd.getOptionValue("i"));
				} else {
					logger.log(Level.SEVERE, "Missing path to input sequences");
					help();
				}
				
				if(cmd.hasOption("h")) {
					conf.setHierarchyFilePath(cmd.getOptionValue("h"));
				} else {
					logger.log(Level.SEVERE, "Missing path to hierarchy file");
					help();
				}
				
				if(cmd.hasOption("e")) {
					conf.setEncodedSequencesPath(cmd.getOptionValue("e"));
				} else {
					logger.log(Level.SEVERE, "Missing path to encoded input");
					help();
				}
			} else if (cmd.hasOption("mine")) {
				encode = false;
				mine = true;
				if(cmd.hasOption("e")) {
					conf.setEncodedSequencesPath(cmd.getOptionValue("e"));
				} else {
					logger.log(Level.SEVERE, "Missing path to encoded input");
					help();
				}
				
				if(cmd.hasOption("o")) {
					conf.setOutputSequencesPath(cmd.getOptionValue("o"));
				} else {
					logger.log(Level.SEVERE, "Missing path to output sequences");
					help();
				}
				
				if(cmd.hasOption("p")) {
					conf.setPatternExpression(cmd.getOptionValue("p"));
				} else {
					logger.log(Level.SEVERE, "Missing pattern expression");
					help();
				}
				
				if(cmd.hasOption("s")) {
					conf.setSigma(Integer.parseInt(cmd.getOptionValue("s")));
				} else {
					logger.log(Level.INFO, "Missing minimum support, using defaul" + conf.getSigma());
				}
				
				if(cmd.hasOption("method")) {
					if(cmd.getOptionValue("method").equals("desq-count")) {
						conf.setMethod(Method.DESQCOUNT);
					} else if(cmd.getOptionValue("method").equals("desq-dfs")) {
						conf.setMethod(Method.DESQDFS);
					} else if(cmd.getOptionValue("method").equals("desq-dfs-scored")) {
						conf.setMethod(Method.DESQDFSSCORED);
					} else {
						logger.log(Level.SEVERE, "Incorrect method");
						help();
					}
				}
			} else {
				help();
			}
			
			
		} catch (ParseException e) {
			logger.log(Level.SEVERE, "Failed to parser command line arguments");
			help();
		}
	}
	
	private void help() {
		System.out.println("To encode input data, use");
		System.out.println("-encode -i Path/to/input/sequences -h Path/to/hierarchy-file -e Path/to/encoded-input");
		System.out.println("\n\nTo mine input data, use");
		System.out.println("-mine -e Path/to/encoded-input/ -p \"pattern expression\" -s support -o Path/to/output/sequences/");
		System.out.println("\n");
		System.exit(-1);
	}
	
	private void run() throws Exception {
		if(encode) {
			EncodeData.run(conf);
		} else if (mine) {
			if (conf.getMethod() == Method.DESQCOUNT) {
				DesqCountDriver.run(conf);
			} else if (conf.getMethod() == Method.DESQDFS) {
				DesqDfsDriver.run(conf);
			} else if (conf.getMethod() == Method.DESQDFSSCORED) {
				DesqDfsScoredDriver.run(conf);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s [%1$tc]%n");
		Driver driver = new Driver(args);
		driver.parseArgs();
		driver.run();
	}
}
