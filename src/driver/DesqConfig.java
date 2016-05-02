package driver;

public class DesqConfig {
	public static enum Method {
		DESQCOUNT, DESQDFS
	};
	
	// I/O paths
	private String inputSequencesPath;
	private String outputSequencesPath;
	private String hierarchyFilePath;
	private String encodedSequencesPath;
	
	// Parameters
	private int sigma;
	private String patternExpression;
	
	// Method
	private Method method;
	
	// Developer specific options
	private boolean isWriteOutput = true;
	private boolean isUseFlist = true;
	
	public DesqConfig() {
		// Defaults
		this.inputSequencesPath = null;
		this.outputSequencesPath = null;
		this.hierarchyFilePath = null;
		this.encodedSequencesPath = null;
		this.sigma = 100;
		this.patternExpression = null;
		this.method = Method.DESQDFS;
	}
	
	public DesqConfig(DesqConfig conf) {
		this.inputSequencesPath = conf.inputSequencesPath;
		this.outputSequencesPath = conf.outputSequencesPath;
		this.hierarchyFilePath = conf.hierarchyFilePath;
		this.encodedSequencesPath = conf.encodedSequencesPath;
		this.sigma = conf.sigma;
		this.patternExpression = conf.patternExpression;
		this.method = conf.method;
	}

	/**
	 * @return the inputSequencesPath
	 */
	public String getInputSequencesPath() {
		return inputSequencesPath;
	}

	/**
	 * @param inputSequencesPath the inputSequencesPath to set
	 */
	public void setInputSequencesPath(String inputSequencesPath) {
		this.inputSequencesPath = inputSequencesPath;
	}

	/**
	 * @return the outputSequencesPath
	 */
	public String getOutputSequencesPath() {
		return outputSequencesPath;
	}

	/**
	 * @param outputSequencesPath the outputSequencesPath to set
	 */
	public void setOutputSequencesPath(String outputSequencesPath) {
		this.outputSequencesPath = outputSequencesPath;
	}

	/**
	 * @return the hierarchyFilePath
	 */
	public String getHierarchyFilePath() {
		return hierarchyFilePath;
	}

	/**
	 * @param hierarchyFilePath the hierarchyFilePath to set
	 */
	public void setHierarchyFilePath(String hierarchyFilePath) {
		this.hierarchyFilePath = hierarchyFilePath;
	}

	/**
	 * @return the encodedSequencesPath
	 */
	public String getEncodedSequencesPath() {
		return encodedSequencesPath;
	}

	/**
	 * @param encodedSequencesPath the encodedSequencesPath to set
	 */
	public void setEncodedSequencesPath(String encodedSequencesPath) {
		this.encodedSequencesPath = encodedSequencesPath;
	}

	/**
	 * @return the sigma
	 */
	public int getSigma() {
		return sigma;
	}

	/**
	 * @param sigma the sigma to set
	 */
	public void setSigma(int sigma) {
		this.sigma = sigma;
	}

	/**
	 * @return the patternExpression
	 */
	public String getPatternExpression() {
		return patternExpression;
	}

	/**
	 * @param patternExpression the patternExpression to set
	 */
	public void setPatternExpression(String patternExpression) {
		this.patternExpression = patternExpression;
	}

	/**
	 * @return the method
	 */
	public Method getMethod() {
		return method;
	}

	/**
	 * @param method the method to set
	 */
	public void setMethod(Method method) {
		this.method = method;
	}

	/**
	 * @return the isWriteOutput
	 */
	public boolean isWriteOutput() {
		return isWriteOutput;
	}

	/**
	 * @param isWriteOutput the isWriteOutput to set
	 */
	public void setWriteOutput(boolean isWriteOutput) {
		this.isWriteOutput = isWriteOutput;
	}

	/**
	 * @return the isUseFlist
	 */
	public boolean isUseFlist() {
		return isUseFlist;
	}

	/**
	 * @param isUseFlist the isUseFlist to set
	 */
	public void setUseFlist(boolean isUseFlist) {
		this.isUseFlist = isUseFlist;
	}
	
}
