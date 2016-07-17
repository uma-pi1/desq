package fst;


/**
 * Transition.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class Transition {

	int iLabel;
	OutputLabel oLabel;
	
	State to;
	
	Transition() {}
	
	Transition(int iLabel, OutputLabel oLabel, State to){
		this.iLabel = iLabel;
		this.oLabel = oLabel;
		this.to = to;
	}
	
	public int getInputLabel() {
		return iLabel;
	}


	public void setInputLabel(int iLabel) {
		this.iLabel = iLabel;
	}


	public State getToState() {
		return to;
	}


	public void setToState(State to) {
		this.to = to;
	}
	
}
