package fst;


/**
 * OutputLabel.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class OutputLabel {
	
	public enum Type {
		EPSILON, CONSTANT, SELF, SELFGENERALIZE;
	};
	
	public Type type;
	
	public int item;

	OutputLabel(Type type) {
		this.type = type;
		this.item = -1;
	}

	OutputLabel(Type type, int item) {
		this.type = type;
		this.item = item;
	}
	
	OutputLabel(OutputLabel oLabel) {
		this.type = oLabel.type;
		this.item = oLabel.item;
	}

	public OutputLabel() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof OutputLabel))
			return false;
		if (obj == this)
			return true;
		OutputLabel oLabel = (OutputLabel) obj;
		if (oLabel.type == this.type && oLabel.item == this.item)
			return true;
		return false;
	}

	@Override
	public int hashCode() {
		return type.hashCode() + 31 * item;
	}
	
	public String toString(){
		StringBuilder s = new StringBuilder();
		switch(this.type) {
		case EPSILON:
			s.append("e");
			break;
		case CONSTANT:
			s.append(this.item);
			break;
		case SELF:
			s.append("$");
			break;
		case SELFGENERALIZE:
			s.append("$-" + this.item);
			break;
		default:
			break;
		}
		
		return s.toString();
	}

}