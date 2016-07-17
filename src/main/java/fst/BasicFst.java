package fst;

import fst.OutputLabel.Type;

/**
 * BasicFst.java: Creates a two state FST for an item expression
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public final class BasicFst {

	private BasicFst() {
	};

	public static Fst translateWildCard(boolean generalize, boolean capture) {
		return translateItemExpression(0, false, generalize, capture);
	}

	public static Fst translateItemExpression(int iLabel, boolean force, boolean generalize, boolean capture) {
		Fst fst = new Fst();

		OutputLabel oLabel;

		if (capture) {
			if (force && generalize) { // case: A=^
				oLabel = new OutputLabel(Type.CONSTANT, iLabel);
			} else if (force & !generalize) { // case A=
				oLabel = new OutputLabel(Type.CONSTANT, iLabel);
				iLabel = -iLabel;
			} else if (!force && generalize) { // case A^
				oLabel = new OutputLabel(Type.SELFGENERALIZE, iLabel);
			} else { // case A
				oLabel = new OutputLabel(Type.SELF);
			}
		} else {
			assert (generalize == false);
			oLabel = new OutputLabel(Type.EPSILON);
			if (force) {
				iLabel = -iLabel;
			}
		}

		Transition t = new Transition(iLabel, oLabel, new State(true));
		fst.initialState.addTransition(t);
		return fst;
	}

}
