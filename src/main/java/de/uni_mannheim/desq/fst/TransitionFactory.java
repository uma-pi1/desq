package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.BasicDictionary;

/**
 * Created by rgemulla on 12.12.2016.
 */
public class TransitionFactory {
    /* Creates a transition for item expression: . */
    public static final Transition uncapturedDot(final BasicDictionary dict, final State toState) {
        return new TransitionUncapturedDot(dict, toState);
    }

    /** Creates a transition for item expression: (.) */
    public static final Transition capturedDot(final BasicDictionary dict, final State toState) {
        return new TransitionCapturedDot(dict, toState);
    }

    /** Creates a transition for item expression: (.^) */
    public static final Transition capturedGeneralizedDot(final BasicDictionary dict, final State toState) {
        return new TransitionCapturedGeneralizedDot(dict, toState);
    }

    /** Creates a transition for item expression of form: A or A= */
    public static final Transition uncapturedItem(final BasicDictionary dict, final State toState,
                                                  final int itemFid, final String itemLabel,
                                                  final boolean matchDescendants) {
        return new TransitionUncapturedItem(dict, toState, itemFid, itemLabel, matchDescendants);
    }

    /** Creates a transition for item expression of form: (A) or (A=) */
    public static final Transition capturedItem(final BasicDictionary dict, final State toState,
                                                final int itemFid, final String itemLabel,
                                                final boolean matchDescendants) {
        return new TransitionCapturedItem(dict, toState, itemFid, itemLabel, matchDescendants);
    }

    /** Creates a transition for item expression of form: (A=^) */
    public static final Transition capturedConstant(final BasicDictionary dict, final State toState,
                                                    final int itemFid, final String itemLabel) {
        return new TransitionCapturedConstant(dict, toState, itemFid, itemLabel);
    }

    /** Creates a transition for item expression of form: (A^) */
    public static final Transition capturedGeneralizedItem(final BasicDictionary dict, final State toState,
                                                           final int itemFid, final String itemLabel) {
        return new TransitionCapturedGeneralizedItem(dict, toState, itemFid, itemLabel);
    }

}
