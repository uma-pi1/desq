package de.uni_mannheim.desq.patex;

import de.uni_mannheim.desq.dictionary.BasicDictionary;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.mining.DesqMinerContext;

/**
 * Created by rgemulla on 10.01.2017.
 */
public class PatExUtils {
    public static Fst toFst(BasicDictionary dict, String patternExpression) {
        return toFst(dict, patternExpression, false);
    }

    public static Fst toFst(BasicDictionary dict, String patternExpression, boolean itemsetPatEx) {
        PatExToFst p = new PatExToFst(patternExpression, dict);
        Fst fst = p.translate(itemsetPatEx);
        fst.minimize(); //TODO: move to translate
        fst.annotate();
        fst.exportGraphViz("minimized.pdf");
        return fst;
    }

    public static String toFidPatEx(BasicDictionary dict, String patternExpression) {
        PatExToPatEx p = new PatExToPatEx(dict, patternExpression, PatExToPatEx.Type.FID);
        return p.translate();
    }

    public static String toGidPatEx(BasicDictionary dict, String patternExpression) {
        PatExToPatEx p = new PatExToPatEx(dict, patternExpression, PatExToPatEx.Type.GID);
        return p.translate();
    }

    public static String toSidPatEx(BasicDictionary dict, String patternExpression) {
        PatExToPatEx p = new PatExToPatEx(dict, patternExpression, PatExToPatEx.Type.SID);
        return p.translate();
    }

    static int asFid(BasicDictionary dict, PatExParser.ItemContext ctx) {
        // parse item
        String label = ctx.getText();
        String sid = null;
        int gid = -1;
        int fid = -1;
        if (ctx.INT() != null) {
            sid = label;
        } else if (ctx.GID() != null) {
            gid = Integer.parseInt(label.substring(1));
        } else if (ctx.FID() != null) {
            fid = Integer.parseInt(label.substring(2));
        } else if (ctx.SID() != null) {
            sid = label;
        } else if (ctx.QSID() != null) {
            sid = label.substring(1, label.length()-1); // strip quotes
        }

        // determine missing item identifier
        if (sid != null) {
            if (dict instanceof Dictionary) {
                fid = ((Dictionary)dict).fidOf(sid);
            } else {
                throw new RuntimeException("cannot determine identifier for item " + label
                        + " at " + ctx.getStart().getLine() + ":" + ctx.getStart().getCharPositionInLine()
                        + " because BasicDictionary does not contain string item identifiers");
            }
        } else if (gid >= 0) {
            fid = dict.fidOf(gid);
        } else { // fid >= 0
            fid = dict.containsFid(fid) ? fid : -1;
        }

        // check for errors
        if (fid < 0) {
            throw new RuntimeException("unknown item " + label + " at " + ctx.getStart().getLine()
                    + ":" + ctx.getStart().getCharPositionInLine());
        }

        // return result
        return fid;
    }

    static int asGid(BasicDictionary dict, PatExParser.ItemContext ctx) {
        // parse item
        String label = ctx.getText();
        String sid = null;
        int gid = -1;
        int fid = -1;
        if (ctx.INT() != null) {
            sid = label;
        } else if (ctx.GID() != null) {
            gid = Integer.parseInt(label.substring(1));
        } else if (ctx.FID() != null) {
            fid = Integer.parseInt(label.substring(2));
        } else if (ctx.SID() != null) {
            sid = label;
        } else if (ctx.QSID() != null) {
            sid = label.substring(1, label.length()-1); // strip quotes
        }

        // determine missing item identifier
        if (sid != null) {
            if (dict instanceof Dictionary) {
                gid = ((Dictionary)dict).gidOf(sid);
            } else {
                throw new RuntimeException("cannot determine identifier for item " + label
                        + " at " + ctx.getStart().getLine() + ":" + ctx.getStart().getCharPositionInLine()
                        + " because BasicDictionary does not contain string item identifiers");
            }
        } else if (fid >= 0) {
            gid = dict.gidOf(fid);
        } else { // gid >= 0
            gid = dict.containsGid(gid) ? gid : -1;
        }

        // check for errors
        if (gid < 0) {
            throw new RuntimeException("unknown item " + label + " at " + ctx.getStart().getLine()
                    + ":" + ctx.getStart().getCharPositionInLine());
        }

        // return result
        return gid;
    }

    static String asSid(BasicDictionary dict, PatExParser.ItemContext ctx) {
        String label = ctx.getText();
        if (!(dict instanceof Dictionary)) {
            throw new RuntimeException("cannot determine identifier for item " + label
                    + " at " + ctx.getStart().getLine() + ":" + ctx.getStart().getCharPositionInLine()
                    + " because BasicDictionary does not contain string item identifiers");
        }

        // parse item
        String sid = null;
        int gid = -1;
        int fid = -1;
        if (ctx.INT() != null) {
            sid = label;
        } else if (ctx.GID() != null) {
            gid = Integer.parseInt(label.substring(1));
        } else if (ctx.FID() != null) {
            fid = Integer.parseInt(label.substring(2));
        } else if (ctx.SID() != null) {
            sid = label;
        } else if (ctx.QSID() != null) {
            sid = label.substring(1, label.length()-1); // strip quotes
        }

        // determine missing item identifier
        if (sid != null) {
            sid = ((Dictionary) dict).containsSid(sid) ? sid : null;
        } else if (fid >= 0) {
            sid = ((Dictionary)dict).sidOfFid(fid);
        } else { // gid >= 0
            sid = ((Dictionary)dict).sidOfGid(gid);
        }

        // check for errors
        if (sid == null) {
            throw new RuntimeException("unknown item " + label + " at " + ctx.getStart().getLine()
                    + ":" + ctx.getStart().getCharPositionInLine());
        }

        // return result
        return sid;
    }

}
