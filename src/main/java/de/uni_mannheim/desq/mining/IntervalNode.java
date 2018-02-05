package de.uni_mannheim.desq.mining;

public class IntervalNode implements Comparable<IntervalNode>{
    protected int start;
    protected int end;
    protected long support;
    protected long exclusiveSupport;
    protected boolean isFinalComplete;

    public IntervalNode(int start, int end, long support, long exclusiveSupport, boolean isFinalComplete){
        this.start = start;
        this.end = end;
        this.support = support;
        this.exclusiveSupport = exclusiveSupport;
        this.isFinalComplete = isFinalComplete;
    }

    /**
     *
     * @param other the other node
     * @return
     * >0 if is "more" than the other node
     * <0 if is "less" than other node
     * 0 if both are equal
     * Sort direction is per default ascending -> "less" comes before "more"
     * Example: [2,4]@3 has to be before [2,3]@2
     */
    @Override
    public int compareTo(IntervalNode other) {
        if(start != other.start)
            //smaller start than other -> before other (<0)
            return start - other.start;
        else if(end != other.end)
            //same start, but larger end than other -> before other (<0)
            return other.end - end;
        else if(isFinalComplete != other.isFinalComplete)
            //same start and end, but FinalComplete (other not) -> before other (<0)
            return (isFinalComplete) ? -1 : +1;
        else
            //if exactly same interval: higher support has precedence
            // (parent has higher support than child)
            // higher support than other -> before other (<0)
            return (int) (other.support - support);
    }

    @Override
    public String toString(){
        return "[" + start + "," + end + "]@" + support + "FC:" + isFinalComplete;
    }

    public boolean equals(IntervalNode other){
        return other != null
                && this.start == other.start
                && this.end == other.end
                && this.support == other.support
                && this.isFinalComplete == other.isFinalComplete;
    }
}
