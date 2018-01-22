package de.uni_mannheim.desq.mining;

public class IntervalNode implements Comparable<IntervalNode>{
    protected int start;
    protected int end;
    protected long support;

    public IntervalNode(int start, int end, long support){
        this.start = start;
        this.end = end;
        this.support = support;
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
        else
            //if exactly same interval: higher support has precedence
            // (parent has higher support than child)
            // higher support than other -> before other (<0)
            return (int) (other.support - support);
    }

    @Override
    public String toString(){
        return "[" + start + "," + end + "]@" + support;
    }
}
