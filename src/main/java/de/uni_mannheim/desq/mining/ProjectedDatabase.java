package de.uni_mannheim.desq.mining;

/**
 * Created by rgemulla on 19.07.2016.
 */
final class ProjectedDatabase {
    int itemFid;
    long support;
    int currentInputId = -1;
    int currentPosition;
    PostingList postingList;

    ProjectedDatabase() {
        this.postingList = new PostingList();
        clear();
    }

    void clear() {
        itemFid = -1;
        support = 0;
        currentInputId = -1;
        currentPosition = -1;
        postingList = new PostingList();
    }
}
