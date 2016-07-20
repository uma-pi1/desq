package de.uni_mannheim.desq.mining;

/**
 * Created by rgemulla on 19.07.2016.
 */
final class ProjectedDatabase {
    int itemFid;
    long support;
    int lastInputId;
    int lastPosition;
    NewPostingList postingList;

    ProjectedDatabase() {
        this.postingList = new NewPostingList();
        clear();
    }

    void clear() {
        itemFid = -1;
        support = 0;
        lastInputId = -1;
        lastPosition = -1;
        postingList = new NewPostingList();
    }
}
