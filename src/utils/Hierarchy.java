package utils;

public interface Hierarchy {
	public int[] getParents(int itemId);
	public int[] getAncestors(int itemId);
	public boolean hasParent(int itemId);
	public boolean isParent(int itemId, int parentId);
	
}