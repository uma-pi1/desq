package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;

/**
 * Utility methods for (de)compressing posting lists. To be removed; use {@link PostingList} instead.
 */
@Deprecated
public final class OldPostingList {

	/**
	 * Appends compressed value v to the given posting list. Set v=0 for a
	 * separator, v=transactionId+1 for a transaction gid, and v=position+1 for a
	 * position.
	 */
	public static void addCompressed(int v, ByteArrayList postingList) {
		assert v >= 0;
		do {
			byte b = (byte) (v & 127);
			v >>= 7;
			if (v == 0) {
				postingList.add(b);
				break;
			} else {
				b += 128;
				postingList.add(b);
			}
		} while (true);
	}

	/** Iterator-like decompression of posting lists. */
	public static final class Decompressor {

		public ByteArrayList postingList;

		public int offset;

		public Decompressor() {
			this.postingList = null;
			this.offset = 0;

		}

		public Decompressor(ByteArrayList postingList) {
			this.postingList = postingList;
			this.offset = 0;
		}

		/** Is there another value in the posting? */
		public boolean hasNextValue() {
			return offset < postingList.size() && postingList.getByte(offset) != 0;
		}

		/**
		 * Returns the next transactionId/positopm in the posting. Throws an
		 * exception if the end of the posting has been reached (so be sure to use
		 * hasNextValue()).
		 */
		public int nextValue() {
			int result = 0;
			int shift = 0;
			do {
				byte b = postingList.getByte(offset);
				offset++;
				result += (b & 127) << shift;
				if (b < 0) {
					shift += 7;
				} else {
					break;
				}
			} while (true);
			return result - 1; // since we stored transactionId/positions incremented
													// by 1
		}

		/**
		 * Moves to the next posting in the posting list and returns true if such a
		 * posting exists.
		 */
		public boolean nextPosting() {
			do {
				offset++;
				if (offset >= postingList.size())
					return false;
			} while (postingList.getByte(offset - 1) != 0); // previous byte is not a
																									// separator byte
			return true;
		}
	}

	public static int merge(ByteArrayList listA, ByteArrayList listB, ByteArrayList postingList) {
		int support = 0;

		OldPostingList.Decompressor postingListA = new OldPostingList.Decompressor();
		OldPostingList.Decompressor postingListB = new OldPostingList.Decompressor();
		postingListA.postingList = listA;
		postingListA.offset = 0;
		postingListB.postingList = listB;
		postingListB.offset = 0;

		int aTransactionId = postingListA.nextValue();
		int bTransactionId = postingListB.nextValue();

		while (true) {

			/** Merge positions if same transaction gid */
			if (aTransactionId == bTransactionId) {
				if (postingList.size() > 0)
					OldPostingList.addCompressed(0, postingList);
				/** Add transaction gid */
				OldPostingList.addCompressed(aTransactionId + 1, postingList);
				support++;

				/** Merge positions and add */

				int bPosition = postingListB.nextValue();
				int aPosition = postingListA.nextValue();

				while (true) {

					if (aPosition < bPosition) {
						OldPostingList.addCompressed(aPosition + 1, postingList);
						if (postingListA.hasNextValue())
							aPosition = postingListA.nextValue();
						else
							break;
					} else if (aPosition > bPosition) {
						OldPostingList.addCompressed(bPosition + 1, postingList);
						if (postingListB.hasNextValue())
							bPosition = postingListB.nextValue();
						else
							break;
					} else {
						OldPostingList.addCompressed(aPosition + 1, postingList);

						if (postingListA.hasNextValue()) {
							aPosition = postingListA.nextValue();
							if (postingListB.hasNextValue()) {
								bPosition = postingListB.nextValue();
							} else {
								break;
							}
						} else if (postingListB.hasNextValue()) {
							bPosition = postingListB.nextValue();
							break;
						} else
							break;

					}
				}
				/** add left over positions to postingList */
				if (aPosition > bPosition) {
					do {
						OldPostingList.addCompressed(aPosition + 1, postingList);
						if (postingListA.hasNextValue()) {
							aPosition = postingListA.nextValue();
						} else
							break;

					} while (true);
				} else if (aPosition < bPosition) {
					do {
						OldPostingList.addCompressed(bPosition + 1, postingList);
						if (postingListB.hasNextValue()) {
							bPosition = postingListB.nextValue();
						} else
							break;
					} while (true);
				}

				/** Advance the postings as necessary */

				if (postingListA.nextPosting()) {
					aTransactionId = postingListA.nextValue();
					if (postingListB.nextPosting()) {
						bTransactionId = postingListB.nextValue();
					} else {
						break;
					}
				} else if (postingListB.nextPosting()) {
					bTransactionId = postingListB.nextValue();
					break;
				} else
					break;

			} else if (aTransactionId < bTransactionId) {

				/** Add transaction */
				if (postingList.size() > 0)
					OldPostingList.addCompressed(0, postingList);

				OldPostingList.addCompressed(aTransactionId + 1, postingList);
				support++;

				/** Add positions */
				while (postingListA.hasNextValue()) {
					int position = postingListA.nextValue();
					OldPostingList.addCompressed(position + 1, postingList);
				}
				/** Advance to next posting */
				if (postingListA.nextPosting()) {
					aTransactionId = postingListA.nextValue();
				} else
					break;
			} else if (aTransactionId > bTransactionId) {

				/** Add transaction */
				if (postingList.size() > 0)
					OldPostingList.addCompressed(0, postingList);

				OldPostingList.addCompressed(bTransactionId + 1, postingList);
				support++;

				/** Add positions */
				while (postingListB.hasNextValue()) {
					int position = postingListB.nextValue();
					OldPostingList.addCompressed(position + 1, postingList);
				}

				/** Advance to next posting */
				if (postingListB.nextPosting()) {
					bTransactionId = postingListB.nextValue();
				} else
					break;
			}
		}
		// Add leftover posting list
		if (aTransactionId > bTransactionId) {
			/** Add the current transaction and positions */
			OldPostingList.addCompressed(0, postingList);
			OldPostingList.addCompressed(aTransactionId + 1, postingList);
			support++;

			while (postingListA.hasNextValue()) {
				int position = postingListA.nextValue();
				OldPostingList.addCompressed(position + 1, postingList);
			}

			/** Add remaining transactions in this posting list */
			while (postingListA.nextPosting()) {
				aTransactionId = postingListA.nextValue();
				OldPostingList.addCompressed(0, postingList);
				OldPostingList.addCompressed(aTransactionId + 1, postingList);
				support++;

				while (postingListA.hasNextValue()) {
					int position = postingListA.nextValue();
					OldPostingList.addCompressed(position + 1, postingList);
				}
			}
		} else if (aTransactionId < bTransactionId) {
			/** Add the current transaction and positions */
			OldPostingList.addCompressed(0, postingList);
			OldPostingList.addCompressed(bTransactionId + 1, postingList);
			support++;

			while (postingListB.hasNextValue()) {
				int position = postingListB.nextValue();
				OldPostingList.addCompressed(position + 1, postingList);
			}

			/** Add remaining transactions in this posting list */
			while (postingListB.nextPosting()) {
				bTransactionId = postingListB.nextValue();
				OldPostingList.addCompressed(0, postingList);
				OldPostingList.addCompressed(bTransactionId + 1, postingList);
				support++;

				while (postingListB.hasNextValue()) {
					int position = postingListB.nextValue();
					OldPostingList.addCompressed(position + 1, postingList);
				}
			}

		}

		return support;

	}

}