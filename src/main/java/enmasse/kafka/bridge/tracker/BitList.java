package enmasse.kafka.bridge.tracker;

/**
 * A list of bits.
 * 
 * Each bit tracks whether the messages at a given offset has been received.
 * {@link #set(int) allows to mark the receipt of a message.
 * {@link #lowestSetBits()} returns the commit offset
 * {@link #rshift(int)} shifts the bits.
 * 
 * A similar thing could be achieved using {@link java.util.BigInteger}, but since that's immutable
 * the bit setting and shifting would result in unnecessary garbage.
 * {@link java.util.BitSet} is unsuitable because it doesn't support a shift operation.
 *
 * Assuming an upper bound to the number of delivered messages, over the 
 * long run this class won't require allocations.
 */
public class BitList {

	private final static int BITS_PER_WORD = 64;
	
	private long[] words;
	
	private long offset;
	
	public BitList(long offset) {
		this(offset, 1);
	}
	
	private BitList(long offset, int n) {
		this.offset = offset;
		this.words = new long[n];
	}
	
	public long offset() {
		return this.offset;
	}

	/**
	 * Set the bit at the given index.
	 * @param index
	 */
	public void set(long offset) {
		final int index = (int)(offset-this.offset);
		// reallocate, if necessary 
		if (index / BITS_PER_WORD >= this.words.length) {
			long[] temp = new long[index / BITS_PER_WORD + 1];
			System.arraycopy(this.words, 0, temp, 0, this.words.length);
			this.words = temp;
		}
		
		// now set the bit
		this.words[index / BITS_PER_WORD] = this.words[index / BITS_PER_WORD] | (1L << index % BITS_PER_WORD);
	}

	/**
	 * Shift all the bits to the right the given number of places.
	 * @param places.
	 * @return the new offset
	 */
	public long rshift(int places) {
		if (places < 0) {
			throw new IllegalArgumentException();
		}
		int shiftWords = places / 64;
		int shiftBits = places % 64;
		// if places >= 64 then we can copy whole longs
		if (shiftWords > 0) {
			System.arraycopy(this.words, shiftWords, this.words, 0, this.words.length-shiftWords);
			while (shiftWords > 0) {
				this.words[this.words.length-shiftWords] = 0L;
				shiftWords--;
			}
		}
		// shift each long, but having copied the lost bits, 
		// and then copy those lost bits into the next long
		for (int i = 0; i < this.words.length; i++) {
			this.words[i] = this.words[i] >>> shiftBits;
			if (i + 1 < this.words.length) {
				long bottom = this.words[i + 1] & (-1L >> (BITS_PER_WORD - shiftBits));
				this.words[i] = this.words[i] | (bottom << (BITS_PER_WORD - shiftBits));
			}
		}
		this.offset += places;
		return this.offset;
	}

	/**
	 * Return the number of low order bits contiguously set.
	 * 
	 * For example if the bit list is
	 * 
	 * 1001010110111
	 * 
	 * then lowestSetBits() returns 3, because the lowest 3 bits are set.
	 * 
	 * This number is the maximum that can be {@linkplain #rshift(int) committed} 
	 * without claiming to handled any unaccepted messages.
	 * 
	 * @return the number of low order bits contiguously set.
	 */
	public long lowestSetBits() {
		// Alternative algo: find the the lowest n bits set
		//   ~x & (x+1)
		// and then switch?
		
		// Iterate looking for the first unset bit
		long low = 0;
		OUTER: for (int ii = 0; ii < this.words.length; ii++) {
			if (this.words[ii] != -1L) {
				for (int jj = 0; jj < BITS_PER_WORD; jj++) {
					if ((this.words[ii] & (1 << jj)) == 0) {
						low = BITS_PER_WORD * (long)ii + jj;
						break OUTER;
					}
				}
			}
		}
		return low;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("offset:").append(this.offset).append(" bits:");
		
		for (int i = this.words.length-1; i >= 0; i--) {
			String s = Long.toBinaryString(this.words[i]);
			//if (i != this.words.length-1) {
				for (int j = s.length(); j < BITS_PER_WORD; j++) {
					sb.append('0');
				}
			//} 
			sb.append(s);
		}
		return sb.toString();
	}


}
