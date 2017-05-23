package enmasse.kafka.bridge;

import static org.junit.Assert.*;
import org.junit.Test;

import enmasse.kafka.bridge.tracker.BitList;

public class BitListTest {

	@Test
	public void singleWord() {
		BitList bl = new BitList(0);
		assertEquals(0, bl.lowestSetBits());
		
		// message 5 accepted
		bl.set(5);
		assertEquals(0, bl.lowestSetBits());
		assertEquals(0, bl.offset());

		// message 0 accepted
		bl.set(0);
		assertEquals(1, bl.lowestSetBits());
		assertEquals(0, bl.offset());
		
		// message 3 accepted
		bl.set(3);
		assertEquals(1, bl.lowestSetBits());
		assertEquals(0, bl.offset());
		
		// message 2 accepted
		bl.set(2);
		assertEquals(1, bl.lowestSetBits());
		assertEquals(0, bl.offset());
		
		// message 1 accepted
		bl.set(1);
		assertEquals(4, bl.lowestSetBits());
		assertEquals(0, bl.offset());
		
		// commit for message 0
		assertEquals(1, bl.rshift(1));
		assertEquals(3, bl.lowestSetBits());
		assertEquals(1, bl.offset());

		// commit for messages 1..5
		assertEquals(4, bl.rshift(3));
		assertEquals(0, bl.lowestSetBits());
		assertEquals(4, bl.offset());
	}

	@Test
	public void multiWord() {
		// test bitlist with > 64 bits (i.e. >1 word)
		BitList bl = new BitList(0);
		bl.set(0);
		assertEquals(1, bl.lowestSetBits());
		assertEquals(0, bl.offset());
		
		for (int i = 2; i < 70; i++) {
			bl.set(i);
			assertEquals(1, bl.lowestSetBits());
		}
		
		bl.set(1);
		assertEquals(70, bl.lowestSetBits());
		
		assertEquals(63, bl.rshift(63));
		System.out.println(bl);
		assertEquals(7, bl.lowestSetBits());
	}
	
	@Test
	public void multiWordShift() {
		// test shifts > 64
		BitList bl = new BitList(0);
		bl.set(0);
		assertEquals(1, bl.lowestSetBits());
		assertEquals(0, bl.offset());
		
		for (int i = 2; i < 70; i++) {
			bl.set(i);
			assertEquals(1, bl.lowestSetBits());
		}
		
		bl.set(1);
		assertEquals(70, bl.lowestSetBits());

		assertEquals(70, bl.rshift(70));
		assertEquals(0, bl.lowestSetBits());
	}
	
	@Test
	public void multiWordShift2() {
		// test shifts > 64
		BitList bl = new BitList(0);
		bl.set(0);
		assertEquals(1, bl.lowestSetBits());
		assertEquals(0, bl.offset());
		
		for (int i = 2; i < 70; i++) {
			bl.set(i);
			assertEquals(1, bl.lowestSetBits());
		}
		
		bl.set(1);
		assertEquals(70, bl.lowestSetBits());

		assertEquals(69, bl.rshift(69));
		assertEquals(1, bl.lowestSetBits());
	}
}
