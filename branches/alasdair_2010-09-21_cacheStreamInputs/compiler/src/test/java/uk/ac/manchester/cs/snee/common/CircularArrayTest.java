package uk.ac.manchester.cs.snee.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CircularArrayTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testIsEmpty_true() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(20);
		assertTrue(circularArray.isEmpty());
	}

	@Test
	public void testIsEmpty_false() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(20);
		circularArray.add(new Integer(2));
		assertFalse(circularArray.isEmpty());
	}

	@Test
	public void testSize_empty() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(20);
		assertEquals(0, circularArray.size());
	}

	@Test
	public void testSize_one() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(20);
		circularArray.add(new Integer(2));
		assertEquals(1, circularArray.size());
	}

	@Test
	public void testCapacity_empty() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(20);
		assertEquals(20, circularArray.capacity());
	}

	@Test
	public void testCapacity_twenty() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(20);
		circularArray.add(new Integer(2));
		assertEquals(20, circularArray.capacity());
	}
	
	@Test(expected=IndexOutOfBoundsException.class)
	public void testGetZero_empty() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(20);
		circularArray.get(0);
	}
	
	@Test
	public void testGetZero() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(20);
		circularArray.add(new Integer(0));
		assertEquals(0, circularArray.get(0).intValue());
	}
	
	@Test
	public void testCircularWrap() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 0; i < 5; i++) {
			circularArray.add(new Integer(i));
		}
		assertEquals(5, circularArray.size());
		assertEquals(5, circularArray.capacity());
		for (int i = 0; i < 5; i++) {
			assertEquals(i, circularArray.get(i).intValue());
		}
		circularArray.add(new Integer(5));
		assertEquals(6, circularArray.size());
		assertEquals(5, circularArray.capacity());
		for (int i = 1; i < 6; i++) {
			assertEquals(i, circularArray.get(i).intValue());
		}
	}
	
	@Test
	public void testGetOldest() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 0; i < 5; i++) {
			circularArray.add(new Integer(i));
//			System.out.println("i=" + i + " " + circularArray.getOldest());
			assertEquals(0, circularArray.getOldest().intValue());
		}
		circularArray.add(new Integer(5));
//		System.out.println("5 " + circularArray.getOldest());
		assertEquals(1, circularArray.getOldest().intValue());
	}
	
	@Test
	public void testGetNewest() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 0; i < 5; i++) {
			circularArray.add(new Integer(i));
//			System.out.println("i=" + i + " " + circularArray.getNewest());
			assertEquals(i, circularArray.getNewest().intValue());
		}
		circularArray.add(new Integer(5));
//		System.out.println("5 " + circularArray.getNewest());
		assertEquals(5, circularArray.getNewest().intValue());
	}
	
	@Test
	public void testCircularIterator_zero() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		circularArray.circularIterator(0);
	}
	
	@Test
	public void testCircularIterator_headNoWrap() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 1; i <= 5; i++) {
			circularArray.add(new Integer(i));
		}
		circularArray.circularIterator(1);
	}
	
	@Test(expected=IndexOutOfBoundsException.class)
	public void testCircularIterator_afterTailNoWrap() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 1; i <= 5; i++) {
			circularArray.add(new Integer(i));
		}
		circularArray.circularIterator(6);
	}
	
	@Test(expected=IndexOutOfBoundsException.class)
	public void testCircularIterator_afterTailWrap() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 1; i <= 8; i++) {
			circularArray.add(new Integer(i));
		}
		circularArray.circularIterator(9);
	}
	
	@Test
	public void testCircularIterator_noWrap() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 0; i < 5; i++) {
			circularArray.add(new Integer(i));
		}
		int expected = 0;
		for (Integer integer : circularArray) {
			assertEquals(expected, integer.intValue());
			expected++;
		}
		assertEquals(5, expected);
	}
	
	@Test
	public void testCircularIterator_hasNextFalse() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 0; i < 3; i++) {
			circularArray.add(new Integer(i));
		}
		Iterator<Integer> it = circularArray.circularIterator(2);
		it.next();
		assertFalse(it.hasNext());
	}
	
	@Test
	public void testCircularIterator_hasNextTrue() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 1; i <= 5; i++) {
			circularArray.add(new Integer(i));
		}
		Iterator<Integer> it = circularArray.circularIterator(1);
		assertTrue(it.hasNext());
	}

	@Test(expected=IndexOutOfBoundsException.class)
	public void testCircularIterator_beforeHeadNoWrap() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 0; i < 5; i++) {
			circularArray.add(new Integer(i));
		}
		circularArray.circularIterator(-1);
	}

	@Test(expected=IndexOutOfBoundsException.class)
	public void testCircularIterator_beforeHeadWrap() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 0; i < 8; i++) {
			circularArray.add(new Integer(i));
		}
		circularArray.circularIterator(1);
	}

	@Test
	public void testCircularIterator_iteratorNoWrap() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 0; i < 5; i++) {
			circularArray.add(new Integer(i));
		}
		Iterator<Integer> it = circularArray.iterator();
		int expected = 0;
		while (it.hasNext()) {
			assertEquals(expected, it.next().intValue());
			expected++;
		}
		assertEquals(5, expected);

	}

	@Test
	public void testCircularIterator_iteratorWrap() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 0; i < 13; i++) {
			circularArray.add(new Integer(i));
		}
		Iterator<Integer> it = circularArray.iterator();
		int expected = 8;
		while (it.hasNext()) {
			assertEquals(expected, it.next().intValue());
			expected++;
		}
		assertEquals(13, expected);

	}

	@Test
	public void testCircularIterator_headWrap() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 0; i < 8; i++) {
			circularArray.add(new Integer(i));
		}
		circularArray.circularIterator(3);
	}
	
	@Test
	public void testCircularIterator_wrap() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 0; i < 8; i++) {
			circularArray.add(new Integer(i));
		}
		int expected = 3;
		for (Integer integer : circularArray) {
			assertEquals(expected, integer.intValue());
			expected++;
		}
		assertEquals(8, expected);
	}
	
	@Test(expected=UnsupportedOperationException.class)
	public void testCircularIterator_remove() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 0; i < 3; i++) {
			circularArray.add(new Integer(i));
		}
		Iterator<Integer> it = circularArray.circularIterator(1);
		it.remove();
	}
	
	@Test
	public void testSubList_zeroStart() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 0; i < 5; i++) {
			circularArray.add(new Integer(i));
		}
		List<Integer> list = circularArray.subList(0, 2);
		assertEquals(2, list.size());
	}
	
	@Test(expected=IndexOutOfBoundsException.class)
	public void testSubList_beforeHead() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 0; i < 6; i++) {
			circularArray.add(new Integer(i));
		}
		circularArray.subList(0, 2);
	}
	
	@Test(expected=IndexOutOfBoundsException.class)
	public void testSubList_afterTail() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 0; i < 5; i++) {
			circularArray.add(new Integer(i));
		}
		circularArray.subList(1, 7);
	}
	
	@Test(expected=IndexOutOfBoundsException.class)
	public void testSubList_afterTailWrap() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 0; i < 6; i++) {
			circularArray.add(new Integer(i));
		}
		circularArray.subList(2, 8);
	}
	
	@Test
	public void testSubList_noWrap() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 0; i < 5; i++) {
			circularArray.add(new Integer(i));
		}
		List<Integer> list = circularArray.subList(0, 5);
		// Test size and content of sublist
		assertEquals(5, list.size());
		int expected = 0;
		for (Integer integer : list) {
			assertEquals(expected, integer.intValue());
			expected++;
		}
		assertEquals(5, expected);
	}
	
	@Test
	public void testSubList_wrap() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 0; i < 7; i++) {
			circularArray.add(new Integer(i));
		}
		List<Integer> list = circularArray.subList(2, 7);
		// Test size and content of sublist
		assertEquals(5, list.size());
		int expected = 2;
		for (Integer integer : list) {
			assertEquals(expected, integer.intValue());
			expected++;
		}
		assertEquals(7, expected);
	}
	
	@Test
	public void testSubList_wrapSubList() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 0; i < 7; i++) {
			circularArray.add(new Integer(i));
		}
		List<Integer> list = circularArray.subList(4, 5);
		// Test size and content of sublist
		assertEquals(1, list.size());
		int expected = 4;
		for (Integer integer : list) {
			assertEquals(expected, integer.intValue());
			expected++;
		}
		assertEquals(5, expected);
	}
	
	@Test
	public void testSubList_startEqEnd() {
		CircularArray<Integer> circularArray = 
			new CircularArray<Integer>(5);
		for (int i = 1; i <= 5; i++) {
			circularArray.add(new Integer(i));
		}
		List<Integer> list = circularArray.subList(2, 2);
		assertTrue(list.isEmpty());
	}

}
