package uk.ac.manchester.cs.snee.common;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;

import org.junit.Test;

public class SNEEQueueTest {

	public static void main(String[] args) {
		SNEEQueueTest tst = new SNEEQueueTest();
		//tst.testIncreaseQueueCapacity();
		//tst.testIncreaseCircledQueueCapacity();
		tst.testCircularQueue();
	}

	private void testCircularQueue() {
		CircularArray<Integer> sneeQueue = new CircularArray<Integer>(5);
		System.out.println("Size of queue: " + sneeQueue.capacity());
		for (int i = 0; i < 15; i++) {
			System.out.println("****************************************");
			System.out.println("Added: " + i + ": " + sneeQueue.add(i));
			System.out.println("****************************************");
			if (i % 2 == 0) {
				System.out.println("------------------------------------");
				System.out.println("Removed: " + sneeQueue.poll());
				System.out.println("------------------------------------");
			}
			/*if (i % 3 == 0) {
				System.out.println("Removed: " + sneeQueue.poll());
			}*/
		}
		printCircularQueue(sneeQueue);
	}

	private void printCircularQueue(CircularArray<Integer> sneeQueue) {
		Iterator<Integer> qIterator = sneeQueue.iterator();
		if (qIterator.hasNext()) {
			do {
				System.out.println(qIterator.next());
			} while (qIterator.hasNext());
		}
		System.out.println("Size of queue: " + sneeQueue.capacity());
		
	}

	@Test
	public void testIncreaseQueueCapacity() {
		SNEEQueue<Integer> sneeQueue = new SNEEQueue<Integer>(5);
		System.out.println("Size of queue: " + sneeQueue.capacity());
		for (int i = 0; i < 100; i++) {
			System.out.println("Added: " + i + ": " + sneeQueue.offer(i));
			if (i % 2 == 0) {
				System.out.println("Removed: " + sneeQueue.poll());
			}
			if (i % 3 == 0) {
				System.out.println("Removed: " + sneeQueue.poll());
			}
		}
		printQueue(sneeQueue);
	}
	
	@Test
	public void testIncreaseCircledQueueCapacity() {
		SNEEQueue<Integer> sneeQueue = new SNEEQueue<Integer>(5);
		System.out.println("Size of queue: " + sneeQueue.capacity());		
		for (int i = 0; i < 5; i++) {
			System.out.println("Added: " + i + ": " + sneeQueue.offer(i));			
		}
		System.out.println("Removed: " + sneeQueue.poll());
		System.out.println("Removed: " + sneeQueue.poll());
		for (int i = 5; i < 7; i++) {
			System.out.println("Added: " + i + ": " + sneeQueue.offer(i));			
		}
		sneeQueue.printIndexes();
		printQueue(sneeQueue);
		for (int i = 7; i < 12; i++) {
			System.out.println("Added: " + i + ": " + sneeQueue.offer(i));			
		}
		sneeQueue.printIndexes();
		printQueue(sneeQueue);
	}

	private void printQueue(SNEEQueue<Integer> sneeQueue) {
		Iterator<Integer> qIterator = sneeQueue.iterator();
		if (qIterator.hasNext()) {
			do {
				System.out.println(qIterator.next());
			} while (qIterator.hasNext());
		}
		System.out.println("Size of queue: " + sneeQueue.capacity());

	}

}
