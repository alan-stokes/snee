package uk.ac.manchester.cs.snee.common;

import java.util.Arrays;
import java.util.Iterator;
import java.util.AbstractQueue;

import org.apache.log4j.Logger;

public class SNEEQueue<E> extends AbstractQueue<E> {
	
	private Logger logger = 
		Logger.getLogger(this.getClass().getName());
	
	private E[] array;

	private int firstIndex;
	private int lastIndex;
	private int numElements = 0;
	private boolean full;
	private boolean isIncreaseQueueCapacityEnabled;	
	private int capacityToIncrease;

	public SNEEQueue(int capacity) {
		firstIndex = 0;
		lastIndex = 0;
		// this.capacity = capacity;
		full = false;
		array = newElementArray(capacity);
		//TODO set property for isIncreaseQueueCapacityEnabled
		isIncreaseQueueCapacityEnabled = true;
		//Increasing by 50% of current capacity 
		capacityToIncrease = (int)(0.5*capacity);
	}

	@SuppressWarnings("unchecked")
	private E[] newElementArray(int size) {
		return (E[]) new Object[size];
	}

	@Override
	public synchronized boolean offer(E object) {
		if (full) {
			if (isIncreaseQueueCapacityEnabled) {
				increaseQueueCapacity(capacityToIncrease);
			} else {
				return false;
			}
		}
		array[lastIndex] = object;
		//System.out.println("Added"+object);
		numElements++;
		increaseQueueSize();
		return true;
	}

	private void increaseQueueCapacity(int incCapacity) {
		// TODO Auto-generated method stub
		E[] newArray = newElementArray(array.length);
		Iterator<E> qIterator = this.iterator();
		int i = 0;
		if (qIterator.hasNext()) {
			do {
				//System.out.println("i: "+i);
				if (logger.isDebugEnabled()) {
					logger.debug("i: "+i);
				}
				newArray[i++] = qIterator.next();
				//System.out.println("Element: "+newArray[i - 1]);
				if (logger.isDebugEnabled()) {
					logger.debug("Element: "+newArray[i - 1]);
				}
			} while (qIterator.hasNext());
		}		
		System.out.println("Current Capacity: "+ capacity());
		if (logger.isDebugEnabled()) {
			logger.debug("Current Capacity: "+ capacity());
		}
		array = Arrays.copyOf(newArray, capacity()+ incCapacity);
		System.out.println("Increased Capacity: "+ capacity());
		if (logger.isDebugEnabled()) {
			logger.debug("Current Capacity: "+ capacity());
		}
		firstIndex = 0;
		lastIndex = i;
		full = false;
	}

	private void increaseQueueSize() {
		lastIndex = increaseSize(lastIndex);
		// lastIndex = (lastIndex + 1) % capacity();
		full = (lastIndex == firstIndex);
	}

	private int increaseSize(int index) {
		return (index + 1) % capacity();
	}

	@Override
	public synchronized E poll() {
		E object = null;
		if (size() > 0) {
			object = array[firstIndex];
			//Setting the array position to
			//null for garbage collection
			array[firstIndex] = null;
			decreaseQueueSize();
		}
		return object;

	}

	private void decreaseQueueSize() {
		// firstIndex = (firstIndex + 1) % capacity();
		firstIndex = increaseSize(firstIndex);
		full = false;
	}

	@Override
	public E peek() {
		E object = null;
		if (size() > 0) {
			object = array[firstIndex];
		}
		return object;
	}

	@Override
	public Iterator<E> iterator() {
		return new QueueIterator();
	}

	@Override
	public int size() {
		if (full) {
			return capacity();
		}

		if (lastIndex >= firstIndex) {
			return lastIndex - firstIndex;
		} else {
			return lastIndex - firstIndex + capacity();
		}
	}

	public int capacity() {
		return array.length;
	}
	
	public void printIndexes() {
		System.out.println("Last Index: "+ lastIndex);
		System.out.println("First Index: "+ firstIndex);
	}

	private class QueueIterator implements Iterator<E> {

		private int nextPosition;
		private int last;		
		private boolean isFirst = true;
		private int numberElements;

		public QueueIterator() {
			nextPosition = firstIndex;
			numberElements = size();
		}

		@Override
		public boolean hasNext() {
			boolean hasNext = false;
			if (numberElements > 0) {
				if (isFirst) {
					//nextPosition = firstIndex;
					last = nextPosition; 
					nextPosition = increaseSize(nextPosition);
					hasNext = true;
					isFirst = false;
				} else if (nextPosition == lastIndex) {
					hasNext = false;
				} else {
					last = nextPosition; 
					nextPosition = increaseSize(nextPosition);
					hasNext = true;					
				}

			}
			return hasNext;
		}
		
		@Override
		public E next() {
			// TODO Auto-generated method stub
			return array[last];
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}
	/**
	 * Returns the oldest element currently stored in this {@code SNEEQueue}.
	 * 
	 * @return the oldest element currently stored in this {@code SNEEQueue}.
	 */
	public E getOldest() {
		return array[firstIndex];
	}
	
	/**
	 * Returns the newest element stored in this {@code SNEEQueue}.
	 * 
	 * @return the newest element stored in this {@code SNEEQueue}.
	 */
	public E getNewest() {		
		if (lastIndex == 0) {
			return null;
		} else {
			return array[lastIndex-1];
		}
	}

	/**
	 * Returns the number of elements that have been inserted in this {@code SNEEQueue}.
	 *  
	 * @return the number of elements that have been inserted in this {@code SNEEQueue}.
	 */
	public int totalObjectsInserted() {
		return numElements;		
	}

}
