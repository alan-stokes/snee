package uk.ac.manchester.cs.snee.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class CircularArray<E> implements Iterable<E> {

	private E[] array;
	private int firstIndex;
	private int lastIndex;
	private int capacity;
	private int numberElements = 0;

	/**
	 * Constructs a new instance of {@code ArrayList} with the specified
	 * capacity.
	 * 
	 * @param capacity
	 *            the initial capacity of this {@code ArrayList}.
	 */
	public CircularArray(int capacity) {
		if (capacity <= 0) {
			throw new IllegalArgumentException();
		}
		firstIndex = lastIndex = 0;
		this.capacity = capacity;
		array = newElementArray(capacity);
	}
	
	/**
	 * Constructs a new instance of {@code CircularArray} containing the elements of
	 * the specified collection. The size of the {@code CircularArray} will be set
	 * to the specified capacity.
   	 * 
   	 * @param collection
   	 *            the collection of elements to add.
	 */
	public CircularArray(int capacity, 
			Collection<? extends E> collection) {
		this(capacity);
		this.addAll(collection);
	}
	
	@SuppressWarnings("unchecked")
	private E[] newElementArray(int size) {
		return (E[]) new Object[size + 1];
	}
	
	/**
	 * Returns {@code true} if this {@code CircularArray} contains no elements.
	 * 
	 * @return {@code true} if this {@code CircularArray} contains no elements.
	 */
	public boolean isEmpty() {
		return (numberElements == 0);
	}
	
	/**
	 * Adds the specified object at the end of this {@code CircularArray}.
	 * @param object
	 * 			  the object to add.
	 * @return always true
	 */
	public boolean add(E object) {
		// Insert then increment
		array[lastIndex] = object;
		numberElements++;
		lastIndex = incrementPointer(lastIndex);
		/* 
		 * Check to see if we are over writing first element
		 * If we are, increment the first element
		 */
		if (lastIndex == firstIndex) {
			firstIndex = incrementPointer(firstIndex);
		}
		return true;
	}
	
	/**
	 * Adds the objects in the specified collection to this {@code CircularArray}.
	 * @param collection
	 * 			the collection of objects.
	 * @return {@code true} if this {@code ArrayList} is modified, {@code false}
	 *         otherwise.
	 */
	public boolean addAll(Collection<? extends E> collection) {
		for (E ob : collection) {
			this.add(ob);
		}
		return true;
	}
	
	/**
	 * Returns the element at the specified position in this {@code CircularArray}.
	 * 
	 * @param index index of the element to return
	 * 
	 * @return the element at the specified position in the list if it has not been overwritten.
	 */
	public E get(int index) {
		int lowerBound = Math.max(0, numberElements - capacity);
		if (lowerBound <= index && index < numberElements) {
			int location = index % array.length; 
			return array[location];
		}
		int upperBound = numberElements - 1;
		throw new IndexOutOfBoundsException(
				"Valid range that can currently be retrieve: " +
					lowerBound + " to " + upperBound);
	}

	private int incrementPointer(int pointer) {
		return (pointer + 1) % array.length;
	}
	
	/**
	 * Returns the number of elements that have been inserted in this {@code CircularArray}.
	 * 
	 * @return the number of elements that have been inserted in this {@code CircularArray}.
	 */
	public int size() {
		return numberElements;
	}
	
	/**
	 * Returns the number of elements that can be stored in this {@code CircularArray}.
	 * 
	 * @return the number of elements that can be stored in this {@code CircularArray}.
	 */
	public int capacity() {
		return capacity;
	}

	/**
	 * Returns the oldest element currently stored in this {@code CircularArray}.
	 * 
	 * @return the oldest element currently stored in this {@code CircularArray}.
	 */
	public E getOldest() {
		return array[firstIndex];
	}

	/**
	 * Returns the newest element stored in this {@code CircularArray}.
	 * 
	 * @return the newest element stored in this {@code CircularArray}.
	 */
	public E getNewest() {
		return array[numberElements - 1 % array.length];
	}
	
	/**
	 * Returns a part of consecutive elements of this {@code CircularArray} as a view. The
	 * returned view will be of zero length if start equals end. 
	 * <p>
	 * The subList is not modified when the {@code CircularArray} is updated.
	 * 
	 * @param start
	 *            start index of the subList (inclusive).
	 * @param end
	 *            end index of the subList, (exclusive).
	 * @return a subList view of this list starting from {@code start}
	 *         (inclusive), and ending with {@code end} (exclusive)
	 * @throws IndexOutOfBoundsException
	 *             if (start < {@code head} || end > size())
	 * @throws IllegalArgumentException
	 *             if (start > end)
	 */
	public List<E> subList(int start, int end) {
		if (firstIndex <= start && end <= numberElements) {
			List<E> elementList = new ArrayList<E>(end - start);
			for (int i = start; i < end; i++) {
				elementList.add(get(i));
			}
			return elementList;
		}
		throw new IndexOutOfBoundsException();
	}
	
	public Iterator<E> iterator() {
		return circularIterator(firstIndex);
	}
	
	public Iterator<E> circularIterator(int index) {
		if (firstIndex <= index && index <= numberElements) {
			return new CircularIterator(index);
		}
		throw new IndexOutOfBoundsException();
	}
	
	/**
	 * A read-only iterator over the {@code CircularArray}.
	 * 
	 * @param <E>
	 */
	private class CircularIterator implements Iterator<E> {

		/**
		 * Initiate the position to be less than the head 
		 */
		int lastPosition = firstIndex - 1;
		
		public CircularIterator(int start) {
			lastPosition = start - 1;
		}
		
		@Override
		public boolean hasNext() {
			return lastPosition + 1 < numberElements;
		}

		@Override
		public E next() {
			lastPosition++;
			E result = get(lastPosition);
			return result;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}
	
}
