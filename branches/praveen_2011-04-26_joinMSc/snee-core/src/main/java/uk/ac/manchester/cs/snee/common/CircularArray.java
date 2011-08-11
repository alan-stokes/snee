package uk.ac.manchester.cs.snee.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

public class CircularArray<E> implements Iterable<E> {

	private Logger logger = 
		Logger.getLogger(this.getClass().getName());
	
	private E[] array;
	private int firstIndex;
	private int lastIndex;
	private int capacity;
	private int numberElements = 0;
	//The id of the operator that initialised this DS
	private String operatorId;
	//This variable holds the total number of objects that has ever 
	//been inserted into this array
	private int totalNumberOfObjects = 0;

	/**
	 * Constructs a new instance of {@code ArrayList} with the specified
	 * capacity.
	 * 
	 * @param capacity
	 *            the initial capacity of this {@code ArrayList}.
	 */
	public CircularArray(int capacity) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER CircularArray() with capacity=" +
					capacity);
		}
		if (capacity <= 0) {
			throw new IllegalArgumentException();
		}
		firstIndex = lastIndex = 0;
		this.capacity = capacity;
		array = newElementArray(capacity);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN CircularArray()");
		}
	}
	
	/**
	 * Constructs a new instance of {@code ArrayList} with the specified
	 * capacity.
	 * 
	 * @param capacity
	 *            the initial capacity of this {@code ArrayList}.
	 */
	public CircularArray(int capacity, String id) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER CircularArray() with capacity=" +
					capacity);
		}
		if (capacity <= 0) {
			throw new IllegalArgumentException();
		}
		firstIndex = lastIndex = 0;
		this.capacity = capacity;
		array = newElementArray(capacity);
		setOperatorId(id);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN CircularArray()");
		}
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
	
	/**
	 * Constructs a new instance of {@code CircularArray} containing the elements of
	 * the specified collection. The size of the {@code CircularArray} will be set
	 * to the specified capacity.
   	 * 
   	 * @param collection
   	 *            the collection of elements to add.
	 */
	public CircularArray(int capacity, String opId, 
			Collection<? extends E> collection) {
		this(capacity);
		setOperatorId(opId);
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
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER add() with " + object);
		}
		// System.out.println("Add begin");
		// printIndexes();
		// Insert then increment		
		array[lastIndex] = object;
		numberElements++;
		totalNumberOfObjects++;
		if (logger.isTraceEnabled()) {
			logger.trace("Inserted object at index " + lastIndex
					+ "\n\tTotal number of inserted objects " + totalNumberOfObjects +
					"\n\tNumber of inserted objects " + numberElements
					+ "\n\tPosition of head " + firstIndex);
		}
		lastIndex = incrementPointer(lastIndex);
		/*
		 * Check to see if we are over writing first element If we are,
		 * increment the first element
		 */
		if (lastIndex == firstIndex) {			
			if (logger.isInfoEnabled() && getOperatorId()!= null) {
				logger.info("Object dropped in CircularArray for operator id: "+getOperatorId()+ "with size: "+size()+ 
						"and capacity: "+capacity+ " with last Index: "+lastIndex + " and firstIndex: "+firstIndex);
			}
			firstIndex = incrementPointer(firstIndex);			
			numberElements--;			
		}
		if (logger.isTraceEnabled()) {
			logger.trace("Next insert index " + lastIndex
					+ "\n\tPosition of head " + firstIndex);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN add() with true");
		}
		// System.out.println("Add begin");
		// printIndexes();
		return true;
	}	
	

	/**
	 * The last index is decremented to point to where the
	 * data is currently inserted, so that when the next
	 * insertion/add of data comes, the one to be replaced
	 * will be the last inserted element. 
	 */
	public void dropLastInsertedObject() {
		if (lastIndex == firstIndex) {
			lastIndex = decrementPointer(lastIndex);
		}
		
	}
	

	public void printIndexes() {
		System.out.println("Last Index: "+ lastIndex);
		System.out.println("First Index: "+ firstIndex);
	}

	/**
	 * Adds the objects in the specified collection to this {@code CircularArray}.
	 * @param collection
	 * 			the collection of objects.
	 * @return {@code true} if this {@code ArrayList} is modified, {@code false}
	 *         otherwise.
	 */
	public boolean addAll(Collection<? extends E> collection) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER addAll() collection size=" + 
					collection.size());
		}
		for (E ob : collection) {
			this.add(ob);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN addAll() number of elements " +
					numberElements);
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
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER get() with " + index);
		}
		int lowerBound = Math.max(0, totalNumberOfObjects - capacity);
		if (lowerBound <= index && index < totalNumberOfObjects) {
			int location = index % array.length; 
			if (logger.isTraceEnabled()) {
				logger.trace("Retrieve location " + location +
						"\n\tTotal number of inserted objects " + totalNumberOfObjects +
						"\n\tCurrent number of inserted elements " + numberElements +
						"\n\tlast index " + lastIndex +
						"\n\tPosition of head " + firstIndex);
			}
			E obj = array[location];
			if (logger.isDebugEnabled()) {
				logger.debug("RETURN get() with " + obj);
			}
			return obj;
		}
		int upperBound = totalNumberOfObjects - 1;
		String message = index + " out of range. " +
				"Valid range that can currently be retrieve: " +
				lowerBound + " to " + upperBound;
		logger.warn(message);
		throw new IndexOutOfBoundsException(message);
	}

	private int incrementPointer(int pointer) {
		return (pointer + 1) % array.length;
	}
	
	private int decrementPointer(int pointer) {		
		return (pointer - 1) % array.length;
	}
	
	/**
	 * This method retrieves the head of the queue and
	 * removes the item from the head of the queue. If
	 * there are no items in the queue, then it  returns
	 * null
	 * 
	 * @return 
	 */
	public E poll() {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER poll()");
		}
		E object = null;
		//System.out.println("Polling begin");
		//printIndexes();
		if (logger.isTraceEnabled()) {
			logger.trace(
					"\n\tTotal number of inserted objects " + totalNumberOfObjects +
					"\n\tCurrent number of inserted elements " + numberElements +
					"\n\tlast index " + lastIndex +
					"\n\tPosition of head " + firstIndex);
		}
		if (!isEmpty()) {
			object = getOldest();
			numberElements--;
			firstIndex = incrementPointer(firstIndex);
			//If the first index has incremented it to be equal to
			//the last index, then it means there are no more elements
			//in the queue and hence the pointers can be reset to 0
			if (size() == 0) {
				firstIndex = lastIndex = 0;
			}
		}
		//System.out.println("Polling end");
		//printIndexes();
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN poll() with " + ((object==null)?object:object.toString()));
		}
		return object;
	}
	
	/**
	 * Returns the number of elements that are currently present in this {@code CircularArray}.
	 * 
	 * @return the number of elements that are currently present in this {@code CircularArray}.
	 */
	public int size() {
		return numberElements;
	}
	
	/**
	 * Returns the number of elements that have been inserted in this {@code CircularArray}.
	 * 
	 * @return the number of elements that have been inserted in this {@code CircularArray}.
	 */
	public int totalObjectsInserted() {
		return totalNumberOfObjects;
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
		return array[totalNumberOfObjects - 1 % array.length];
	}
	
	/**
	 * Returns the newest element stored in this {@code CircularArray}.
	 * 
	 * @return the newest element stored in this {@code CircularArray}.
	 */
	public E getLatest() {
		if (lastIndex == 0) {
			return null;
		} else {
			return array[lastIndex-1];
		}
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
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER subList() [" + start + 
					", " + end + ")");
		}
		int lowerBound = Math.max(0, totalNumberOfObjects - capacity);
		if (lowerBound <= start && end <= totalNumberOfObjects) {
			List<E> elementList = new ArrayList<E>(end - start);
			for (int i = start; i < end; i++) {
				elementList.add(get(i));
			}
			if (logger.isDebugEnabled()) {
				logger.debug("RETURN subList() with " + elementList);
			}
			return elementList;
		}
		int upperBound = numberElements;
		String message = "Valid range that can currently be retrieve: " +
				lowerBound + " to " + upperBound;
		logger.warn(message);
		throw new IndexOutOfBoundsException(message);
	}
	
	/**
	 * @param operatorId the operatorId to set
	 */
	public void setOperatorId(String operatorId) {
		this.operatorId = operatorId;
	}

	/**
	 * @return the operatorId
	 */
	public String getOperatorId() {
		return operatorId;
	}

	public Iterator<E> iterator() {
		int lowerBound = Math.max(0, totalNumberOfObjects - capacity);
		return circularIterator(lowerBound);
	}
	
	public Iterator<E> circularIterator(int index) {
		int lowerBound = Math.max(0, totalNumberOfObjects - capacity);
		if (lowerBound <= index && index <= totalNumberOfObjects) {
			return new CircularIterator(index);
		}
		int upperBound = totalNumberOfObjects;
		String message = "Valid range that can currently be retrieve: " +
				lowerBound + " to " + upperBound;
		logger.warn(message);
		throw new IndexOutOfBoundsException(message);
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
			return lastPosition + 1 < totalNumberOfObjects;
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
