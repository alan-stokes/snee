/****************************************************************************\
*                                                                            *
*  SNEE (Sensor NEtwork Engine)                                              *
*  http://snee.cs.manchester.ac.uk/                                          *
*  http://code.google.com/p/snee                                             *
*                                                                            *
*  Release 1.x, 2009, under New BSD License.                                 *
*                                                                            *
*  Copyright (c) 2009, University of Manchester                              *
*  All rights reserved.                                                      *
*                                                                            *
*  Redistribution and use in source and binary forms, with or without        *
*  modification, are permitted provided that the following conditions are    *
*  met: Redistributions of source code must retain the above copyright       *
*  notice, this list of conditions and the following disclaimer.             *
*  Redistributions in binary form must reproduce the above copyright notice, *
*  this list of conditions and the following disclaimer in the documentation *
*  and/or other materials provided with the distribution.                    *
*  Neither the name of the University of Manchester nor the names of its     *
*  contributors may be used to endorse or promote products derived from this *
*  software without specific prior written permission.                       *
*                                                                            *
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS   *
*  IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, *
*  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR    *
*  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR          *
*  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,     *
*  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,       *
*  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR        *
*  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF    *
*  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING      *
*  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS        *
*  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.              *
*                                                                            *
\****************************************************************************/
package uk.ac.manchester.cs.snee.evaluator.types;

import java.util.Collection;
import java.util.Iterator;

import java.util.*;
import java.io.*;

@SuppressWarnings("serial")
public class CircularList extends AbstractList<Object>
implements List<Object>, Serializable
{
	private Object[] elementData;
	// head points to the first logical element in the array, and
	// tail points to the element following the last.  This means
	// that the list is empty when head == tail.  It also means
	// that the elementData array has to have an extra space in it.
	private int head=0, tail=0;
	// Strictly speaking, we don't need to keep a handle to size,
	// as it can be calculated programmatically, but keeping it
	// makes the algorithms faster.
	private int size=0;

	public CircularList() {
		this(10);
	}

	public CircularList(int size) {
		elementData = new Object[size];
	}

	public CircularList(Collection c) {
		tail = c.size();
		elementData = new Object[c.size()];
		c.toArray(elementData);
	}

	// The convert() method takes a logical index (as if head was
	// always 0) and calculates the index within elementData
	private int convert(int index) {
		return (index + head) % elementData.length;
	}

	public boolean isEmpty() {
		return head == tail; // or size == 0
	}

	// We use this method to ensure that the capacity of the
	// list will suffice for the number of elements we want to
	// insert.  If it is too small, we make a new, bigger array
	// and copy the old elements in.
	public void ensureCapacity(int minCapacity) {
		int oldCapacity = elementData.length;
		if (minCapacity > oldCapacity) {
			int newCapacity = (oldCapacity * 3)/2 + 1;
			if (newCapacity < minCapacity)
				newCapacity = minCapacity;
			Object newData[] = new Object[newCapacity];
			toArray(newData);
			tail = size;
			head = 0;
			elementData = newData;
		}
	}

	public int size() {
		// the size can also be worked out each time as:
		// (tail + elementData.length - head) % elementData.length
		return size;
	}

	public boolean contains(Object elem) {
		return indexOf(elem) >= 0;
	}

	public int indexOf(Object elem) {
		if (elem == null) {
			for (int i = 0; i < size; i++)
				if (elementData[convert(i)]==null)
					return i;
		} else {
			for (int i = 0; i < size; i++)
				if (elem.equals(elementData[convert(i)]))
					return i;
		}
		return -1;
	}

	public int lastIndexOf(Object elem) {
		if (elem == null) {
			for (int i = size-1; i >= 0; i--)
				if (elementData[convert(i)]==null)
					return i;
		} else {
			for (int i = size-1; i >= 0; i--)
				if (elem.equals(elementData[convert(i)]))
					return i;
		}
		return -1;
	}

	public Object[] toArray() {
		return toArray(new Object[size]);
	}

	public Object[] toArray(Object a[]) {
		if (a.length < size)
			a = (Object[])java.lang.reflect.Array.newInstance(
					a.getClass().getComponentType(), size);
		if (head < tail) {
			System.arraycopy(elementData, head, a, 0, tail-head);
		} else {
			System.arraycopy(elementData, head, a, 0,
					elementData.length-head);
			System.arraycopy(elementData, 0, a, elementData.length-head,
					tail);
		}
		if (a.length > size)
			a[size] = null;
		return a;
	}

	private void rangeCheck(int index) {
		if (index >= size || index < 0)
			throw new IndexOutOfBoundsException(
					"Index: "+index+", Size: "+size);
	}

	public Object get(int index) {
		rangeCheck(index);
		return elementData[convert(index)];
	}

	public Object set(int index, Object element) {
		modCount++;
		rangeCheck(index);
		Object oldValue = elementData[convert(index)];
		elementData[convert(index)] = element;
		return oldValue;
	}

	public boolean add(Object o) {
		modCount++;
		// We have to have at least one empty space
		ensureCapacity(size + 1 + 1);
		elementData[tail] = o;
		tail = (tail+1)%elementData.length;
		size++;
		return true;
	}

	// This method is the main reason we re-wrote the class.
	// It is optimized for removing first and last elements
	// but also allows you to remove in the middle of the list.
	public Object remove(int index) {
		modCount++;
		rangeCheck(index);
		int pos = convert(index);
		// an interesting application of try/finally is to avoid
		// having to use local variables
		try {
			return elementData[pos];
		} finally {
			elementData[pos] = null; // Let gc do its work
			// optimized for FIFO access, i.e. adding to back and
			// removing from front
			if (pos == head) {
				head = (head+1)%elementData.length;
			} else if (pos == tail) {
				tail = (tail-1+elementData.length)%elementData.length;
			} else {
				if (pos > head && pos > tail) { // tail/head/pos
					System.arraycopy(elementData, head, elementData, head+1,
							pos-head);
					head = (head+1)%elementData.length;
				} else {
					System.arraycopy(elementData, pos+1, elementData, pos,
							tail-pos-1);
					tail = (tail-1+elementData.length)%elementData.length;
				}
			}
			size--;
		}
	}

	public void clear() {
		modCount++;
		// Let gc do its work
		for (int i=head; i!=tail; i=(i+1)%elementData.length)
			elementData[i] = null;
		head = tail = size = 0;
	}

	public boolean addAll(Collection c) {
		modCount++;
		int numNew = c.size();
		// We have to have at least one empty space
		ensureCapacity(size + numNew + 1);
		Iterator e = c.iterator();
		for (int i=0; i < numNew; i++) {
			elementData[tail] = e.next();
			tail = (tail+1)%elementData.length;
			size++;
		}
		return numNew != 0;
	}

	public void add(int index, Object element) {
		throw new UnsupportedOperationException(
		"This method left as an exercise to the reader ;-)");
	}

	public boolean addAll(int index, Collection c) {
		throw new UnsupportedOperationException(
		"This method left as an exercise to the reader ;-)");
	}

	private synchronized void writeObject(ObjectOutputStream s)
	throws IOException {
		s.writeInt(size);
		for (int i=head; i!=tail; i = (i+1)%elementData.length)
			s.writeObject(elementData[i]);
	}

	private synchronized void readObject(ObjectInputStream s)
	throws IOException, ClassNotFoundException {
		// Read in size of list and allocate array
		head = 0;
		size = tail = s.readInt();
		elementData = new Object[tail];
		// Read in all elements in the proper order.
		for (int i=0; i < tail; i++)
			elementData[i] = s.readObject();
	}
}
