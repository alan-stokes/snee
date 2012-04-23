package uk.ac.manchester.cs.snee.compiler.costmodels;
/****************************************************************************\ 
*                                                                            *
*  SNEE (Sensor NEtwork Engine)                                              *
*  http://code.google.com/p/snee                                             *
*  Release 1.0, 24 May 2009, under New BSD License.                          *
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class HashMapList<K,V> implements Serializable{

    
    /**
   * 6398551926792184048L
   */
  private static final long serialVersionUID = -6398551926792184048L;
    private HashMap<K, ArrayList<V>> mapping = 
      new HashMap<K, ArrayList<V>>();  
    
    public HashMapList() {
    }
  
    public void add(K key, V value) {
      if (!this.mapping.containsKey(key)) {
        this.mapping.put(key, new ArrayList<V>());
      }
    ArrayList<V> opInstArray = 
      this.mapping.get(key);
    if (!opInstArray.contains(value)) {
      opInstArray.add(value);     
    }
    }

  public void set(K key, Collection<V> valueColl) {
    this.mapping.put(key, 
      new ArrayList<V>(valueColl));
  }
    
  public ArrayList<V> get(K key) {
    if (!this.mapping.containsKey(key)) {
      return new ArrayList<V>();
    }
    return this.mapping.get(key);
  }
  
  
  public void remove(K key, V value) {
    ArrayList<V> opInstArray = 
      this.mapping.get(key);
    opInstArray.remove(value);
  }
  
  public void remove(K key) 
  { 
    this.mapping.remove(key);
  }
    
  public Set<K> keySet() {
    return this.mapping.keySet();
  }

  public void addAll(K key, ArrayList<V> valueArray)
  {
    Iterator<V> valueIterator = valueArray.iterator();
    while(valueIterator.hasNext())
    {
      V value = valueIterator.next();
      this.add(key, value);
    }
  }
  
  public void clear()
  {
    mapping.clear();
  }
  
}
