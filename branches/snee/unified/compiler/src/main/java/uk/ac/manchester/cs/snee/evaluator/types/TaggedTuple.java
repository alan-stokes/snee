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

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import uk.ac.manchester.cs.snee.common.Constants;

/**
 * TaggedTuple extends the Tuple class by providing a evaluation time and an
 * index for a tuple
 */
public class TaggedTuple implements Output {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6403031060390197325L;

	private int _index = -1;
	private long _evalTime;
	private Tuple _tuple;

	DateFormat dateFormat =
		new SimpleDateFormat(Constants.TIMESTAMP_FORMAT);
	
	public TaggedTuple(Tuple tuple) {
		_tuple = tuple;
		_evalTime = System.currentTimeMillis();
	}
	
	public long getEvalTime() {
		return _evalTime;
	}
	
	public int getIndex() {
		return _index;
	}
	
	public void setIndex(int index) {
		_index = index;
	}

	public Tuple getTuple() {
		return _tuple;
	}

	public void setTuple(Tuple tuple) {
		_tuple = tuple;
	}
	
	public String toString() {
		String retString="(["+ _index +","+dateFormat.format(_evalTime)+"],[";
		retString += _tuple.toString();
		retString += "])";
		return retString;
	}

}
