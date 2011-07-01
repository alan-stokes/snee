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
package uk.ac.manchester.cs.snee.compiler.queryplan.expressions;

/**
 * Represents the type of compare.
 * Includes the Nesc symbol and if this type results in boolean output. 
 * 
 * @author Christian
 */
public enum MultiType {

	//Mathematical types
	/** Addition. */
	ADD (" + ", false),
	/** Boolean and. */
	AND (" && ", true),
	/** Dision. */
	DIVIDE (" / ", false),
	/** Boolean Equals. */
	EQUALS (" == ", true),
	/** Boolean Greater than. */
	GREATERTHAN (" > ", true),
	/** Boolean Greater than of Equals. */
	GREATERTHANEQUALS (" >= ", true),
	/** Boolean Less than. */
	LESSTHAN (" < ", true),
	/** Boolean less than or equals.*/
	LESSTHANEQUALS (" <= ", true),
	/** Minus or subtraction. */
	MINUS (" - ", false),
	/** Minus or subtraction. */
	MOD (" % ", false),
	/** Multiplication. */
	MULTIPLY (" * ", false),
	/** Not equals. */
	NOTEQUALS (" ! =", true),
	/** Boolean or. */
	OR (" || ", true), // Nesc To be confirmed
	/** Power. */
	POWER (" ^ ", false),// Nesc To be confirmed
	/** SquareRoot. 
	 * Nsec code generation to do. */
	SQUAREROOT (" SQUAREROOT ",false);

	/** Hold the nesC string representation of the selected value. */
	private String nesC;

	/**
	 * Records if this type results in Boolean data.
	 */
	private boolean booleanDataType;

	/** Constructor.
	 * 
	 * @param s One of the Enum Values
	 * @param b Will this result in Boolean data
	 */
	private MultiType(String s, boolean b) {
		this.nesC = s;
		this.booleanDataType = b;
	}	

	/**
	 * The Enum as a String.
	 * 
	 * @return String representation of this enum.
	 */ 
	public String toString() {
		return this.nesC;
	}

	/** 
	 * The nesc symbols required for this combination.
	 * @return The symbols to be used in the ensc code.
	 */
	public String getNesCSymbol() {
		return nesC;
	}
	
	/**
	 * checks if this type results in Boolean data.
	 * @return True if expression evaluates to a boolean.
	 */
	public boolean isBooleanDataType() {
		return booleanDataType;
	}

}
