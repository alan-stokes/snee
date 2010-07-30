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
package uk.ac.manchester.cs.snee.common.options;

/* This code in this class taken from http://www.javaworld.com/javaworld/jw-08-2004/jw-0816-command.html?page=1,
 * and was written by Dr Matthias Laux, JavaWorld.com, 08/16/04
 * 
 * The copyright statement at http://www.javaworld.com/javaworld/common/jw-copyright.html 
 * states that "Content derived from JavaWorld may be reproduced in print or displayed online and 
 * distributed, for free, in limited quantities for nonprofit, educational purposes with proper attribution." 
 */

/**
 * This class holds all the data for an option. This includes the prefix, the key, the separator
 * (for value options), the multiplicity, and all the other settings describing the option. The class
 * is designed to be only a data container from a user perspective, i. e. the user has read-access to
 * any data determined by the {@link Options#check()}, but not access to any of the other methods
 * which are used internally for the operation of the actual check.
 */

public class OptionData {

    private final static String CLASS = "OptionData";

    private Options.Prefix prefix = null;

    private String key = null;

    private boolean detail = false;

    private Options.Separator separator = null;

    private boolean value = false;

    private Options.Multiplicity multiplicity = null;

    private java.util.regex.Pattern pattern = null;

    private int counter = 0;

    private java.util.ArrayList<String> values = null;

    private java.util.ArrayList<String> details = null;

    /**
     * The constructor
     */

    OptionData(Options.Prefix prefix, String key, boolean detail,
	    Options.Separator separator, boolean value,
	    Options.Multiplicity multiplicity) {

	if (prefix == null)
	    throw new IllegalArgumentException(CLASS
		    + ": prefix may not be null");
	if (key == null)
	    throw new IllegalArgumentException(CLASS + ": key may not be null");
	if (separator == null)
	    throw new IllegalArgumentException(CLASS
		    + ": separator may not be null");
	if (multiplicity == null)
	    throw new IllegalArgumentException(CLASS
		    + ": multiplicity may not be null");

	//.... The data describing the option

	this.prefix = prefix;
	this.key = key;
	this.detail = detail;
	this.separator = separator;
	this.value = value;
	this.multiplicity = multiplicity;

	//.... Create the pattern to match this option

	if (value) {
	    if (separator == Options.Separator.BLANK) {
		if (detail) {
		    pattern = java.util.regex.Pattern.compile(prefix.getName()
			    + key + "((\\w|\\.)+)$");
		} else {
		    pattern = java.util.regex.Pattern.compile(prefix.getName()
			    + key + "$");
		}
	    } else {
		if (detail) {
		    pattern = java.util.regex.Pattern.compile(prefix.getName()
			    + key + "((\\w|\\.)+)" + separator.getName()
			    + "(.+)$");
		} else {
		    pattern = java.util.regex.Pattern.compile(prefix.getName()
			    + key + separator.getName() + "(.+)$");
		}
	    }
	} else {
	    pattern = java.util.regex.Pattern.compile(prefix.getName() + key
		    + "$");
	}

	//.... Structures to hold result data

	if (value) {
	    values = new java.util.ArrayList<String>();
	    if (detail)
		details = new java.util.ArrayList<String>();
	}

    }

    /**
     * Getter method for <code>prefix</code> property
     * <p>
     * @return The value for the <code>prefix</code> property
     */

    Options.Prefix getPrefix() {
	return prefix;
    }

    /**
     * Getter method for <code>key</code> property
     * <p>
     * @return The value for the <code>key</code> property
     */

    String getKey() {
	return key;
    }

    /**
     * Getter method for <code>detail</code> property
     * <p>
     * @return The value for the <code>detail</code> property
     */

    boolean useDetail() {
	return detail;
    }

    /**
     * Getter method for <code>separator</code> property
     * <p>
     * @return The value for the <code>separator</code> property
     */

    Options.Separator getSeparator() {
	return separator;
    }

    /**
     * Getter method for <code>value</code> property
     * <p>
     * @return The value for the <code>value</code> property
     */

    boolean useValue() {
	return value;
    }

    /**
     * Getter method for <code>multiplicity</code> property
     * <p>
     * @return The value for the <code>multiplicity</code> property
     */

    Options.Multiplicity getMultiplicity() {
	return multiplicity;
    }

    /**
     * Getter method for <code>pattern</code> property
     * <p>
     * @return The value for the <code>pattern</code> property
     */

    java.util.regex.Pattern getPattern() {
	return pattern;
    }

    /**
     * Get the number of results found for this option, which is number of times the key matched
     * <p>
     * @return The number of results
     */

    public int getResultCount() {
	if (value) {
	    return values.size();
	} else {
	    return counter;
	}
    }

    /**
     * Get the value with the given index. The index can range between 0 and {@link #getResultCount()}<code> - 1</code>.
     * However, only for value options, a non-<code>null</code> value will be returned. Non-value options always
     * return <code>null</code>.
     * <p>
     * @param index The index for the desired value
     * <p>
     * @return The option value with the given index
     * <p>
     * @throws IllegalArgumentException If the value for <code>index</code> is out of bounds
     */

    public String getResultValue(int index) {
	if (!value)
	    return null;
	if (index < 0 || index >= getResultCount())
	    throw new IllegalArgumentException(CLASS
		    + ": illegal value for index");
	return values.get(index);
    }

    //IG added
    public boolean getBooleanResultValue(int index) {
	//if (!value) return null;
	if (index < 0 || index >= getResultCount())
	    throw new IllegalArgumentException(CLASS
		    + ": illegal value for index");
	if (values.get(index).toLowerCase().equals("true")) {
	    return true;
	} else {
	    return false;
	}
    }

    //IG added
    public int getIntResultValue(int index) {
	//if (!value) return null;
	if (index < 0 || index >= getResultCount())
	    throw new IllegalArgumentException(CLASS
		    + ": illegal value for index");
	return new Integer(values.get(index)).intValue();
    }

    /**
     * Get the detail with the given index. The index can range between 0 and {@link #getResultCount()}<code> - 1</code>.
     * However, only for value options which take details, a non-<code>null</code> detail will be returned. Non-value options
     * and value options which do not take details always return <code>null</code>.
     * <p>
     * @param index The index for the desired value
     * <p>
     * @return The option detail with the given index
     * <p>
     * @throws IllegalArgumentException If the value for <code>index</code> is out of bounds
     */

    public String getResultDetail(int index) {
	if (!detail)
	    return null;
	if (index < 0 || index >= getResultCount())
	    throw new IllegalArgumentException(CLASS
		    + ": illegal value for index");
	return details.get(index);
    }

    /**
     * Store the data for a match found
     */

    void addResult(String valueData, String detailData) {
	if (value) {
	    if (valueData == null)
		throw new IllegalArgumentException(CLASS
			+ ": valueData may not be null");
	    values.add(valueData);
	    if (detail) {
		if (detailData == null)
		    throw new IllegalArgumentException(CLASS
			    + ": detailData may not be null");
		details.add(detailData);
	    }
	}
	counter++;
    }

    /**
     * This is the overloaded {@link Object#toString()} method, and it is provided mainly for debugging
     * purposes.
     * <p>
     * @return A string representing the instance
     */

    public String toString() {

	StringBuffer sb = new StringBuffer();

	sb.append("Prefix      : ");
	sb.append(prefix);
	sb.append('\n');
	sb.append("Key         : ");
	sb.append(key);
	sb.append('\n');
	sb.append("Detail      : ");
	sb.append(detail);
	sb.append('\n');
	sb.append("Separator   : ");
	sb.append(separator);
	sb.append('\n');
	sb.append("Value       : ");
	sb.append(value);
	sb.append('\n');
	sb.append("Multiplicity: ");
	sb.append(multiplicity);
	sb.append('\n');
	sb.append("Pattern     : ");
	sb.append(pattern);
	sb.append('\n');

	sb.append("Results     : ");
	sb.append(counter);
	sb.append('\n');
	if (value) {
	    if (detail) {
		for (int i = 0; i < values.size(); i++) {
		    sb.append(details.get(i));
		    sb.append(" / ");
		    sb.append(values.get(i));
		    sb.append('\n');
		}
	    } else {
		for (int i = 0; i < values.size(); i++) {
		    sb.append(values.get(i));
		    sb.append('\n');
		}
	    }
	}

	return sb.toString();

    }

}
