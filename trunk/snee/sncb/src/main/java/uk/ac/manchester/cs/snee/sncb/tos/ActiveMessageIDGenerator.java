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
package uk.ac.manchester.cs.snee.sncb.tos;

import java.util.HashMap;
import java.util.Iterator;

public final class ActiveMessageIDGenerator {

    private static final HashMap<String, String> activeMessageIDMap = new HashMap<String, String>();

    private static String generateActiveMessageIDKey(final String sourceFragID,
	    final String destFragID, final String destNodeID,
	    final String sendingNodeID) {
	final String key = "AM_Frag" + sourceFragID + "_Frag" + destFragID
		+ "Site" + destNodeID + "_txSite" + sendingNodeID;
	return key;
    }

    /**
     * This method generates a unique Active message ID for each message type between a two sites in the
     * sensor network.  It's a bit of a hack, but is successful in ensuring the the right messages
     * get sent to the right sites.
     * 
     * @param sourceFragID
     * @param destFragID
     * @param destNodeID
     * @param sendingNodeID
     * @return
     */
    public static String getActiveMessageIDKey(final String sourceFragID,
	    final String destFragID, final String destNodeID,
	    final String sendingNodeID) {
	final String key = generateActiveMessageIDKey(sourceFragID, destFragID,
		destNodeID, sendingNodeID);
	if (activeMessageIDMap.containsKey(key)) {
	    return key;
	} else {
	    if (activeMessageIDMap.size() == 256) { //TODO: unhardcode this
		//	logger.severe("Out of Active Mesage IDs");
		System.exit(0); //TODO: throw nesCGenerationException
	    }

	    final String nextActiveMessageID = new Integer(activeMessageIDMap
		    .size()+20).toString();
	    activeMessageIDMap.put(key, nextActiveMessageID);
	    return key;
	}
    }

    public static String getActiveMessageID(final String key) {
    	if (activeMessageIDMap.containsKey(key)) {
    	    return activeMessageIDMap.get(key);
    	} else {
    	    if (activeMessageIDMap.size() == 256) { //TODO: unhardcode this
    		//	logger.severe("Out of Active Mesage IDs");
    		System.exit(0); //TODO: throw nesCGenerationException
    	    }

    	    final String nextActiveMessageID = new Integer(activeMessageIDMap
    		    .size()+20).toString();
    	    activeMessageIDMap.put(key, nextActiveMessageID);
    	    return nextActiveMessageID;
    	}   	
    }
    
    public static String getActiveMessageID(final String sourceFragID,
	    final String destFragID, final String destNodeID,
	    final String sendingNodeID) {
	final String key = generateActiveMessageIDKey(sourceFragID, destFragID,
		destNodeID, sendingNodeID);
		return getActiveMessageID(key);
    }

    public static Iterator<String> activeMessageIDKeyIterator() {
	final Iterator<String> amIter = activeMessageIDMap.keySet().iterator();
	return amIter;
    }

    public static String getactiveMessageID(final String Id) {
	return activeMessageIDMap.get(Id);
    }

    public static boolean isEmpty() {
	return activeMessageIDMap.size() == 0;
    }
}
