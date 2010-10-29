package uk.ac.manchester.cs.snee.compiler.metadata.source;

import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEProperties;

public class SourceMetadataUtils {

	/**
	 * Logger for this class.
	 */
	private static Logger logger = 
		Logger.getLogger(SNEEProperties.class.getName());
	
	
	private static int countTokens(StringTokenizer tokens)
	throws SourceMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER countTokens() " + tokens);
		int count = 0;
		String temp;
		while (tokens.hasMoreTokens()) {
			temp = tokens.nextToken();
			if (temp.indexOf('-') == -1) {
				count++;
			} else {
				int start = Integer.parseInt(temp.substring(0, temp
						.indexOf('-')));
				int end = Integer.parseInt(temp.substring(temp
						.indexOf('-') + 1, temp.length()));
				//logger.trace(temp+" "+start+"-"+end);
				if (end < start) {
					String message = "Start less than end";
					logger.warn(message);
					throw new SourceMetadataException(message);
				}
				count = count + end - start + 1;
			}
		}
		if (count == 0) {
			String message = "No nodes defined";
			logger.warn(message);
			throw new SourceMetadataException(message);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN countTokens() number of tokens " + count);
		return count;
	}

	//TODO: Change metadata definition to take a xs:list rather than comma separated string
	public static int[] convertNodes(String text) 
	throws SourceMetadataException {
		if (logger.isDebugEnabled())
			logger.trace("ENTER convertNodes " + text);
		String temp;
		StringTokenizer tokens = new StringTokenizer(text, ",");
		int count = countTokens(tokens);
		//logger.trace("count = "+count);
		
		int[] list = new int[count];
		count = 0;
		tokens = new StringTokenizer(text, ",");
		while (tokens.hasMoreTokens()) {
			temp = tokens.nextToken();
			if (temp.indexOf('-') == -1) {
				list[count] = Integer.parseInt(temp);
				count++;
			} else {
				int start = Integer.parseInt(temp.substring(0, temp
						.indexOf('-')));
				int end = Integer.parseInt(temp.substring(temp
						.indexOf('-') + 1, temp.length()));
				for (int i = start; i <= end; i++) {
					list[count] = i;
					count++;
				}
			}
		}
		Arrays.sort(list);
		count = 0;
		for (int i = 0; i < list.length - 2; i++) {
			if (list[i] >= list[i + 1]) {
				list[i] = Integer.MAX_VALUE;
				count++;
			}
		}
		if (count > 0) {
			Arrays.sort(list);
			int[] newList = new int[list.length - count];
			for (int i = 0; i < newList.length; i++) {
				newList[i] = list[i];
			}
			list = newList;
		}
		String t = "";
		for (int element : list) {
			t = t + "," + element;
		}
		if (logger.isDebugEnabled()) {
			logger.trace("nodes = " + t);
			logger.trace("RETURN convertNodes()");
		}
		return list;
	}

	
}
