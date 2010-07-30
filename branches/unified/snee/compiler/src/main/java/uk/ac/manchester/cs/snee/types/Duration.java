package uk.ac.manchester.cs.snee.types;

import java.util.concurrent.TimeUnit;

public class Duration {
	
	/**
	 * Store the duration in milliseconds
	 */
	private long _duration;

	/**
	 * Creates a duration with the specified interval given
	 * in milliseconds.
	 * 
	 * @param duration
	 */
	public Duration(long duration) {
		_duration = duration;
	}
	
	/**
	 * Creates a duration of the specified interval with the
	 * given time unit.
	 *  
	 * @param duration
	 * @param unit
	 */
	public Duration(long duration, TimeUnit unit) {
		_duration = TimeUnit.MILLISECONDS.convert(duration, unit);
	}
	
	/**
	 * Returns the duration in the specified time unit.
	 * 
	 * @param unit time unit in which the duration should be returned
	 * @return Duration in the specified time unit
	 */
	public long getDuration(TimeUnit unit) {
		long result = 0;
		switch (unit) {
		case MILLISECONDS: 
			result = _duration;
			break;
		case SECONDS: 
			result = TimeUnit.SECONDS.convert(_duration, TimeUnit.MILLISECONDS);
			break;
		case MINUTES:
			result = TimeUnit.MINUTES.convert(_duration, TimeUnit.MILLISECONDS);
			break;
		case HOURS:
			result = TimeUnit.HOURS.convert(_duration, TimeUnit.MILLISECONDS);
			break;
		case DAYS:
			result = TimeUnit.DAYS.convert(_duration, TimeUnit.MILLISECONDS);
			break;
		}
		return result;
	}
	
	public long getDuration() {
		return _duration;
	}
	
	public String toString() {
		return TimeUnit.SECONDS.convert(_duration, TimeUnit.MILLISECONDS) +
			" seconds";
	}

}
