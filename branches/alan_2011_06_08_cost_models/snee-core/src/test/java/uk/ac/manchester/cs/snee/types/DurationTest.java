package uk.ac.manchester.cs.snee.types;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DurationTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testGetDuration_milliseconds2milliseconds() {
		Duration duration = new Duration(42, TimeUnit.MILLISECONDS);
		assertEquals(42, duration.getDuration());
	}
	
	@Test
	public void testGetDuration_seconds2milliseconds() {
		Duration duration = new Duration(42, TimeUnit.SECONDS);
		assertEquals(42000, duration.getDuration());
	}
	
	@Test
	public void testGetDuration_minutes2milliseconds() {
		Duration duration = new Duration(42, TimeUnit.MINUTES);
		assertEquals(2520000, duration.getDuration());
	}
	
	@Test
	public void testGetDuration_hours2milliseconds() {
		Duration duration = new Duration(42, TimeUnit.HOURS);
		assertEquals(151200000, duration.getDuration());
	}
	
	@Test
	public void testGetDuration_days2milliseconds() {
		Duration duration = new Duration(24, TimeUnit.DAYS);
		assertEquals(2073600000, duration.getDuration());
	}

	@Test
	public void testGetDurationTimeUnit_seconds2seconds() {
		Duration duration = new Duration(42, TimeUnit.SECONDS);
		assertEquals(42, duration.getDuration(TimeUnit.SECONDS));
	}

	@Test
	public void testGetDurationTimeUnit_minutes2seconds() {
		Duration duration = new Duration(42, TimeUnit.MINUTES);
		assertEquals(2520, duration.getDuration(TimeUnit.SECONDS));
	}

	@Test
	public void testGetDurationTimeUnit_hours2minutes() {
		Duration duration = new Duration(84, TimeUnit.HOURS);
		assertEquals(5040, duration.getDuration(TimeUnit.MINUTES));
	}

	@Test
	public void testGetDurationTimeUnit_days2hourss() {
		Duration duration = new Duration(99, TimeUnit.DAYS);
		assertEquals(2376, duration.getDuration(TimeUnit.HOURS));
	}
	
	@Test
	public void testGetDurationTimeUnit_poorGranularity() {
		Duration duration = new Duration(42);
		assertEquals(0, duration.getDuration(TimeUnit.SECONDS));
	}

}
