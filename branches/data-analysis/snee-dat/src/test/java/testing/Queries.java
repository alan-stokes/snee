package testing;

public class Queries {

	public static final String q1 = "SELECT * FROM TestStream t, UpStream u;";
	public static final String q2 = "(SELECT * FROM TestStream);";
	public static final String q3 = "SELECT Timestamp FROM TestStream WHERE Timestamp < 42;";
	public static final String q4 = "SELECT * FROM TestStream[FROM NOW-10 ROWS TO NOW - 0 SLIDE 5 ROWS];";
	public static final String q5 = "(SELECT timestamp FROM TestStream) UNION (SELECT timestamp FROM PullStream);";
	public static final String q6 = "SELECT timestamp FROM TestStream ts, (SELECT * FROM UpStream) us WHERE ts.timestamp = us.timestamp;";
	public static final String q7 = "SELECT timestamp FROM TestStream ts, (SELECT * FROM UpStream us WHERE us.timestamp > 10) us;";
	public static final String q8 = "SELECT Timestamp FROM TestStream WHERE (Timestamp < 42);";

	public static final String q9 = "SELECT RSTREAM F.temperature, RF.pressure FROM forestTemp[NOW] F, LRF RF WHERE F.temperature=RF.temperature;";
	public static final String q10 = "SELECT * FROM " +
			"(SELECT RSTREAM F.temperature, RF.pressure FROM forestTemp[NOW] F, LRF RF WHERE F.temperature=RF.temperature) tmp " +
			"WHERE tmp.temperature > 50;";

	public static final String q11 = "SELECT * FROM TestStream WHERE (temperature > 15)AND (pressure < 20);";
	public static final String q12 = "SELECT * FROM TestStream WHERE (candor = foonot) AND (pressure < 20);";


	public static final String q13 = 
		"SELECT RSTREAM f.temperature FROM flood[NOW] f, d3 od WHERE f.temperature=od.temperature AND f.pressure=od.pressure;";
}
