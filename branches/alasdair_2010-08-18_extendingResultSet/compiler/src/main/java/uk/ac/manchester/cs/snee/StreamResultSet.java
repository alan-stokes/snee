package uk.ac.manchester.cs.snee;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.evaluator.types.Field;
import uk.ac.manchester.cs.snee.evaluator.types.TaggedTuple;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;

public class StreamResultSet 
implements ResultSet {

	private Logger logger = 
		Logger.getLogger(this.getClass().getName());
	
	private int cursorPosition = 0;
	int numRows = 0;
	private Object[][] data;
	private ResultSetMetaData metadata;
	
	protected StreamResultSet(ResultSetMetaData metadata, 
			List<TaggedTuple> results)
	throws SQLException, SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER StreamResultSet() #results=" +
					results.size());
		}
		this.metadata = metadata;
		populateData(results);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN StreamResultSet()");
		}
	}

	private void populateData(List<TaggedTuple> results) 
	throws SQLException, SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER populateData()");
		}
		int numColumns = metadata.getColumnCount();
		//XXX: Don't store anything in 0,0
		data = new Object[results.size() + 1][numColumns + 1];
		for (TaggedTuple tt : results) {
			numRows++;
			Tuple tuple = tt.getTuple();
			for (int i = 1; i <= numColumns; i++) {
				String colName = metadata.getColumnLabel(i);
				Field field = tuple.getField(colName);
				data[numRows][i] = field.getData();
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN populateData() with\n" + printData());
		}
	}

	private String printData() {
		StringBuffer output = new StringBuffer("Data:");
		for (int i = 1; i < data.length; i++) {
			output.append("\n  Row " + i +":\t");
			Object[] row = data[i];
			for (int j = 1; j < row.length; j++) {
				output.append(row[j]).append("\t");
			}
		}
		return output.toString();
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER absolute() with " + row);
		}
		boolean response;
		if (row >= 1 && row <= numRows) {
			cursorPosition = row;
			response = true;
		} else if (row <= -1 && row >= (0 - numRows)) {
			cursorPosition = numRows + 1 + row;
			response = true;
		} else {
			if (row == 0) {
				cursorPosition = 0;
			} else if (row > numRows) {
				cursorPosition = numRows + 1;
			}
			response = false;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN absolute() with " + response +
					" cursorPostion=" + cursorPosition);
		}
		return response;
	}

	@Override
	public void afterLast() throws SQLException {
		if (numRows != 0) {
			cursorPosition = numRows + 1;
		}
	}

	@Override
	public void beforeFirst() throws SQLException {
		if (numRows != 0) {
			cursorPosition = 0;
		}
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		//XXX: Method not supported. Has no effect.
	}

	@Override
	public void clearWarnings() throws SQLException {
		//XXX: Method not supported. Has no effect.
	}

	@Override
	public void close() throws SQLException {
		//XXX: Method not supported. Has no effect.
	}

	@Override
	public void deleteRow() throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER findColumn() with " + columnLabel);
		}
		int numCols = metadata.getColumnCount();
		int columnNumber = 0;
		for (int i = 1; i <= numCols; i++) {
			String label = metadata.getColumnLabel(i);
			if (label.equalsIgnoreCase(columnLabel)) {
				columnNumber = i;
				break;
			}
		}
		if (columnNumber == 0) {
			String message = "Column " + columnLabel +
				" does not exist.";
			logger.warn(message);
			throw new SQLException(message);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN findColumn() with " + columnNumber);
		}
		return columnNumber;
	}

	@Override
	public boolean first() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER first()");
		}
		boolean response;
		if (numRows > 0) {
			cursorPosition = 1;
			response = true;
		} else {
			response = false;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN first() with " + response);
		}
		return response;
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		throw new SQLException("SQLType not supported.");
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		throw new SQLException("SQLType not supported.");
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getBigDecimal() with " + columnIndex);
		}
		int colType = metadata.getColumnType(columnIndex);
		if (colType != Types.DECIMAL) {
			String message = "Column type is not a decimal.";
			logger.warn(message);
			throw new SQLException(message);
		}
		BigDecimal response = 
			(BigDecimal) data[cursorPosition][columnIndex];
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN getBigDecimal() with " + response);
		}
		return response;
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getBigDecimal() " + columnLabel);
		}
		int columnIndex = findColumn(columnLabel);
		BigDecimal response = getBigDecimal(columnIndex);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN getBigDecimal() with " + response);
		}
		return response;
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getBoolean() with " + columnIndex);
		}
		int colType = metadata.getColumnType(columnIndex);
		if (colType != Types.BOOLEAN) {
			String message = "Column type is not a boolean.";
			logger.warn(message);
			throw new SQLException(message);
		}
		boolean response = (Boolean) data[cursorPosition][columnIndex];
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN getBoolean() with " + response);
		}
		return response;
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getBoolean() " + columnLabel);
		}
		int columnIndex = findColumn(columnLabel);
		boolean response = getBoolean(columnIndex);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN getBoolean() with " + response);
		}
		return response;
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public int getConcurrency() throws SQLException {
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public String getCursorName() throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return ResultSet.FETCH_UNKNOWN;
	}

	@Override
	public int getFetchSize() throws SQLException {
		return 0;
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getFloat() with " + columnIndex);
		}
		int colType = metadata.getColumnType(columnIndex);
		if (colType != Types.FLOAT) {
			String message = "Column type is not a float.";
			logger.warn(message);
			throw new SQLException(message);
		}
		float response = (Float) data[cursorPosition][columnIndex];
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN getFloat() with " + response);
		}
		return response;
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getFloat() " + columnLabel);
		}
		int columnIndex = findColumn(columnLabel);
		float response = getFloat(columnIndex);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN getFloat() with " + response);
		}
		return response;
	}

	@Override
	public int getHoldability() throws SQLException {
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getInt() with " + columnIndex);
		}
		int colType = metadata.getColumnType(columnIndex);
		if (colType != Types.INTEGER) {
			String message = "Column type is not an integer.";
			logger.warn(message);
			throw new SQLException(message);
		}
		int response = (Integer) data[cursorPosition][columnIndex];
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN getInt() with " + response);
		}
		return response;
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getInt() " + columnLabel);
		}
		int columnIndex = findColumn(columnLabel);
		int response = getInt(columnIndex);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN getInt() with " + response);
		}
		return response;
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return metadata;
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public int getRow() throws SQLException {
		return cursorPosition;
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Statement getStatement() throws SQLException {
		return null;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getString() with " + columnIndex);
		}
		int colType = metadata.getColumnType(columnIndex);
		if (colType != Types.VARCHAR) {
			String message = "Column type is not a string.";
			logger.warn(message);
			throw new SQLException(message);
		}
		String response = (String) data[cursorPosition][columnIndex];
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN getString() with " + response);
		}
		return response;
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getString() " + columnLabel);
		}
		int columnIndex = findColumn(columnLabel);
		String response = getString(columnIndex);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN getString() with " + response);
		}
		return response;
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getTimestamp() with " + columnIndex);
		}
		int colType = metadata.getColumnType(columnIndex);
		if (colType != Types.TIMESTAMP) {
			String message = "Column type is not an timestamp.";
			logger.warn(message);
			throw new SQLException(message);
		}
		//XXX: Value not always returned as a timestamp!
		Object value = data[cursorPosition][columnIndex];
		Timestamp response;
		if (value instanceof Timestamp) {			
			response = (Timestamp) value;
		} else {
			long t = (Long) value;
			response = new Timestamp(t);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN getTimestamp() with " + response);
		}
		return response;
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getTimestamp() " + columnLabel);
		}
		int columnIndex = findColumn(columnLabel);
		Timestamp response = getTimestamp(columnIndex);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN getTimestamp() with " + response);
		}
		return response;
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public int getType() throws SQLException {
		return ResultSet.TYPE_SCROLL_INSENSITIVE;
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	public void insertRow() throws SQLException {
		throw new SQLException("Result set is read only.");
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER isAfterLast()");
		}
		boolean response;
		if (cursorPosition > numRows) {
			response = true;
		} else {
			response = false;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN isAfterLast() with " + response);
		}
		return response;
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER isBeforeFirst()");
		}
		boolean response;
		if (cursorPosition == 0 && numRows > 0) {
			response = true;
		} else {
			response = false;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN isBeforeFirst() with " + response);
		}
		return response;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return false;
	}

	@Override
	public boolean isFirst() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER isFirst()");
		}
		boolean response;
		if (cursorPosition == 1) {
			response = true;
		} else {
			response = false;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN isFirst() with " + response);
		}
		return response;
	}

	@Override
	public boolean isLast() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER isLast()");
		}
		boolean response;
		if (cursorPosition == numRows && numRows > 0) {
			response = true;
		} else {
			response = false;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN isLast() with " + response);
		}
		return response;
	}

	@Override
	public boolean last() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER last()");
		}
		boolean response;
		if (numRows == 0) {
			response = false;
		} else {
			cursorPosition = numRows;
			response = true;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN last() with " + response);
		}
		return response;
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		throw new SQLException("Result set is read only.");
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throw new SQLException("Result set is read only.");
	}

	@Override
	public boolean next() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER next()");
		}
		boolean response = relative(1);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN next() with " + response);
		}
		return response;
	}

	@Override
	public boolean previous() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER previous()");
		}
		boolean response = relative(-1);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN previous() with " + response);
		}
		return response;
	}

	@Override
	public void refreshRow() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER relative() with " + rows);
		}
		boolean response;
		if (cursorPosition + rows > numRows) {
			cursorPosition = numRows + 1;
			response = false;
		} else if (cursorPosition + rows < 1) {
			cursorPosition = 0;
			response = false;
		} else {
			cursorPosition += rows;
			response = true;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN relative() with " + response);
		}
		return response;
	}

	@Override
	public boolean rowDeleted() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean rowInserted() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean rowUpdated() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		if (rows < 0) {
			throw new SQLException("Rows must be >= 0");
		}
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x,
			long length) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream,
			long length) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			int length) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateClob(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateNString(int columnIndex, String nString)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateNString(String columnLabel, String nString)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateRow() throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x)
			throws SQLException {
		throw new SQLException("Operation not supported.");
	}

	@Override
	public boolean wasNull() throws SQLException {
		return false;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		boolean response;
		if (iface == ResultSet.class) {
			response = true;
		} else {
			response = false;
		}
		return response;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		if(isWrapperFor(iface)) {
			return (T) this;
		} else {
			throw new SQLException();
		}
	}

}
