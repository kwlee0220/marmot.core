package marmot.io;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

import marmot.Record;
import marmot.RecordSchema;
import marmot.rset.AbstractRecordSet;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SequenceFileRecordSet extends AbstractRecordSet {
	private static final NullWritable NULL = NullWritable.get();
	
	private final MarmotSequenceFile m_file;
	private final RecordSchema m_schema;
	private final SequenceFile.Reader m_reader;
	private final RecordWritable m_value;
	private long m_endOffset = -1;
	
	SequenceFileRecordSet(MarmotSequenceFile file) {
		m_file = file;
		
		try {
			m_schema = file.getFileInfo().getRecordSchema();
			m_value = RecordWritable.from(m_schema);
			m_reader = file.readSequenceFile();
			m_endOffset = -1;
		}
		catch ( IOException e ) {
			throw new MarmotFileException(e);
		}
	}
	
	SequenceFileRecordSet(MarmotSequenceFile file, long start, long length) {
		m_file = file;
		
		try {
			m_schema = file.getFileInfo().getRecordSchema();
			m_value = RecordWritable.from(m_schema);
			
			m_reader = file.readSequenceFile();
			if ( start > 0 ) {
				m_reader.seek(start);
			}
			m_endOffset = (length >= 0)
						? (start>=0 ? start : 0) + length
						: -1;
		}
		catch ( IOException e ) {
			throw new MarmotFileException(e);
		}
	}

	@Override
	protected void closeInGuard() throws Exception {
		m_reader.close();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public boolean next(Record record) {
		try {
			if ( m_endOffset >= 0 && m_reader.getPosition() >= m_endOffset ) {
				return false;
			}
			
			if ( m_reader.next(NULL, m_value) ) {
				record.setAll(m_value.get());
				return true;
			}
			else {
				return false;
			}
		}
		catch ( IOException e ) {
			throw new MarmotFileException("fails to read Record from MarmotSequenceFile: " + m_file
											+ ", cause=" + e);
		}
	}
	
	public double getProgress() {
		try {
			return (double)m_reader.getPosition() / m_file.getLength();
		}
		catch ( IOException e ) {
			throw new MarmotFileException(e);
		}
	}
	
	public long getFileLength() {
		return m_file.getLength();
	}
	
	public long getPosition() {
		try {
			return m_reader.getPosition();
		}
		catch ( IOException e ) {
			throw new MarmotFileException("fails to get the current position: path=" + m_file);
		}
	}
	
	public void setEndPosition(long offset) {
		m_endOffset = offset;
	}
	
	public void seek(long position) {
		try {
			m_reader.seek(position);
		}
		catch ( IOException e ) {
			throw new MarmotFileException("fails to seek to the position: pos=" + position
										+ ", path=" + m_file);
		}
	}
}
