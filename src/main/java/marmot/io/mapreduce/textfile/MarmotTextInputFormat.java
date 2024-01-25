package marmot.io.mapreduce.textfile;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import marmot.RecordSchema;
import marmot.io.RecordWritable;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotTextInputFormat extends InputFormat<NullWritable,RecordWritable> {
	private static final String PROP_COMMENT_MARKER = "marmot.input.text.comment";
	private final TextInputFormat m_format = new TextInputFormat();
	
	public static String getCommentMarker(Configuration conf) {
		return conf.get(PROP_COMMENT_MARKER);
	}
	
	public static void setCommentMarker(Configuration conf, String marker) {
		conf.set(PROP_COMMENT_MARKER, marker);
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		return m_format.getSplits(context);
	}

	@Override
	public RecordReader<NullWritable, RecordWritable> createRecordReader(InputSplit split,
							TaskAttemptContext context) throws IOException, InterruptedException {
		return new TextReader(m_format.createRecordReader(split, context));
	}

	private static class TextReader extends RecordReader<NullWritable, RecordWritable> {
		public static final RecordSchema SCHEMA = RecordSchema.builder()
//															.addColumn("key", DataType.LONG)
															.addColumn("text", DataType.STRING)
															.build();
		
		private final RecordReader<LongWritable, Text> m_base;
		private String m_commentMarker = null;
		private RecordWritable m_next;
		
		TextReader(RecordReader<LongWritable, Text> base) {
			m_base = base;
		}
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
			m_base.initialize(split, context);
			
			Configuration conf = context.getConfiguration();
			m_commentMarker = MarmotTextInputFormat.getCommentMarker(conf);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return (m_next = getNextRecord()) != null;
		}

		@Override
		public NullWritable getCurrentKey() throws IOException, InterruptedException {
			return m_next != null ? NullWritable.get() : null;
		}

		@Override
		public RecordWritable getCurrentValue() throws IOException, InterruptedException {
			return m_next;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return m_base.getProgress();
		}

		@Override
		public void close() throws IOException {
			m_base.close();
		}
		
		private RecordWritable getNextRecord() throws IOException, InterruptedException {
			while ( m_base.nextKeyValue() ) {
				String line = m_base.getCurrentValue().toString();

				if ( m_commentMarker == null || line.startsWith(m_commentMarker) ) {
					return RecordWritable.from(SCHEMA, new Object[]{line});
				}
			}
			
			return null;
		}
	}
}
