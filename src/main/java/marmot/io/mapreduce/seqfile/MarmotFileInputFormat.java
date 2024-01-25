package marmot.io.mapreduce.seqfile;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import marmot.io.RecordWritable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotFileInputFormat extends SequenceFileInputFormat<NullWritable, RecordWritable> {
	@Override
	public RecordReader<NullWritable, RecordWritable> createRecordReader(InputSplit split,
																	TaskAttemptContext context)
		throws IOException {
		return new SequenceFileRecordReader<NullWritable, RecordWritable>();
	}
}
