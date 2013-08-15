package com.tgam.hadoop.pig;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * A custom record reader for parsing raw Omniture daily hit data files (hit_data.tsv).
 * @author Duncan M. Gunn (<a href="mailto:gunn.duncan@gmail.com">gunn.duncan@gmail.com</a>) based on original by Mike Sukmanowsky (<a href="mailto:mike.sukmanowsky@gmail.com">mike.sukmanowsky@gmail.com</a>)
 */
public class OmnitureDataFileRecordReader extends RecordReader<LongWritable, Text> {
	
	private static final Log LOG = LogFactory.getLog(OmnitureDataFileRecordReader.class);
	private static final int NUMBER_OF_FIELDS = 254;
	
	private int maxLineLength;
	private long start;
	private long pos;
	private long end;
	private CompressionCodecFactory compressionCodecs = null;
	private EscapedLineReader lineReader;
	private LongWritable key = null;
	private Text value = null; 

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		this.maxLineLength = job.getInt("mapred.escapedlinereader.maxlength", Integer.MAX_VALUE);
		this.start = split.getStart();
		this.end = start + split.getLength();
		final Path file = split.getPath();
		this.compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);
		
		// Open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		boolean skipFirstLine = false;
		if (codec != null) {
			lineReader = new EscapedLineReader(codec.createInputStream(fileIn), job);
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				skipFirstLine = true;
				--start;
				fileIn.seek(start);
			}
			lineReader = new EscapedLineReader(fileIn, job);
		}
		if (skipFirstLine) {
			start += lineReader.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
		}
		this.pos = start;
	}
	
	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return this.key;
	}

	@Override
	/**
	 * Returns the current line.  All instances of \\t, \\n and \\r\n are replaced by a space.
	 * @return the value last read when nextKeyValue() was called.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public Text getCurrentValue() throws IOException, InterruptedException {
		return this.value;
	}

	@Override
	public void close() throws IOException {
		if (lineReader != null) {
			lineReader.close();
		}
		
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
	    if (start == end) {
	    	return 0.0f;
	    } else {
	    	return Math.min(1.0f, (pos - start) / (float)(end - start));
	    }
	}


	@Override
	/**
	 * Reads the next record in the split.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public boolean nextKeyValue() throws IOException, InterruptedException {
		String line;
		String[] fields = new String[1];
		
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		
		if (value == null) {
			value = new Text();
		}
		
		int bytesRead = 0;
		// Stay within the split
		while (pos < end) {			
			bytesRead = lineReader.readLine(value, maxLineLength, Math.max((int)Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));

			// If we didn't read anything, then we're done
			if (bytesRead == 0) {
				break;
			}
			// Modify the line that's returned by the EscapedLineReader so that the tabs won't be an issue to split on
			// Remember that in Java a \\\\ within a string regex = one backslash
			line = value.toString().replaceAll("\\\\\t", " ").replaceAll("\\\\(\n|\r|\r\n)", "");
			fields = line.split("\t", -1);
			value.set(line);
			
			// Move the position marker
			pos += bytesRead;
			
			// Ensure that we didn't read more than we were supposed to and that we don't have a bogus line which should be skipped
			if (bytesRead < maxLineLength && fields.length == NUMBER_OF_FIELDS) {
				break;
			}
			
			if (fields.length != NUMBER_OF_FIELDS) {
				// TODO: Implement counters to track number of skipped lines, possibly only available via map and reduce functions
				LOG.warn("Skipped line at position " + (pos - bytesRead) + " with incorrect number of fields (expected "+ NUMBER_OF_FIELDS + " but found " + fields.length + ")");
			} else {
				// Otherwise the line is too long and we need to skip this line
				LOG.warn("Skipped line of size " + bytesRead + " at position " + (pos - bytesRead));
			}
		}
		
		// Check to see if we actually read a line and return appropriate boolean
		if (bytesRead == 0 || fields.length != NUMBER_OF_FIELDS) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}

}
