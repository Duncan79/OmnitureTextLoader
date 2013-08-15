package com.tgam.hadoop.pig;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * A custom input format for dealing with Omniture's hit_data.tsv daily data feed files.
 * @author Duncan M. Gunn (<a href="mailto:gunn.duncan@gmail.com">gunn.duncan@gmail.com</a>) based on original by Mike Sukmanowsky (<a href="mailto:mike.sukmanowsky@gmail.com">mike.sukmanowsky@gmail.com</a>)
 */
public class OmnitureDataFileInputFormat extends TextInputFormat {
	
	
	public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext context)
	{
		return new OmnitureDataFileRecordReader();
	}

}