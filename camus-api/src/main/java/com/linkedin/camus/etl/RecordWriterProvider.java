package com.linkedin.camus.etl;

import com.linkedin.camus.coders.KeyedCamusWrapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;

/**
 *
 *
 */
public interface RecordWriterProvider {

    String getFilenameExtension();

    RecordWriter<IEtlKey, KeyedCamusWrapper> getDataRecordWriter(
            TaskAttemptContext context, String fileName, KeyedCamusWrapper data, FileOutputCommitter committer) throws IOException,
            InterruptedException;
}
