package com.linkedin.camus.etl;

import java.io.IOException;

import com.linkedin.camus.coders.CamusWrapperBase;
import com.linkedin.camus.coders.KeyedCamusWrapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

/**
 *
 *
 */
public interface RecordWriterProvider {

    String getFilenameExtension();

    RecordWriter<IEtlKey, KeyedCamusWrapper> getDataRecordWriter(
            TaskAttemptContext context, String fileName, CamusWrapperBase data, FileOutputCommitter committer) throws IOException,
            InterruptedException;
}
