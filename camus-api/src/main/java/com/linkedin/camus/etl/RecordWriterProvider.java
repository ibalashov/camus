package com.linkedin.camus.etl;

import java.io.IOException;

import com.linkedin.camus.coders.CamusWrapperLight;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

/**
 *
 *
 */
public interface RecordWriterProvider {

    String getFilenameExtension();

    RecordWriter<IEtlKey, CamusWrapperLight> getDataRecordWriter(
            TaskAttemptContext context, String fileName, CamusWrapperLight data, FileOutputCommitter committer) throws IOException,
            InterruptedException;
}
