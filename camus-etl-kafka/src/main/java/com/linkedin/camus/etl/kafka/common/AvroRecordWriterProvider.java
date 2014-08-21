package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.coders.CamusWrapperLight;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import java.io.IOException;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 *
 */
public class AvroRecordWriterProvider implements RecordWriterProvider {
    public final static String EXT = ".avro";

    public AvroRecordWriterProvider(TaskAttemptContext context) {
    }
    
    @Override
    public String getFilenameExtension() {
        return EXT;
    }

    @Override
    public RecordWriter<IEtlKey, CamusWrapperLight> getDataRecordWriter(
            TaskAttemptContext context,
            String fileName,
            CamusWrapperLight data,
            FileOutputCommitter committer) throws IOException, InterruptedException {
        final DataFileWriter<Object> writer = new DataFileWriter<Object>(
                new SpecificDatumWriter<Object>());

        if (FileOutputFormat.getCompressOutput(context)) {
            if ("snappy".equals(EtlMultiOutputFormat.getEtlOutputCodec(context))) {
                writer.setCodec(CodecFactory.snappyCodec());
            } else {
                int level = EtlMultiOutputFormat.getEtlDeflateLevel(context);
                writer.setCodec(CodecFactory.deflateCodec(level));
            }
        }

        Path path = committer.getWorkPath();
        path = new Path(path, EtlMultiOutputFormat.getUniqueFile(context, fileName, EXT));
        writer.create(((GenericRecord) data.getRecord()).getSchema(),
                path.getFileSystem(context.getConfiguration()).create(path));

        writer.setSyncInterval(EtlMultiOutputFormat.getEtlAvroWriterSyncInterval(context));

        return new RecordWriter<IEtlKey, CamusWrapperLight>() {
            @Override
            public void write(IEtlKey ignore, CamusWrapperLight data) throws IOException {
                writer.append(data.getRecord());
            }

            @Override
            public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
                writer.close();
            }
        };
    }
}
