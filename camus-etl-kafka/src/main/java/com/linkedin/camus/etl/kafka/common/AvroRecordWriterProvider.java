package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.coders.KeyedCamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AvroRecordWriterProvider implements RecordWriterProvider {
    public final static String EXT = ".avro";

    public AvroRecordWriterProvider(TaskAttemptContext context) {
    }
    
    @Override
    public String getFilenameExtension() {
        return EXT;
    }

    @Override
    public RecordWriter<IEtlKey, KeyedCamusWrapper> getDataRecordWriter(
            TaskAttemptContext context,
            String fileName,
            KeyedCamusWrapper data,
            FileOutputCommitter committer) throws IOException, InterruptedException {
        final DataFileWriter<Object> writer = new DataFileWriter<>(new SpecificDatumWriter<>());

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

        return new RecordWriter<IEtlKey, KeyedCamusWrapper>() {
            @Override
            public void write(IEtlKey ignore, KeyedCamusWrapper data) throws IOException {
                writer.append(data.getRecord());
            }

            @Override
            public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
                writer.close();
            }
        };
    }
}
