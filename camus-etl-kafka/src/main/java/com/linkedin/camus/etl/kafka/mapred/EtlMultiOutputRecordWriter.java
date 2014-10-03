package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.coders.KeyedCamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.ExceptionWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;

public class EtlMultiOutputRecordWriter extends RecordWriter<EtlKey, Object> {
    private static Logger log = Logger.getLogger(EtlMultiOutputRecordWriter.class);
    private TaskAttemptContext context;
    private Writer errorWriter = null;
    private String currentTopic = "";
    private long beginTimeStamp = 0;
    private HashMap<String, RecordWriter<IEtlKey, KeyedCamusWrapper>> dataWriters = new HashMap<>();

    private EtlMultiOutputCommitter committer;

    public EtlMultiOutputRecordWriter(TaskAttemptContext context, EtlMultiOutputCommitter committer) throws IOException,
            InterruptedException {
        this.context = context;
        this.committer = committer;
        errorWriter =
                SequenceFile.createWriter(FileSystem.get(context.getConfiguration()),
                        context.getConfiguration(),
                        new Path(committer.getWorkPath(),
                                EtlMultiOutputFormat.getUniqueFile(context,
                                        EtlMultiOutputFormat.ERRORS_PREFIX,
                                        "")),
                        EtlKey.class,
                        ExceptionWritable.class);

        if (EtlInputFormat.getKafkaMaxHistoricalDays(context) != -1) {
            int maxDays = EtlInputFormat.getKafkaMaxHistoricalDays(context);
            beginTimeStamp = (new DateTime()).minusDays(maxDays).getMillis();
        } else {
            beginTimeStamp = 0;
        }
        log.info("beginTimeStamp set to: " + beginTimeStamp);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        for (String w : dataWriters.keySet()) {
            dataWriters.get(w).close(context);
        }
        errorWriter.close();
    }

    @Override
    public void write(EtlKey key, Object val) throws IOException,
            InterruptedException {
        if (val instanceof KeyedCamusWrapper) {
            if (key.getTime() < beginTimeStamp) {
                // ((Mapper.Context)context).getCounter("total",
                // "skip-old").increment(1);
                committer.addOffset(key);
            } else {
                if (!key.getTopic().equals(currentTopic)) {
                    for (RecordWriter<IEtlKey, KeyedCamusWrapper> writer : dataWriters.values()) {
                        writer.close(context);
                    }
                    dataWriters.clear();
                    currentTopic = key.getTopic();
                }

                KeyedCamusWrapper value = (KeyedCamusWrapper) val;
                String filename = value.getKey().toString().replaceAll("[^a-zA-Z0-9\\._]+", "_");
                committer.addCounts(key, filename);
                String workingFileName = EtlMultiOutputFormat.getWorkingFileName(context, key, filename);

                if (!dataWriters.containsKey(workingFileName)) {
                    RecordWriter<IEtlKey, KeyedCamusWrapper> dataRecordWriter = getDataRecordWriter(context, workingFileName, value);

                    dataWriters.put(workingFileName, dataRecordWriter);
                }
                dataWriters.get(workingFileName).write(key, value);
            }
        }
//    else if (val instanceof CamusWrapperBase<?>)
//    {
//      if (key.getTime() < beginTimeStamp)
//      {
//        // ((Mapper.Context)context).getCounter("total",
//        // "skip-old").increment(1);
//        committer.addOffset(key);
//      }
//      else
//      {
//        if (!key.getTopic().equals(currentTopic))
//        {
//          for (RecordWriter<IEtlKey, CamusWrapperBase> writer : dataWriters.values())
//          {
//            writer.close(context);
//          }
//          dataWriters.clear();
//          currentTopic = key.getTopic();
//        }
//
//        committer.addCounts(key);
//        CamusWrapperBase value = (CamusWrapperBase) val;
//        String workingFileName = EtlMultiOutputFormat.getWorkingFileName(context, key);
//        if (!dataWriters.containsKey(workingFileName))
//        {
//          dataWriters.put(workingFileName, getDataRecordWriter(context, workingFileName, value));
//        }
//        dataWriters.get(workingFileName).write(key, value);
//      }
//    }
        else if (val instanceof ExceptionWritable) {
            committer.addOffset(key);
            System.err.println(key.toString());
            System.err.println(val.toString());
            errorWriter.append(key, (ExceptionWritable) val);
        }
    }

    private RecordWriter<IEtlKey, KeyedCamusWrapper> getDataRecordWriter(TaskAttemptContext context,
                                                                         String fileName,
                                                                         KeyedCamusWrapper value) throws IOException, InterruptedException {
        {
            RecordWriterProvider recordWriterProvider = null;
            try {
                //recordWriterProvider = EtlMultiOutputFormat.getRecordWriterProviderClass(context).newInstance();
                Class<RecordWriterProvider> rwp = EtlMultiOutputFormat.getRecordWriterProviderClass(context);
                Constructor<RecordWriterProvider> crwp = rwp.getConstructor(TaskAttemptContext.class);
                recordWriterProvider = crwp.newInstance(context);
            } catch (InstantiationException e) {
                throw new IllegalStateException(e);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            return recordWriterProvider.getDataRecordWriter(context, fileName, value, committer );
        }
    }
}
