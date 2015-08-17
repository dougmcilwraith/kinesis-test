package com.unruly.pipeline;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.*;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.*;
import com.amazonaws.services.kinesis.model.Record;
import com.sun.prism.impl.Disposer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;
import java.util.UUID;

/**
 * Created by dougmcilwraith on 13/08/2015.
 */
public class KinesisSubscribe {

    public void doWork() throws UnknownHostException {
        DefaultAWSCredentialsProviderChain credentialsProvider = new DefaultAWSCredentialsProviderChain();
        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        IRecordProcessor rp = new RecordProcessFactory().createProcessor();
        final KinesisClientLibConfiguration config = new KinesisClientLibConfiguration("test-application-2","test-kinesis",credentialsProvider,workerId);
        final IRecordProcessorFactory recordProcessorFactory = new RecordProcessFactory();
        final Worker worker = new Worker.Builder()
                .recordProcessorFactory(recordProcessorFactory)
                .config(config)
                .build();

        worker.run();
    }

    public class RecordProcessor implements IRecordProcessor{

        private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

        public void initialize(InitializationInput initializationInput) {
            System.out.println("Initializing with shardid: "+initializationInput.getShardId());
            System.out.println(initializationInput.getExtendedSequenceNumber());
        }

        public void processRecords(ProcessRecordsInput processRecordsInput) {
            System.out.println("Processing");

            List<Record> recordList = processRecordsInput.getRecords();

            for (Record r : recordList) {
                try {
                    String data = decoder.decode(r.getData()).toString();
                    System.out.println("Found: " + data);
                    try { // Checkpoint every record
                        processRecordsInput.getCheckpointer().checkpoint(r);
                    } catch (InvalidStateException e) {
                        e.printStackTrace();
                    } catch (ShutdownException e) {
                        e.printStackTrace();
                    }
                } catch (CharacterCodingException e) {
                    System.out.println(e);
                }
            }

        }

        public void shutdown(ShutdownInput shutdownInput) {
            System.out.println("Shutting down!");
        }
    }

    public class RecordProcessFactory implements IRecordProcessorFactory{
        public RecordProcessFactory(){
            super();
        }

        public IRecordProcessor createProcessor(){
            return new RecordProcessor();
        }

    }

    public static void main(String[] args){
        KinesisSubscribe ks = new KinesisSubscribe();
        try {
            ks.doWork();
        }catch(UnknownHostException e){
            System.out.println(e);
        }
    }
}
