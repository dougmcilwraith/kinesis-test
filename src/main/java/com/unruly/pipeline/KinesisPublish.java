package com.unruly.pipeline;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * Created by dougmcilwraith on 13/08/2015.
 */
public class KinesisPublish {

    public static void main(String[] args){

        System.out.println("Just testing");
        // KinesisProducer gets credentials automatically like
        // DefaultAWSCredentialsProviderChain.
        // It also gets region automatically from the EC2 metadata service.

        KinesisProducerConfiguration config = new KinesisProducerConfiguration()
                .setRecordMaxBufferedTime(3000)
                .setMaxConnections(1)
                .setRequestTimeout(60000)
                .setRegion("us-east-1");

        KinesisProducer k = new KinesisProducer(config);
        for (int i = 0; i < 100; ++i) {
            try {
                ByteBuffer data = ByteBuffer.wrap("myNewData".getBytes("UTF-8"));
                System.out.println("Sending "+data);
                k.addUserRecord("test-kinesis", "myPartitionKey", data); //non-blocking
            }catch(UnsupportedEncodingException e){
                System.out.println(e);
            }
        }

    }

}
