package com.bigstep;

import com.mongodb.reactivestreams.client.*;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.reactivestreams.Subscription;

import java.io.Serializable;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.regex;


public class MongoSampler extends AbstractJavaSamplerClient implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger logger = Logger.getLogger(MongoSampler.class);

    private MongoClient client=null;
    private MongoDatabase db=null;
    private MongoCollection<Document> collection=null;

    private long timeout=10000;

    // set up default arguments for the JMeter GUI
    @Override
    public Arguments getDefaultParameters() {
        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument("testType", "read");

        defaultParameters.addArgument("key", "");
        defaultParameters.addArgument("value", "");

        defaultParameters.addArgument("connectionString", "mongodb://localhost");
        defaultParameters.addArgument("database", "mydb");
        defaultParameters.addArgument("collection", "test");
        defaultParameters.addArgument("batchSize", "100");

        return defaultParameters;
    }

    @Override
    public void setupTest(JavaSamplerContext context)
    {
        try
        {
            String connString = context.getParameter("connectionString");
            String dbName = context.getParameter("database");
            String collectionName = context.getParameter( "collection" );

            client =  MongoClients.create(connString);

            db = client.getDatabase(dbName);

            collection = db.getCollection( collectionName );
            if(collection==null)
                throw new Exception("Collection not initialized!");

            logger.info("database connection "+dbName+"initialized");


        }
        catch(Exception ex)
        {
            java.io.StringWriter stringWriter = new java.io.StringWriter();
            ex.printStackTrace( new java.io.PrintWriter( stringWriter ) );

            logger.error("setupTest:"+ex.getMessage()+stringWriter.toString());
        }
    }

    @Override
    public void teardownTest(JavaSamplerContext context)
    {
        if(null!=client)
            client.close();
    }

    private byte [] md5(final String pValue) throws Exception
    { return MessageDigest.getInstance("MD5").digest(pValue.getBytes("UTF-8")); }


    public SampleResult runTest(JavaSamplerContext context) {

        String key = context.getParameter( "key" );
        String value = context.getParameter( "value" );
        String testType = context.getParameter( "testType" );

        SampleResult result = new SampleResult();

        boolean successful = true;
        String message = "OK";

        try {
            if(collection==null)
                throw new Exception("Collection not initialized!");


            if(null==client)
                throw new Exception("Mongo Client not initialised");

            if(testType.equals("read"))
            {
                result.sampleStart(); // start stopwatch

                ObservableSubscriber<Document> subscriber = new ObservableSubscriber<Document>();
                collection.find(eq("_id", key)).first().subscribe(subscriber);

                subscriber.await(timeout, TimeUnit.MILLISECONDS);

                result.sampleEnd(); // stop stopwatch

                List<Document> docs=subscriber.getReceived();

                 if(docs.isEmpty()) {
                     successful=false;
                     message = "no value was returned to read request";
                 }
                 else
                 if(!value.equals("")) {

                    String retrivedVal = docs.get(0).getString("value");

                    if (!value.equals(retrivedVal)) {
                        successful = false;
                        message = "Value verification failed. Expected:" + value + " got instead:" + retrivedVal;
                    }
                }

            }
            else
            if(testType.equals("write"))
            {
                if(value.equals(""))
                {
                    logger.error("Null value insert not implemented at test type write");
                    throw new Exception("please configure a value for insert.");
                }
                else
                {
                  Document doc = new Document("_id", key).append("value",value);

                  OperationSubscriber<Success> subscriber = new OperationSubscriber<Success>();

                  result.sampleStart(); // start stopwatch

                  collection.insertOne(doc).subscribe(subscriber);

                  subscriber.await(timeout, TimeUnit.MILLISECONDS);

                  result.sampleEnd(); // stop stopwatch

                  if(!subscriber.isCompleted()) {
                       successful =false;
                       message=  "subscriber request was not completed. Timeout?";
                  }

                }
            }
            else
            if(testType.equals("writeMany"))
            {
                if(value.equals(""))
                {
                    logger.error("Null value insert not implemented at test type write");
                    throw new Exception("please configure a value for insert.");
                }
                else
                {
                    Long batchSize = context.getLongParameter( "batchSize" );

                    List<Document> documents = new ArrayList<>();
                    for (int i = 0; i < batchSize; i++) {
                        documents.add(new Document("_id", key+"-"+i).append("value",value));
                    }
                    OperationSubscriber<Success> subscriber = new OperationSubscriber<>();

                    result.sampleStart(); // start stopwatch

                    collection.insertMany(documents).subscribe(subscriber);

                    subscriber.await(timeout, TimeUnit.MILLISECONDS);

                    result.sampleEnd(); // stop stopwatch

                    if(!subscriber.isCompleted()) {
                        successful = false;
                        message ="subscriber request was not completed. Timeout?";
                    }
                }
            }
            else
            if(testType.equals("readMany"))
            {
                if(value.equals(""))
                {
                    logger.error("Null value insert not implemented at test type write");
                    throw new Exception("please configure a value for insert.");
                }
                else
                {
                    Long batchSize = context.getLongParameter( "batchSize" );

                    result.sampleStart(); // start stopwatch

                    ObservableSubscriber<Document> subscriber = new ObservableSubscriber<Document>();

                    collection.find(regex("_id", key+".*")).subscribe(subscriber);

                    subscriber.await(timeout, TimeUnit.MILLISECONDS);

                    result.sampleEnd(); // stop stopwatch

                    List<Document> docs=subscriber.getReceived();

                    if(docs.isEmpty()) {
                        successful = false;
                        message = "no value was returned to read request";
                    }
                    else
                    if(docs.size()!=batchSize) {
                        successful=false;
                        message="The number of values returned is not equal to the batchSize, it is " + docs.size() + " expected " + batchSize;
                    }
                    else
                    if(!value.equals("")) {
                        String retrievedVal = docs.get(0).getString("value");

                        if (!value.equals(retrievedVal)) {
                            successful=false;
                            message = "Value verification failed. Expected:" + value + " got instead:" + retrievedVal;
                        }
                    }
                }
            }



        } catch (Throwable e) {

            // get stack trace as a String to return as document data
            java.io.StringWriter stringWriter = new java.io.StringWriter();
            e.printStackTrace( new java.io.PrintWriter( stringWriter ) );
            message = stringWriter.toString();

            logger.error("runTest: "+e.getClass()+" "+e.getMessage()+" "+stringWriter.toString());
        }
        finally {


            result.setResponseMessage(message);
            result.setResponseData( message , "utf-8");
            result.setDataType( org.apache.jmeter.samplers.SampleResult.TEXT );
            result.setSuccessful(successful);

            if(successful)
                result.setResponseCodeOK(); // 200 code
            else
                result.setResponseCode( "500" );

        }


        return result;
    }


    public static class OperationSubscriber<T> extends ObservableSubscriber<T> {

        @Override
        public void onSubscribe(final Subscription s) {
            super.onSubscribe(s);
            s.request(Integer.MAX_VALUE);
        }
    }



}
