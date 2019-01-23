package com.bigstep;

import com.mongodb.reactivestreams.client.*;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.Serializable;
import java.security.MessageDigest;

import static com.mongodb.client.model.Filters.eq;


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

        defaultParameters.addArgument("connectionString", "mongo://localhost");
        defaultParameters.addArgument("database", "mydb");
        defaultParameters.addArgument("collection", "test");

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
        result.sampleStart(); // start stopwatch

        try {
            if(collection==null)
                throw new Exception("Collection not initialized!");


            if(null==client)
                throw new Exception("Mongo Client not initialised");

            boolean testResult = true;

            if(testType.equals("read"))
            {

                Subscriber<Document> subscriber = new VerifierSubscriber<Document>();
                collection.find(eq("key",key)).first().subscribe(subscriber);
                subscriber.wait(timeout);

            }
            else
            if(testType.equals("write"))
            {
                if(value.equals(""))
                {

                  //  String nvalue = Normalizer.normalize(value, Normalizer.Form.NFC);
                  //  DBObject dbObject = (DBObject) JSON.parse(nvalue);
                  // dbObject.put("myid",(Object)key);
                  //dbObject.put("hash",md5(key));
                  //collection.insert(dbObject);

                  Document doc = new Document("key", value);

                  Subscriber<Success> subscriber = new OperationSubscriber<Success>();
                  collection.insertOne(doc).subscribe(subscriber);
                  subscriber.wait(timeout);

                  testResult = true;

                }
                else
                {
                    logger.error("Null value insert not implemented at test type write");
                    testResult = false;
                }
            }

            result.sampleEnd(); // stop stopwatch
            result.setSuccessful( testResult );
            result.setResponseMessage( "OK on object "+key );
            result.setResponseCodeOK(); // 200 code

        } catch (Exception e) {

            result.sampleEnd(); // stop stopwatch
            result.setSuccessful( false );
            result.setResponseMessage( "Exception: " + e );

            // get stack trace as a String to return as document data
            java.io.StringWriter stringWriter = new java.io.StringWriter();
            e.printStackTrace( new java.io.PrintWriter( stringWriter ) );
            result.setResponseData( stringWriter.toString() , "utf-8");
            result.setDataType( org.apache.jmeter.samplers.SampleResult.TEXT );
            result.setResponseCode( "500" );

            logger.error("runTest: "+e.getClass()+" "+e.getMessage()+" "+stringWriter.toString());
        }
        finally
        {
            if (client != null) {
                client.close();
            }
        }

        return result;
    }


     class VerifierSubscriber<T> implements Subscriber<T> {

        private  final Logger logger = Logger.getLogger(MongoSampler.class);

        public void onSubscribe(Subscription subscription) {
            subscription.request(1);
        }

         public void onNext(T t) {

         }

         public void onError(Throwable throwable) {
                logger.error("VerifierSubscriber.onError():"+throwable.getMessage());
         }

         public void onComplete() {

         }

     }

    class OperationSubscriber<Success> implements Subscriber<Success> {

        private final Logger logger = Logger.getLogger(MongoSampler.class);

        public void onSubscribe(Subscription subscription) {
            subscription.request(Integer.MAX_VALUE);
        }

        public void onNext(Success t) {

        }

        public void onError(Throwable throwable) {
            logger.error("VerifierSubscriber.onError():" + throwable.getMessage());
        }

        public void onComplete() {

        }
    }

}
