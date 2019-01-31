package com.bigstep;


import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import org.junit.*;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

public class TestSampler{

    private static final MongodStarter starter = MongodStarter.getDefaultInstance();

    private static MongodExecutable _mongodExe;
    private static MongodProcess _mongod;

    private static MongoClient _mongo;

    @BeforeClass
    public static void setUp() throws Exception {

        _mongodExe = starter.prepare(new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net("localhost", 12345, Network.localhostIsIPv6()))
                .build());
        _mongod = _mongodExe.start();

        _mongo =  MongoClients.create("mongodb://localhost:12345");
    }



    @Test
    public void testReadNonExistentValue(){
        //---------------test read n
        JavaSamplerClient sampler = new MongoSampler();

        //check
        Arguments arguments = new Arguments();
        arguments.addArgument("testType","read");
        arguments.addArgument("key","k2----asdasdasd"); //key that is missing
        arguments.addArgument("value","v1"); //check that it is indeed v1
        arguments.addArgument("connectionString","mongodb://localhost:12345");
        arguments.addArgument("database","mydb");
        arguments.addArgument("collection","test");
        arguments.addArgument("batchSize","1");

        JavaSamplerContext context = new JavaSamplerContext(arguments);
        sampler.setupTest(context);

        //read test, should throw exception
        SampleResult result = sampler.runTest(context);
        assertEquals("500",result.getResponseCode());

        sampler.teardownTest(context);
    }


    @Test
    public void testReadWrite(){

        //write test: --------------------------------
        JavaSamplerClient sampler = new MongoSampler();

        Arguments arguments = new Arguments();
        arguments.addArgument("testType","write");
        arguments.addArgument("key","k1");
        arguments.addArgument("value","v1"); //write v1
        arguments.addArgument("connectionString","mongodb://localhost:12345");
        arguments.addArgument("database","mydb");
        arguments.addArgument("collection","test");
        arguments.addArgument("batchSize","1");

        JavaSamplerContext context = new JavaSamplerContext(arguments);
        sampler.setupTest(context);

        //write test
        SampleResult result = sampler.runTest(context);
        assertTrue (result.isResponseCodeOK());


        //read test: --------------------------------
        JavaSamplerClient sampler2 = new MongoSampler();

        Arguments arguments2 = new Arguments();
        arguments2.addArgument("testType","read");
        arguments2.addArgument("key","k1");
        arguments2.addArgument("value","v1"); //check that it is indeed v1
        arguments2.addArgument("connectionString","mongodb://localhost:12345");
        arguments2.addArgument("database","mydb");
        arguments2.addArgument("collection","test");
        arguments2.addArgument("batchSize","1");

        JavaSamplerContext context2 = new JavaSamplerContext(arguments2);
        sampler2.setupTest(context2);

        //read value
        SampleResult result2 = sampler.runTest(context2);
        assertTrue (result2.isResponseCodeOK());


        //read test with diff value: --------------------------------
        JavaSamplerClient sampler3 = new MongoSampler();

        Arguments arguments3 = new Arguments();
        arguments3.addArgument("testType","read");
        arguments3.addArgument("key","k1");
        arguments3.addArgument("value","v2-asdasd"); //check that it detects an issue
        arguments3.addArgument("connectionString","mongodb://localhost:12345");
        arguments3.addArgument("database","mydb");
        arguments3.addArgument("collection","test");
        arguments3.addArgument("batchSize","1");

        JavaSamplerContext context3 = new JavaSamplerContext(arguments3);
        sampler3.setupTest(context3);

        //read value
        SampleResult result3 = sampler.runTest(context3);
        assertFalse (result3.isResponseCodeOK());


        sampler.teardownTest(context);
    }


    @Test
    public void testReadWriteMany(){

        //write test: --------------------------------
        JavaSamplerClient sampler = new MongoSampler();

        Arguments arguments = new Arguments();
        arguments.addArgument("testType","writeMany");
        arguments.addArgument("key","xk1");
        arguments.addArgument("value","v1"); //write v1
        arguments.addArgument("connectionString","mongodb://localhost:12345");
        arguments.addArgument("database","mydb");
        arguments.addArgument("collection","test");
        arguments.addArgument("batchSize","100");

        JavaSamplerContext context = new JavaSamplerContext(arguments);
        sampler.setupTest(context);

        //write many test
        SampleResult result = sampler.runTest(context);
        assertTrue (result.isResponseCodeOK());


        //read many test: --------------------------------
        JavaSamplerClient sampler2 = new MongoSampler();

        Arguments arguments2 = new Arguments();
        arguments2.addArgument("testType","readMany");
        arguments2.addArgument("key","xk1");
        arguments2.addArgument("value","v1"); //check that it is indeed v1
        arguments2.addArgument("connectionString","mongodb://localhost:12345");
        arguments2.addArgument("database","mydb");
        arguments2.addArgument("collection","test");
        arguments2.addArgument("batchSize","100");

        JavaSamplerContext context2 = new JavaSamplerContext(arguments2);
        sampler2.setupTest(context2);

        //read value
        SampleResult result2 = sampler.runTest(context2);
        assertTrue (result2.isResponseCodeOK());


        //read test with diff value: --------------------------------
        JavaSamplerClient sampler3 = new MongoSampler();

        Arguments arguments3 = new Arguments();
        arguments3.addArgument("testType","readMany");
        arguments3.addArgument("key","xk1");
        arguments3.addArgument("value","v2-asdasd"); //check that it detects an issue
        arguments3.addArgument("connectionString","mongodb://localhost:12345");
        arguments3.addArgument("database","mydb");
        arguments3.addArgument("collection","test");
        arguments2.addArgument("batchSize","100");

        JavaSamplerContext context3 = new JavaSamplerContext(arguments3);
        sampler3.setupTest(context3);

        //read value
        SampleResult result3 = sampler.runTest(context3);
        assertFalse (result3.isResponseCodeOK());


        sampler.teardownTest(context);
    }


    @AfterClass
    public static void tearDown() throws Exception {
        _mongod.stop();
        _mongodExe.stop();
    }
}
