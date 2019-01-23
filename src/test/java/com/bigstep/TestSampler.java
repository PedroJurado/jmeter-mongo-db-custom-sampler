package com.bigstep;

import junit.framework.*;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

public class TestSampler extends TestCase {


    public void testRead(){

        JavaSamplerClient sampler = new MongoSampler();

        Arguments arguments = new Arguments();
        arguments.addArgument("testType","read");
        arguments.addArgument("key","k");
        arguments.addArgument("value","v");
        arguments.addArgument("connectionString","mongodb://localhost");
        arguments.addArgument("database","mydb");
        arguments.addArgument("collection","test");

        JavaSamplerContext context = new JavaSamplerContext(arguments);

        sampler.setupTest(context);

        SampleResult result = sampler.runTest(context);
        assertTrue (result.isResponseCodeOK());

        result = sampler.runTest(context);
        assertTrue (result.isResponseCodeOK());

        result = sampler.runTest(context);
        assertTrue (result.isResponseCodeOK());


        sampler.teardownTest(context);
    }

    public void testWrite(){

        JavaSamplerClient sampler = new MongoSampler();

        Arguments arguments = new Arguments();
        arguments.addArgument("testType","write");
        arguments.addArgument("key","k");
        arguments.addArgument("value","v");
        arguments.addArgument("connectionString","mongodb://localhost");
        arguments.addArgument("database","mydb");
        arguments.addArgument("collection","test");

        JavaSamplerContext context = new JavaSamplerContext(arguments);

        sampler.setupTest(context);

        SampleResult result = sampler.runTest(context);
        assertTrue (result.isResponseCodeOK());

        result = sampler.runTest(context);
        assertTrue (result.isResponseCodeOK());

        result = sampler.runTest(context);
        assertTrue (result.isResponseCodeOK());


        sampler.teardownTest(context);
    }

}
