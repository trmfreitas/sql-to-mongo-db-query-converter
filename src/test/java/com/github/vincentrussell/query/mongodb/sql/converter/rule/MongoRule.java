package com.github.vincentrussell.query.mongodb.sql.converter.rule;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

import org.junit.rules.ExternalResource;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import de.flapdoodle.embed.mongo.commands.MongodArguments;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.IFeatureAwareVersion;
import de.flapdoodle.embed.mongo.transitions.Mongod;
import de.flapdoodle.embed.mongo.transitions.RunningMongodProcess;
import de.flapdoodle.embed.process.io.ProcessOutput;
import de.flapdoodle.embed.process.io.Processors;
import de.flapdoodle.reverse.Transition;
import de.flapdoodle.reverse.TransitionWalker;
import de.flapdoodle.reverse.TransitionWalker.ReachedState;
import de.flapdoodle.reverse.transitions.Start;

public class MongoRule extends ExternalResource {

    private final IFeatureAwareVersion version;
//    private MongodStarter starter = MongodStarter.getDefaultInstance();
//    private MongodProcess mongodProcess;
//    private MongodExecutable mongodExecutable;
    private int port = getRandomFreePort();
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private MongoCollection mongoCollection;
	private TransitionWalker.ReachedState<RunningMongodProcess> runningMongo;


    public MongoRule(IFeatureAwareVersion version) {
        this.version = version;
    }

    @Override
    protected void before() throws Throwable {
    	Mongod mongod = new Mongod() {
    		  @Override
    		  public Transition<MongodArguments> mongodArguments() {
    		    return Start.to(MongodArguments.class)
    		      .initializedWith(MongodArguments.defaults().withIsVerbose(false).withSyncDelay(10)
    		        .withUseNoPrealloc(false)
    		        .withUseSmallFiles(false));
    		  }
    		  public de.flapdoodle.reverse.Transition<Net> net() {
    			  System.out.println("Port is " + port);
    			  return Start.to(Net.class).initializedWith(Net.of("0.0.0.0", port, false));
    		  };
    		  
    		};
    	 runningMongo = mongod.start(version);
//    	ImmutableMongod mongod = Mongod.builder()
//  			  .net(Start.to(Net.class).initializedWith(Net.defaults()
//  			    .withPort(getRandomFreePort()))).
//  			  
//  			  .build();
//        MongodConfig mongodConfig = MongodConfig.builder()
//                .version(version)
//                .cmdOptions(ImmutableMongoCmdOptions.builder()
//                        .useNoPrealloc(false)
//                        .useSmallFiles(false)
//                        .build())
//                .net(new Net(port, Network.localhostIsIPv6()))
//                .build();
//
//        mongodExecutable = starter.prepare(mongodConfig);
//        mongodProcess = mongodExecutable.start();
        mongoClient = MongoClients.create(new ConnectionString("mongodb://localhost:" + port));
//    	 mongoClient = MongoClients.create(new ConnectionString("mongodb://localhost:" + 27017));
    }

    public MongoDatabase getDatabase(String databaseName) {
        return mongoClient.getDatabase(databaseName);
    }

    private static int getRandomFreePort() {
        Random r = new Random();
        int count = 0;

        while (count < 13) {
            int port = r.nextInt((1 << 16) - 1024) + 1024;

            ServerSocket so = null;
            try {
                so = new ServerSocket(port);
                so.setReuseAddress(true);
                return port;
            } catch (IOException ioe) {

            } finally {
                if (so != null)
                    try {
                        so.close();
                    } catch (IOException e) {}
            }

        }

        throw new RuntimeException("Unable to find port");
    }

    @Override
    protected void after() {
        mongoClient.close();
//        runningMongo.close();
    }


}
