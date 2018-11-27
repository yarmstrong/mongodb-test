package com.holkem.mongodb.test;

import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class ConnectTest {
	@Test
    public void shouldCreateANewMongoClientConnectedToLocalhost() throws Exception {
        // When: get/create the MongoClient
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
        
        // Then
        assertThat(mongoClient, is(notNullValue()));
        mongoClient.close();
	}
}
