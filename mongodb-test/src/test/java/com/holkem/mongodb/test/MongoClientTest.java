package com.holkem.mongodb.test;

import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import org.bson.Document;

public class MongoClientTest {
	@Test
    public void shouldGetADatabaseFromTheMongoClient() throws Exception {
        // When: get the database from the client
		MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
    	MongoDatabase db = mongoClient.getDatabase("test");

        // Then
        assertThat(db, is(notNullValue()));
        mongoClient.close();
    }

    @Test
    public void shouldGetACollectionFromTheDatabase() throws Exception {
        // When: get collection
		MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
    	MongoCollection<Document> collection = mongoClient.getDatabase("test").getCollection("employees");

        // Then
        assertThat(collection, is(notNullValue()));
        mongoClient.close();
    }

    @Test(expected = Exception.class)
    public void shouldNotBeAbleToUseMongoClientAfterItHasBeenClosed() throws IllegalStateException {
        // Given
        MongoClient mongoClient = new MongoClient();
        
        // When: close the mongoClient
        mongoClient.close();

        // Then
        mongoClient.getDatabase("test").getCollection("employees").insertOne(new Document()
				.append("employee_id", 0)
				.append("name", "employee1")
				.append("age", 45)
				.append("marital_status", 0)
				.append("eligibility", 0));
    }
}
