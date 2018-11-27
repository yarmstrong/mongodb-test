package com.holkem.mongodb.build;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

public class App {
	private static MongoCollection<Document> coll;
	private static AtomicInteger id = new AtomicInteger(0);
	
    public static void main(String[] args) {        
    	MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
    	MongoDatabase db = mongoClient.getDatabase("test");
    	System.out.println("Connected to database: " + db.getName());
    	
    	for (String dbName : db.listCollectionNames()) {
			System.out.println(dbName);
		}
    	    	
    	coll = db.getCollection("employees");
    	insertTestData(); // SQL INSERT STATEMENTS
    	printCollection(coll.find().iterator());
    	
    	// SQL SELECT STATEMENTS AS MONGODB QUERIES
    	MongoCursor<Document> coll1;
    	
    	// select name, age from employees where employee_id = 5
    	/* shell command pattern
    	 db.enployees.find(
    	 	{ "employee_id": 5 },   => new Document (1 value)
    	 	{ "name": 1, "age": 2 } => new Document + append (projections / fields)
    	 )
    	 */
    	coll1 = coll.find(new Document("employee_id", 5)).projection(new Document("name", 1).append("age", 2)).iterator();
    	System.out.println("employee6 with name and age fields only");
    	printCollection(coll1);
    	
    	// select * from employees where employee_id in (2, 4, 5)
    	/* shell command pattern
    	 db.inventory.find({ 
    	 	status: { $in: [ "A", "D" ] } => status doc with value of, $in doc with value of ArrayList
    	 })
    	 */
    	coll1 = coll.find(new Document("employee_id", new Document("$in", Arrays.asList(2, 4, 5)))).iterator();
    	System.out.println("select in list 2, 4, 5");
    	printCollection(coll1);
    	
    	// select * from employees where age > 30
    	coll1 = coll.find(new Document("age", new Document("$gt", 30))).iterator();
    	System.out.println("above 30");
    	printCollection(coll1);
    	
    	// select * from employees where age > 30 and age <= 40
    	coll1 = coll.find(new Document("age", new Document("$gt", 30).append("$lte", 40))).iterator();
    	System.out.println("ages 30-40");
    	printCollection(coll1);
    	
    	// select * from employees where age > 30 and age <= 40 OR marital_status = "s"
    	/* shell command pattern
    	 db.inventory.find({ 
    	 	$or: [ { status: "A" }, { qty: { $lt: 30 } } ] => $or doc with value of ArrayList (of 2 doc of condition)
    	 })
    	 */
    	coll1 = coll.find(
    			new Document("$or", Arrays.asList(
    				new Document("age", new Document("$gt", 30).append("$lte", 40)),
    				new Document("marital_status", "s")
    			)))
    			.iterator();
    	System.out.println("ages 30-40 or single");
    	printCollection(coll1);
    	
    	// select * from employees where age > 30 and age <= 40 and marital_status = "m"
    	// shell command  pattern: listing of conditions (same as inserting)
    	coll1 = coll.find(
    			new Document("age", new Document("$gt", 30).append("$lte", 40))
    				 .append("marital_status", "m")
    			).iterator();
    	System.out.println("ages 30-40 and married");
    	printCollection(coll1);
    	
    	// SQL UPDATE STATEMENTS
    	// update users set status = "reject" where age < 18;
    	/* shell command pattern
    	 db.users.updateMany(
    	 	{ age: { $lt: 18} },			=> update condition in new doc
    	 	{ $set: { status: "reject" } }  => new doc using $set update operator, with a new doc for new value setting
    	 )
    	 */
    	UpdateResult updateMany = coll.updateMany(
    			new Document("age", new Document("$gt", 30).append("$lte", 40))
					.append("marital_status", "m"),
    			new Document("$set", new Document("eligibility", 0))
    			);
    	System.out.format("eligibility status was completed: %b, and", updateMany.wasAcknowledged());
    	System.out.println("eligibilty status updated for: " + updateMany.getModifiedCount() + " employees");
    	System.out.println(updateMany.wasAcknowledged());
    	coll1 = coll.find(
    			new Document("age", new Document("$gt", 30).append("$lte", 40))
    				 .append("marital_status", "m")
    			).iterator();
    	System.out.println("ages 30-40 and married after eligibility update");
    	printCollection(coll1);
    	
    	// SQL DELETE STATEMENTS
    	// update users set status = "reject" where age < 18;
    	/* shell command pattern
    	 db.users.deleteMany(
    	 	{ age: { $lt: 18} } => delete condition in new doc
    	 )
    	 */
    	DeleteResult deleteMany = coll.deleteMany(new Document("eligibility", 0));
    	System.out.println("Deleted employees: " + deleteMany.getDeletedCount());
    	
    	mongoClient.close();
    }

    private static void printCollection(MongoCursor<Document> cursor) {
        while (cursor.hasNext()) {
        	Document doc = cursor.next();
        	System.out.println(doc.toJson());
        }
        System.out.println();
	}
    
    private static void insertTestData() {
    	coll.drop();
    	
    	Random rand = new Random();
    	String[] status = new String[] { "m", "s" };
    	
    	// batch 1 - use of append
		Document doc1 = new Document()
				.append("employee_id", id.incrementAndGet())
				.append("name", "employee1")
				.append("age", 45)
				.append("marital_status", status[0])
				.append("eligibility", rand.nextInt(2));
        coll.insertOne(doc1);
        
        List<Document> list = new ArrayList<>();
        list.add(new Document()
				.append("employee_id", id.incrementAndGet())
				.append("name", "employee2")
				.append("age", 40)
				.append("marital_status", status[0])
				.append("eligibility", rand.nextInt(2)));
        list.add(new Document()
				.append("employee_id", id.incrementAndGet())
				.append("name", "employee3")
				.append("age", 35)
				.append("marital_status", status[0])
				.append("eligibility", rand.nextInt(2)));
        coll.insertMany(list);
        
        for (int i = 4; i < 11; i++) {
        	coll.insertOne(new Document()
    				.append("employee_id", i)
    				.append("name", "employee" + i)
    				.append("age", 20 + rand.nextInt(25))
    				.append("marital_status", status[rand.nextInt(2)])
    				.append("eligibility", rand.nextInt(2)));
		}
    }
}
