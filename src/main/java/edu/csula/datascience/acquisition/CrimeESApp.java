package edu.csula.datascience.acquisition;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
//import edu.csula.datascience.model.Configuration;
import edu.csula.datascience.model.Crime;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;


public class CrimeESApp {

	public static MongoClient mongoClient = null;
    MongoDatabase database;
    MongoCollection<Document> collection;
    DB db;
	
    private final static String indexName = "crime-data";
    private final static String typeName = "crime-details";
    private final static String awsAddress = "";//"http://search-bg-data-crime-wfb4wjjihogxhq447stgvxmv6a.us-west-2.es.amazonaws.com/";		
	public static void main(String[] args) {
		insertDataIntoElasticSearch();
		getDataFromMongoAndInsertIntoAWS();
	}
	
	
	public static void getDataFromMongoAndInsertIntoAWS()
	{
		String mongoDbName=null,mongoCollection=null;
		DBCollection dbCollection=null;
		DBObject dbObject=null;
		DBCursor cursor=null;
		List <Crime> crimeList=new ArrayList<Crime>();
		int countOfRecords = 0; //limit 500 records while inserting in to AWS
		try{
			mongoClient = new MongoClient("localhost", 27017);
			mongoDbName="";//Configuration.mongodatabase;
			mongoCollection="";//Configuration.mongocollection;
			dbCollection= mongoClient.getDB(mongoDbName).getCollection(mongoCollection);
			cursor=dbCollection.find().sort(new BasicDBObject("date",-1)).limit(5000000);
			Gson gson = new Gson();
			
			JestClientFactory factory = new JestClientFactory();
	        factory.setHttpClientConfig(new HttpClientConfig
	            .Builder(awsAddress)
	            .multiThreaded(true)
	            .build());
	        JestClient client = factory.getObject();
	        
			for (DBObject record : cursor) {
				Crime c=new Crime();
    			c.setArrest(Integer.parseInt(record.get("arrest").toString()));
    			c.setCaseNumber(record.get("caseNumber").toString());
    			c.setDate(record.get("date").toString());
    			c.setDescription(record.get("description").toString());
    			c.setDistrict(record.get("district").toString());
    			c.setDomestic(Integer.parseInt(record.get("domestic").toString()));
    			c.setFbiCode(record.get("fbiCode").toString());
    			c.setId(Integer.parseInt(record.get("id").toString()));
    			c.setLocationDescription(record.get("locationDescription").toString());
    			c.setPrimaryType(record.get("primaryType").toString());
    			c.setUpdatedOn(record.get("updatedOn").toString());
    			c.setYear(Integer.parseInt(record.get("year").toString()));
    			c.setBlock(record.get("block").toString());
    			c.setLocation(record.get("location").toString());
    			if (crimeList.size() < 500)
				{
    				crimeList.add(c);
				}
    			else
    			{
    				insertInToAWSES(crimeList); //insert in to AWS ES
    				crimeList=new ArrayList<Crime>();
    			}
    	
			}
				
		}
		catch(Exception e)
        {
    		System.out.println(e.getMessage());
        }
	}
	public static void insertInToAWSES(List<Crime> list)
	{
	        JestClientFactory factory = new JestClientFactory();
	        factory.setHttpClientConfig(new HttpClientConfig
	            .Builder(awsAddress)
	            .multiThreaded(true)
	            .build());
	        JestClient client = factory.getObject();
	        try {
	            Collection<BulkableAction> actions = Lists.newArrayList();
	            list.stream()
	                .forEach(tmp -> {
	                    actions.add(new Index.Builder(tmp).build());
	                });
	            Bulk.Builder bulk = new Bulk.Builder()
	                .defaultIndex(indexName)
	                .defaultType(typeName)
	                .addAction(actions);
	            client.execute(bulk.build());
	            System.out.println("Inserted 500 documents to cloud");
	        } catch (IOException e) {
	        	insertInToAWSES(list);
	            //e.printStackTrace();
	        }
	}
	public static void insertInToLocalES()
	{
		Node node = nodeBuilder().settings(Settings.builder()
	            .put("cluster.name", "gurujsprasad")
	            .put("path.home", "elasticsearch-data")).node();
	        Client client = node.client();
	        BulkProcessor bulkProcessor = BulkProcessor.builder(
	                client,
	                new BulkProcessor.Listener() {
	                    @Override
	                    public void beforeBulk(long executionId,
	                                           BulkRequest request) {
	                    }

	                    @Override
	                    public void afterBulk(long executionId,
	                                          BulkRequest request,
	                                          BulkResponse response) {
	                    }

	                    @Override
	                    public void afterBulk(long executionId,
	                                          BulkRequest request,
	                                          Throwable failure) {
	                        System.out.println("Error while importing data to elastic search");
	                        failure.printStackTrace();
	                    }
	                })
	                .setBulkActions(10000)
	                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
	                .setFlushInterval(TimeValue.timeValueSeconds(5))
	                .setConcurrentRequests(1)
	                .setBackoffPolicy(
	                    BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
	                .build();
	}

	
	public static void insertDataIntoElasticSearch()
	{
		String mongoDbName=null,mongoCollection=null;
		DBCollection dbCollection=null;
		DBObject dbObject=null;
		DBCursor cursor=null;
		
		  Node node = nodeBuilder().settings(Settings.builder()
		            .put("cluster.name", "navnbp")
		            .put("path.home", "elasticsearch-data")).node();
		        Client client = node.client();

		        
		        BulkProcessor bulkProcessor = BulkProcessor.builder(
		                client,
		                new BulkProcessor.Listener() {
		                    @Override
		                    public void beforeBulk(long executionId,
		                                           BulkRequest request) {
		                    }

		                    @Override
		                    public void afterBulk(long executionId,
		                                          BulkRequest request,
		                                          BulkResponse response) {
		                    }

		                    @Override
		                    public void afterBulk(long executionId,
		                                          BulkRequest request,
		                                          Throwable failure) {
		                        System.out.println("Error while importing data to elastic search");
		                        failure.printStackTrace();
		                    }
		                })
		                .setBulkActions(10000)
		                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
		                .setFlushInterval(TimeValue.timeValueSeconds(5))
		                .setConcurrentRequests(1)
		                .setBackoffPolicy(
		                    BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
		                .build();
		
		      
		        
		        try{
		    		mongoClient = new MongoClient("localhost", 27017);
		    		mongoDbName="";//Configuration.mongodatabase;
		    		mongoCollection="";//Configuration.mongocollection;
		    		dbCollection= mongoClient.getDB(mongoDbName).getCollection(mongoCollection);
		    		cursor=dbCollection.find();
		    		  Gson gson = new Gson();
		    		
		    		  cursor.forEach(record->{
		    			Crime c=new Crime();
		    			c.setArrest(Integer.parseInt(record.get("arrest").toString()));
		    			c.setCaseNumber(record.get("caseNumber").toString());
		    			c.setDate(record.get("date").toString());
		    			c.setDescription(record.get("description").toString());
		    			c.setDistrict(record.get("district").toString());
		    			c.setDomestic(Integer.parseInt(record.get("domestic").toString()));
		    			c.setFbiCode(record.get("fbiCode").toString());
		    			c.setId(Integer.parseInt(record.get("id").toString()));
		    			c.setLocationDescription(record.get("locationDescription").toString());
		    			c.setPrimaryType(record.get("primaryType").toString());
		    			c.setUpdatedOn(record.get("updatedOn").toString());
		    			c.setYear(Integer.parseInt(record.get("year").toString()));
		    			c.setBlock(record.get("block").toString());
		    			c.setLocation(record.get("location").toString());
		    			c.setWard(record.get("ward").toString());
		    			c.setCommunity_area(record.get("community_area").toString());
		    			
		    			   bulkProcessor.add(new IndexRequest(indexName, typeName)
		                           .source(gson.toJson(c)));
		    			
		    		});
		    	 
		    	}
		    	catch(Exception e)
		        {
		    		System.out.println(e.getMessage());
		        }
		    	
		        
		        
	}
	
	
	
}

/*PUT /crime-data/
{
    "mappings" : {
        "crime-details" : {
            "properties" : {
                "id" : {
                    "type" : "long"
                },
                "case-number" : {
                    "type" : "string",
                    "index" : "not_analyzed"
                },
                "date" : {
                    "type" : "date"
                },
				 "block" : {
                    "type" : "string",
					 "index" : "not_analyzed"
                },
				 "primary-type" : {
                    "type" : "string",
					 "index" : "not_analyzed"
                },
				 "description" : {
                    "type" : "string",
					 "index" : "not_analyzed"
                },
				 "location-description" : {
                    "type" : "string",
					 "index" : "not_analyzed"
                },
				 "arrest" : {
                    "type" : "integer"
                },
				 "domestic" : {
                    "type" : "integer"
                },
				 "district" : {
                    "type" : "integer"
				 },
				 "community-area" : {
                    "type" : "integer"
                },
				 "fbi-code" : {
                    "type" : "integer"
                },
                "year": {
                    "type": "date"
                },
				 "updated-on": {
                    "type": "date"
                },
				 "location": {
                    "type": "geo_point"
                }
				 
            }
        }
    }
}

*/

/* "ward" : {
"type" : "integer"
},*/
