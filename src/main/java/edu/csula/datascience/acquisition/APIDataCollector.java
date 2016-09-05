package edu.csula.datascience.acquisition;


import java.util.Collection;
import java.util.stream.Collectors;
import org.bson.Document;
import com.google.gson.Gson;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
//import edu.csula.datascience.model.Configuration;
import edu.csula.datascience.model.Crime;
import edu.csula.datascience.model.CrimeJSON;

public class APIDataCollector  implements Collector<Crime, CrimeJSON> {

	public static MongoClient mongoClient = null;
    MongoDatabase database;
    MongoCollection<Document> collection;
    DB db;
    
    @SuppressWarnings("deprecation")
	public DBCollection getCollection(String db, String tableName){
		return mongoClient.getDB(db).getCollection(tableName);
	}
    
	@Override
	public Collection<Crime> mungee(Collection<CrimeJSON> src) {
		return src.stream()
	            .filter(data -> data.getCase_number() != null)
	            .filter(data -> data.getId() != null)
	            .filter(data -> data.getYear() != null)
	            .filter(data -> data.getDate() != null)
	            .filter(data -> data.getDescription() != null)
	            .filter(data -> data.getDistrict() != null)
	            .filter(data -> data.getDomestic() != null)
	            .filter(data -> data.getFbi_code() != null)
	            .filter(data -> data.getLocation_description() != null)
	            .filter(data -> data.getPrimary_type() != null)
	            .filter(data -> data.getUpdated_on() != null)
	            .map(Crime::buildCrimeEntity)
	            .collect(Collectors.toList());
	}

	@Override
	public void save(Collection<Crime> crimeLst) {
	
		if(crimeLst!=null && crimeLst.size()==0 )
			return;
	
	String mongoDbName=null,mongoCollection=null;
	DBCollection dbCollection=null;
	DBObject dbObject=null;
	 Gson  gson = new Gson();
	 
	try{
		mongoClient = new MongoClient("localhost", 27017);
		mongoDbName="BigData";//Configuration.mongodatabase;
		mongoCollection="Crime";//Configuration.mongocollection;
		dbCollection= this.getCollection(mongoDbName, mongoCollection) ;
		dbCollection.setWriteConcern(WriteConcern.UNACKNOWLEDGED);
	 
	}
	catch(Exception e){}
	
	
	 
	 for (Crime crime : crimeLst) 
	 {
		try{
		 dbObject = (DBObject) JSON.parse( gson.toJson(crime, Crime.class ));
		 dbCollection.insert(dbObject);
		 }catch(Exception e){}
	}
	 if(mongoClient!=null)
		 mongoClient.close();
		
		
		
		
	}

}
