package edu.csula.datascience.acquisition;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.net.ssl.HttpsURLConnection;
import com.fasterxml.jackson.databind.ObjectMapper;
//import edu.csula.datascience.model.Configuration;
import edu.csula.datascience.model.Crime;
import edu.csula.datascience.model.CrimeJSON;


public class APIDataSource implements Source<CrimeJSON> {

	private String input = null;
	 private long ct=0;
	 int exceptionct=0;
	 private BufferedReader br=null; 
	  private ObjectMapper mapper=new ObjectMapper();
	  List<CrimeJSON> listJson=new ArrayList<CrimeJSON>();
	  String https_url="";
	  
	  public APIDataSource(){}
	  
	@Override
	public boolean hasNext() {
		return true;
	}

	@Override
	public Collection<CrimeJSON> next()  {
		
		try{
			  
			  if(input.substring(0,1).contains("["))
				  input = input.substring(1);
		    
		    else if(input.substring(0,1).contains(","))
			     input = input.substring(1);
			  
			    listJson.add(mapper.readValue(input, CrimeJSON.class));
			  	ct++;
			   
		    }catch(Exception e)
		    {
		    	System.out.println("1-->"+e.getMessage());
		    	e.printStackTrace();
		    	  exceptionct++;
		    }
		 
		 return listJson;
	}
	
	
	public void getDataFromAPI()
	{
		  int exceptionct=0;
		  APIDataCollector dataCollector=new APIDataCollector();
		 
		  for(int year=2001;year<2017;year++){
		 try {

//			   https_url = Configuration.url+"?$$app_token="+Configuration.appToken+"&$limit=10000000&year="+year;
			  URL  url = new URL(https_url);
			  System.setProperty("https.protocols", "TLSv1.1");
			  HttpsURLConnection con = (HttpsURLConnection)url.openConnection();
			   br = new BufferedReader(new InputStreamReader(con.getInputStream()));
			
			  while ((input = br.readLine()) != null)
			  {

				  listJson=(List<CrimeJSON>) this.next();
			    
			      if(ct%300==0)
			      {
			    	  try{
			    		
			    		  dataCollector.save((List<Crime>) dataCollector.mungee(listJson));
			    		  listJson=new ArrayList<CrimeJSON>();
					      }catch(Exception e)
					      {
					    	  System.out.println("db-->"+e.getMessage());
					    	  e.printStackTrace();
					      }
					      finally{listJson= new ArrayList<CrimeJSON>();}
			      }
				  System.out.println(ct); 
			   }

//			  System.out.println(ct);
			   System.out.println("Exception ct--> "+exceptionct);
			   System.out.println("-------------"+year+"--- End-----------------------");
			  
				
	      } 
	      catch(Exception e)
	      {
	    	  System.err.println(input);
	    	 e.printStackTrace();
	      }finally
	      {
				try {
						if(br!=null)
						br.close();
						 if(listJson.size()!=0)
						 {
							 dataCollector.save((List<Crime>) dataCollector.mungee(listJson));
//				    		  mongoDAL.insertIntoCollection((List<Crime>) dataCollector.mungee(listJson));
				    		  listJson=new ArrayList<CrimeJSON>();
						 }
					} catch (Exception e) 
					{
						System.out.println(e.getMessage());
					}
				 	finally{listJson= new ArrayList<CrimeJSON>();}
	      }
		  }
		
		
		
	}

}
