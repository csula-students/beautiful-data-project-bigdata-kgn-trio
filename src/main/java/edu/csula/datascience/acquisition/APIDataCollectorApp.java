package edu.csula.datascience.acquisition;

public class APIDataCollectorApp  {

	public static void main(String[] args) {
		
		try{
			APIDataSource apiDataSource=new APIDataSource();
			apiDataSource.getDataFromAPI();
		} catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
		
		 CrimeESApp.insertDataIntoElasticSearch();
		 
		 System.exit(0);
	}

	

}
