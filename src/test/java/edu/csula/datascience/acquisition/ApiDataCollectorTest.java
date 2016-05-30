package edu.csula.datascience.acquisition;


import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import edu.csula.datascience.model.Crime;
import edu.csula.datascience.model.CrimeJSON;

public class ApiDataCollectorTest {
	 private Collector<Crime, CrimeJSON> collector;
	    private Source<CrimeJSON> source;

	    @SuppressWarnings("unchecked")
		@Before
	    public void setup() {
	        collector = new APIDataCollector();
	       source =  new MockCrimeSource();
	    }
	    
	   
	    @Test
	    public void testMethodForMungeeNullValueRejection() throws Exception {

	    	
	    	List<Crime> list = (List<Crime>) collector.mungee(source.next());
	       Assert.assertEquals(list.size(), 3);
	     
	    }
	    
	    
	    @Test
	    public void testMethodForMungeeNullObjectCompare() throws Exception {

	    	
	    	List<Crime> list = (List<Crime>) collector.mungee(source.next());
	        List<Crime> expectedList = Lists.newArrayList(
	           
	        		  new Crime(0, "HZ229112", "2015-03-15", "SIMPLE", "016", 0, "08B", 10488624, "PARKING", "CRMINAL DAMAGE", "2016-04-12", 2016,"[41.694312,-87.631046]","34","49"),
	        		  new Crime(1, "EZ239102", "2014-01-11", "MUGGING", "025", 1, "01Z", 10596626, "GARAGE", "THREAT", "2015-04-12", 2014,"[41.694312,-87.631046]","36","39"),
	        		  new Crime(0, "YP225782", "2013-02-12", "MAJOR CRIME", "305", 0, "18B", 10493624, "NEAR COLLAGE", "THEFT", "2015-10-12", 2013,"[41.694312,-87.631046]","36","29")
	        );
	   
	       
	        for (int i = 0; i < expectedList.size(); i ++) {
	            Assert.assertEquals(list.get(i).getId(), expectedList.get(i).getId());
	            Assert.assertEquals(list.get(i).getCaseNumber(), expectedList.get(i).getCaseNumber());
	        }
	    }
	    
	    
	    @Test
	    public void testMethodForMungeeDataTransformationForArrest() throws Exception {

	    	
	    	List<Crime> list = (List<Crime>) collector.mungee( Lists.newArrayList(
	    			 new CrimeJSON("false", "0131", "0123 S STATE ST", "HZ229112", "33",  "04/15/2016 12:50:00 PM", "SIMPLE", "016", "false", "08B", "10488624", "1320", "42.825359953", "CTA PLATFORM", "-87.456778", "CRMINAL DAMAGE",  "04/12/2016 05:15:00 PM", "17",  "2016")
			        )); 
	   
	        Assert.assertEquals(0, list.get(0).getArrest());
	    }
	    
	    
	    @Test
	    public void testMethodForMungeeDataTransformationForDomestic() throws Exception {

	    	
	    	List<Crime> list = (List<Crime>) collector.mungee( Lists.newArrayList(
	    			 new CrimeJSON("false", "0131", "0123 S STATE ST", "HZ229112", "33",  "04/15/2016 12:50:00 PM", "SIMPLE", "016", "true", "08B", "10488624", "1320", "42.825359953", "CTA PLATFORM", "-87.456778", "CRMINAL DAMAGE",  "04/12/2016 05:15:00 PM", "17", "2016")
			        ));     
	   
	        Assert.assertEquals(1, list.get(0).getDomestic());
	    }
	    
	    
}
