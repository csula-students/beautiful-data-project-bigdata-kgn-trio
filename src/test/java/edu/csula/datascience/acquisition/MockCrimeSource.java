package edu.csula.datascience.acquisition;

import java.util.Collection;

import com.google.common.collect.Lists;

import edu.csula.datascience.model.CrimeJSON;

public class MockCrimeSource implements Source<CrimeJSON> {

	@Override
	public boolean hasNext() {
		
		return true;
	}

	@Override
	public Collection<CrimeJSON> next() {
		 return Lists.newArrayList(
				 new CrimeJSON("false", "0213", "03724 S MICHIGAN AVE", null, "35", "04/16/2016 11:57:00 PM", "POCKET-PICKING", "010", "true", "14", "10488615", "0420", "41.826159953", "APARTMENT", "-87.456778", "THEFT",  "04/20/2016 01:57:00 PM", "44", "1177816", "1880043", "2016"),
				 new CrimeJSON("false", "0131", "0123 S STATE ST", "HZ229112", "33",  "03/15/2015 12:50:00 PM", "SIMPLE", "016", "false", "08B", "10488624", "1320", "42.825359953", "PARKING", "-86.452478", "CRMINAL DAMAGE",  "04/12/2016 05:15:00 PM", "17", "1152973", "1894914", "2015"),
				 new CrimeJSON("true", "0192", "0971 PALMETTO ST", "EZ239102", "16",  "01/01/2014 09:30:00 PM", "MUGGING", "025", "true", "01Z", "10596626", "1520", "43.121239953", "GARAGE", "-84.628778", "THREAT",  "04/12/2015 05:15:00 PM", "30", "1983973", "1964210", "2014"),
				 new CrimeJSON("false", "0131", "800N MONTESARY", "YP225782", "253",  "02/12/2013 09:45:00 PM", "MAJOR CRIME", "305", "false", "18B", "10493624", "1923", "41.123459954", "NEAR COLLAGE", "-88.123456", "THEFT",  "04/10/2015 03:23:00 PM", "98", "1935973", "1892803", "2013")
				 );
	}

}
