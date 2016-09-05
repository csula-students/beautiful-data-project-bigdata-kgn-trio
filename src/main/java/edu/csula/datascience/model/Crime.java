package edu.csula.datascience.model;

public class Crime    {
	
	public int arrest=0;
	public String caseNumber;
	public String date;
	public String description;
	public String district;
	public int domestic=0;
	public String fbiCode;
	public int id;
	public String locationDescription;
	public String primaryType;
	public String updatedOn;
	public int year=0;
	
	public Crime(){}
	
	/* public static Crime build(Crime c)
	{
		Crime crime=new Crime();
		crime.arrest = c.arrest;
		crime.caseNumber = c.caseNumber;
		crime.date = c.date;
		crime.description = c.description;
		crime.district = c.district;
		crime.domestic = c.domestic;
		crime.fbiCode = c.fbiCode;
		crime.id = c.id;
		crime.locationDescription = c.locationDescription;
		crime.primaryType = c.primaryType;
		crime.updatedOn = c.updatedOn;
		crime.year = c.year;
		return crime;
	}*/
	
	
	
	public Crime(int arrest, String caseNumber, String date, String description, String district, int domestic,
			String fbiCode, int id, String locationDescription, String primaryType, String updatedOn, int year) {
		super();
		this.arrest = arrest;
		this.caseNumber = caseNumber;
		this.date = date;
		this.description = description;
		this.district = district;
		this.domestic = domestic;
		this.fbiCode = fbiCode;
		this.id = id;
		this.locationDescription = locationDescription;
		this.primaryType = primaryType;
		this.updatedOn = updatedOn;
		this.year = year;
	}



	public int getArrest() {
		return arrest;
	}
	
	public void setArrest(int arrest) {
		this.arrest = arrest;
	}
	public String getCaseNumber() {
		return caseNumber;
	}
	public void setCaseNumber(String caseNumber) {
		this.caseNumber = caseNumber;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getDistrict() {
		return district;
	}
	public void setDistrict(String district) {
		this.district = district;
	}
	public int getDomestic() {
		return domestic;
	}
	public void setDomestic(int domestic) {
		this.domestic = domestic;
	}
	public String getFbiCode() {
		return fbiCode;
	}
	public void setFbiCode(String fbi_code) {
		this.fbiCode = fbi_code;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getLocationDescription() {
		return locationDescription;
	}
	public void setLocationDescription(String location_description) {
		this.locationDescription = location_description;
	}
	public String getPrimaryType() {
		return primaryType;
	}
	public void setPrimaryType(String primary_type) {
		this.primaryType = primary_type;
	}
	public String getUpdatedOn() {
		return updatedOn;
	}
	public void setUpdatedOn(String updated_on) {
		this.updatedOn = updated_on;
	}
	public int getYear() {
		return year;
	}
	public void setYear(int year) {
		this.year = year;
	}
	
	
	/*public static int nullValueCt=0;
	
	public static int getNullValueCt() {
		return nullValueCt;
	}

	public static void setNullValueCt(int nullValueCt) {
		Crime.nullValueCt = nullValueCt;
	}*/
	
	/* public static Crime build(MockCrime data) {
		
		 Crime crime=new Crime();
			
			crime.setArrest(data.getArrest());
			crime.setCaseNumber(data.getCaseNumber());
			crime.setDate(data.getDate());
			crime.setDistrict(data.getDistrict());
			crime.setDescription(data.getLocationDescription());
			crime.setDomestic(data.getDomestic().equals("true")?1:0);
			crime.setFbiCode(data.getFbiCode());
			crime.setId(data.getId());
			crime.setLocationDescription(data.getLocationDescription());
			crime.setPrimaryType(data.getPrimaryType());
			crime.setUpdatedOn(data.getUpdatedOn());
			crime.setYear(data.getYear());
			return crime;
		 
	    }*/

	 public static Crime buildCrimeEntity(CrimeJSON json)
		{
			Crime crime=new Crime();
			
			crime.setArrest(json.getArrest().equals("true")?1:0);
			crime.setCaseNumber(json.getCase_number());
			crime.setDate(json.getDate().substring(0, 10));
			crime.setDistrict(json.getDistrict());
			crime.setDescription(json.getDescription());
			crime.setDomestic(json.getDomestic().equals("true")?1:0);
			crime.setFbiCode(json.getFbi_code());
			crime.setId(Integer.parseInt(json.getId()));
			crime.setLocationDescription(json.getLocation_description());
			crime.setPrimaryType(json.getPrimary_type());
			crime.setUpdatedOn(json.getUpdated_on().substring(0, 10));
			crime.setYear(Integer.parseInt(json.getYear()));
			return crime;
		}

	 
	public Crime convertToCrimeEntity(CrimeJSON json)
	{
		if(json.getArrest()==null || json.getCase_number()==null||
			json.getDate()==null || json.getDistrict()==null ||
			json.getDescription()==null|| json.getDomestic()==null ||
			json.getFbi_code()==null|| json.getId()==null ||
			json.getLocation_description()==null|| json.getPrimary_type()==null ||
			json.getUpdated_on()==null|| json.getYear()==null )
		{
			/*System.out.println("Null Value -->"+nullValueCt);
			nullValueCt++;*/
			return null;
		}
		
		Crime crime=new Crime();
		
		crime.setArrest(json.getArrest().equals("true")?1:0);
		crime.setCaseNumber(json.getCase_number());
		crime.setDate(json.getDate().substring(0, 10));
		crime.setDistrict(json.getDistrict());
		crime.setDescription(json.getDescription());
		crime.setDomestic(json.getDomestic().equals("true")?1:0);
		crime.setFbiCode(json.getFbi_code());
		crime.setId(Integer.parseInt(json.getId()));
		crime.setLocationDescription(json.getLocation_description());
		crime.setPrimaryType(json.getPrimary_type());
		crime.setUpdatedOn(json.getUpdated_on().substring(0, 10));
		crime.setYear(Integer.parseInt(json.getYear()));
		return crime;
	}
	
	
	
	

}
