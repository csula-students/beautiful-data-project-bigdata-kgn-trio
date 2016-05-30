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
	public String block;
	public String location;
	public String ward;
	public String community_area;
	
	public Crime(){}
	
	
	public Crime(int arrest, String caseNumber, String date, String description, String district, int domestic,
			String fbiCode, int id, String locationDescription, String primaryType, String updatedOn, int year,String location,String ward,String community_area) {
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
		this.location=location;
		this.ward=ward;
		this.community_area=community_area;
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
	public String getBlock() {
		return block;
	}

	public void setBlock(String block) {
		this.block = block;
	}

	
	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}
		
	public String getWard() {
		return ward;
	}


	public void setWard(String ward) {
		this.ward = ward;
	}


	public String getCommunity_area() {
		return community_area;
	}


	public void setCommunity_area(String community_area) {
		this.community_area = community_area;
	}


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
			crime.setBlock(json.getBlock());
			crime.setLocation(json.getLatitude()+","+json.getLongitude());
			crime.setCommunity_area(json.getCommunity_area());
			crime.setWard(json.getWard());
			
			return crime;
		}

	public Crime convertToCrimeEntity(CrimeJSON json)
	{
		if(json.getArrest()==null || json.getCase_number()==null||
			json.getDate()==null || json.getDistrict()==null ||
			json.getDescription()==null|| json.getDomestic()==null ||
			json.getFbi_code()==null|| json.getId()==null ||
			json.getLocation_description()==null|| json.getPrimary_type()==null ||
			json.getUpdated_on()==null|| json.getYear()==null  || json.getBlock()==null
			|| json.getLatitude()==null	||	json.getLongitude()==null  || json.getWard()==null || json.getCommunity_area()==null )
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
		crime.setBlock(json.getBlock());
		crime.setLocation(json.getLongitude()+","+json.getLatitude());
		crime.setCommunity_area(json.getCommunity_area());
		crime.setWard(json.getWard());
		return crime;
	}
	
	
	
	

}
