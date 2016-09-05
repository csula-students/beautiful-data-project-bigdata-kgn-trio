package edu.csula.datascience.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
@JsonIgnoreProperties(ignoreUnknown = true)
public class CrimeJSON {
	
	public String arrest;
//	public String beat;
//	public String block;
	public String case_number;
	public String community_area;
	public String date;
	public String description;
	public String district;
	public String domestic;
	public String fbi_code;
	public String id;
//	public String iucr;
//	public String latitude;
	//public String location;
	public String location_description;
//	public String longitude;
	public String primary_type;
	public String updated_on;
	public String ward;
//	public String x_coordinate;
//	public String y_coordinate;
	public String year;
	
	public CrimeJSON(){}
	
	public CrimeJSON(String arrest, String beat, String block, String case_number, String community_area, String date,
			String description, String district, String domestic, String fbi_code, String id, String iucr,
			String latitude, String location_description, String longitude, String primary_type,
			String updated_on, String ward, String x_coordinate, String y_coordinate, String year) {
		super();
		this.arrest = arrest;
//		this.beat = beat;
//		this.block = block;
		this.case_number = case_number;
		this.community_area = community_area;
		this.date = date;
		this.description = description;
		this.district = district;
		this.domestic = domestic;
		this.fbi_code = fbi_code;
		this.id = id;
//		this.iucr = iucr;
//		this.latitude = latitude;
		//this.location = location;
		this.location_description = location_description;
//		this.longitude = longitude;
		this.primary_type = primary_type;
		this.updated_on = updated_on;
		this.ward = ward;
//		this.x_coordinate = x_coordinate;
//		this.y_coordinate = y_coordinate;
		this.year = year;
	}
	public String getArrest() {
		return arrest;
	}
	public void setArrest(String arrest) {
		this.arrest = arrest;
	}
	/*public String getBeat() {
		return beat;
	}
	public void setBeat(String beat) {
		this.beat = beat;
	}
	public String getBlock() {
		return block;
	}
	public void setBlock(String block) {
		this.block = block;
	}*/
	public String getCase_number() {
		return case_number;
	}
	public void setCase_number(String case_number) {
		this.case_number = case_number;
	}
	public String getCommunity_area() {
		return community_area;
	}
	public void setCommunity_area(String community_area) {
		this.community_area = community_area;
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
	public String getDomestic() {
		return domestic;
	}
	public void setDomestic(String domestic) {
		this.domestic = domestic;
	}
	public String getFbi_code() {
		return fbi_code;
	}
	public void setFbi_code(String fbi_code) {
		this.fbi_code = fbi_code;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	/*public String getIucr() {
		return iucr;
	}
	public void setIucr(String iucr) {
		this.iucr = iucr;
	}
	public String getLatitude() {
		return latitude;
	}
	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}*/
/*	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}*/
	public String getLocation_description() {
		return location_description;
	}
	public void setLocation_description(String location_description) {
		this.location_description = location_description;
	}
	/*public String getLongitude() {
		return longitude;
	}
	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}*/
	public String getPrimary_type() {
		return primary_type;
	}
	public void setPrimary_type(String primary_type) {
		this.primary_type = primary_type;
	}
	public String getUpdated_on() {
		return updated_on;
	}
	public void setUpdated_on(String updated_on) {
		this.updated_on = updated_on;
	}
	public String getWard() {
		return ward;
	}
	public void setWard(String ward) {
		this.ward = ward;
	}
	/*public String getX_coordinate() {
		return x_coordinate;
	}
	public void setX_coordinate(String x_coordinate) {
		this.x_coordinate = x_coordinate;
	}
	public String getY_coordinate() {
		return y_coordinate;
	}
	public void setY_coordinate(String y_coordinate) {
		this.y_coordinate = y_coordinate;
	}*/
	public String getYear() {
		return year;
	}
	public void setYear(String year) {
		this.year = year;
	}
}
