package com.mapr.kafka.serializer.json;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Date;
import java.util.StringTokenizer;


public class Contact {
	private String ProjectCode;
	private String ProjectName;
	private int TotalBid;
	private int ExpectedDuration;
	private String ExpectedStartDate;
	private String ExpectedEndDate;
    private int contactId;
    private String firstName;
    private String lastName;

    public Contact(){

    }
    public Contact(String ProjectCode, String ProjectName, int TotalBid, int ExpectedDuration, String ExpectedStartDate, String ExpectedEndDate) {
	this.ProjectCode=ProjectCode;
	this.ProjectName=ProjectName;
	this.TotalBid=TotalBid;
	this.ExpectedDuration=ExpectedDuration;
	this.ExpectedStartDate=ExpectedStartDate;
	this.ExpectedEndDate=ExpectedEndDate;        
    }

    public void parseString(String csvStr){
        StringTokenizer st = new StringTokenizer(csvStr,",");
	ProjectCode=st.nextToken();
	ProjectName=st.nextToken();
	TotalBid= Integer.parseInt(st.nextToken());
	ExpectedDuration=Integer.parseInt(st.nextToken());
	ExpectedStartDate=(st.nextToken());
	ExpectedEndDate=(st.nextToken());
        
    }

    public String getProjectCode() {
		return ProjectCode;
    	
    }
    
    public void setProjectCode(String ProjectCode) {
		this.ProjectCode= ProjectCode;
    	
    }
    
    public String getProjectName() {
		return ProjectName;
    	
    }
    
    public void setProjectName(String ProjectName) {
		this.ProjectName= ProjectName;
    	
    }
    
    public int getTotalBid() {
		return TotalBid;
    	
    }
    
    public void setTotalBid(int TotalBid) {
		this.TotalBid= TotalBid;
    	
    }
    
    public int getExpectedDuration() {
		return ExpectedDuration;
    	
    }
    
    public void setExpectedDuration(int ExpectedDuration) {
		this.ExpectedDuration= ExpectedDuration;
    	
    }
    
    public String getExpectedStartDate() {
 		return ExpectedStartDate;
     	
     }
     
     public void setExpectedStartDate(String ExpectedStartDate) {
 		this.ExpectedStartDate= ExpectedStartDate;
     	
     }
     
     public String getExpectedEndDate() {
  		return ExpectedEndDate;
      	
      }
      
      public void setExpectedEndDate(String ExpectedEndDate) {
  		this.ExpectedEndDate=ExpectedEndDate;
      	
      }
    
    
    /////////////////////////////////////////
   

    @Override
    public String toString() {
    	//System.out.println("hii");
        return "Project{" +
                "ProjectCode=" + ProjectCode +
                ", ProjectName'" + ProjectName + '\'' +
                ", TotalBid='" + TotalBid + '\'' +
		", ExpectedDuration='" + ExpectedDuration + '\'' +
		", ExpectedStartDate='" + ExpectedStartDate + '\'' +
		", ExpectedEndDate='" + ExpectedEndDate + '\'' +
                '}';
    }

    public static void main(String[] argv)throws Exception{
        ObjectMapper mapper = new ObjectMapper();
        Contact contact = new Contact();
        /*contact.setContactId(1);
        contact.setFirstName("Sachin");
        contact.setLastName("Tendulkar");
        System.out.println(mapper.writeValueAsString(contact));
        contact.parseString("1,Rahul,Dravid");
        System.out.println(mapper.writeValueAsString(contact));*/
    }
}