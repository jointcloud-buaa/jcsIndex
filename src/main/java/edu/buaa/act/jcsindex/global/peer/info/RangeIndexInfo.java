package edu.buaa.act.jcsindex.global.peer.info;

import java.util.ArrayList;

/**
 * indexing range entry
 * @author Wu Sai
 *
 */
public class RangeIndexInfo {

	/**
	 * relation name
	 */
	private String relation;

	/**
	 * attribute name
	 */
	private String attribute;
	
	/**
	 * attribute type
	 */
	private int type;
	
	/**
	 * the range value, although we
	 * represent them as string values,
	 * they can be casted to integer
	 * if necessary
	 */
	private String minValue;
	
	private String maxValue;
	
	/**
	 * all the peers that have data
	 * in this range
	 */
	private ArrayList<PhysicalInfo> owners;
	
	public RangeIndexInfo(String relation, String attribute, int type, String min, String max, ArrayList<PhysicalInfo> ips){
		this.relation = relation;
		this.attribute = attribute;
		this.type = type;
		this.minValue = min;
		this.maxValue = max;
		this.owners = ips;
	}
	
	public RangeIndexInfo(String serializedData){
		try{
			String[] args = serializedData.split("$");
			this.relation = args[0];
			this.attribute = args[1];
			this.type = Integer.parseInt(args[2]);
			this.minValue = args[3];
			this.maxValue = args[4];
			String[] ips = args[5].split(" ");
			this.owners = new ArrayList<PhysicalInfo>();
			for(int i=0; i<ips.length; i++){
				this.owners.add(new PhysicalInfo(ips[i]));
			}
		}
		catch(Exception e){
			System.out.println("Fail to parse the RangeIndexInfo");
			e.printStackTrace();
		}
	}
	
	public int getType(){
		return this.type;
	}
	
	public String getMinValue(){
		return this.minValue;
	}
	
	public String getMaxValue(){
		return this.maxValue;
	}
	
	public ArrayList<PhysicalInfo> getOwners(){
		return this.owners;
	}
	
	public void insertNewOwner(PhysicalInfo ip){
		if(!this.owners.contains(ip))
			this.owners.add(ip);
	}
	
	public void deleteOwner(PhysicalInfo ip){
		this.owners.remove(ip);
	}
	
	public boolean isSatisfied(String relation, String attribute){
		if(relation.equals(this.relation) && attribute.equals(this.attribute))
			return true;
		else return false;
	}
	
	public String serialize(){
		String msg = "";
		msg += this.relation + "$" + this.attribute + "$" + this.type + "$" + this.minValue + "$" + this.maxValue + "$";
		for(int i=0; i<this.owners.size()-1; i++){
			PhysicalInfo ip = this.owners.get(i);
			msg += ip.serialize() + " ";
		}
		PhysicalInfo ip = this.owners.get(this.owners.size()-1);
		msg += ip.serialize();
		return msg;
	}
}
