package edu.buaa.act.jcsindex.global.peer.info;

/**
 * the class of tuple-level index
 * @author Wu Sai
 *
 */
public class TupleIndexInfo {

	/**
	 * the relation name
	 */
	private String relation;
	
	/**
	 * attribute name
	 */
	private String attribute;
	
	/**
	 * attribute index
	 */
	private int idx;
	
	/**
	 * attribute type
	 */
	private int type;
	
	/**
	 * value of the tuple
	 */
	private String[] data;
	
	public TupleIndexInfo(String relation, String attribute, int idx, int type, String[] data){
		this.relation = relation;
		this.attribute = attribute;
		this.idx = idx;
		this.type = type;
		this.data = data;
	}
	
	public TupleIndexInfo(String serializedData){
		try{
			String[] args = serializedData.split("$");
			this.relation = args[0];
			this.attribute = args[1];
			this.idx = Integer.parseInt(args[2]);
			this.type = Integer.parseInt(args[3]);
			String[] data = args[4].split(" ");
		}
		catch(Exception e){
			System.out.println("Fail to parse TupleIndexInfo");
			e.printStackTrace();
		}
	}
	
	public boolean isSatisfied(String relname, String attname){
		if(this.relation.equals(relname) && this.relation.equals(attname)){
			return true;
		}
		else return false;
	}
	
	public int getIdx(){
		return this.idx;
	}
	
	public int getType(){
		return this.type;
	}
	
	public String[] getData(){
		return this.data;
	}
    
	public String serialize(){
		String msg = "";
		msg += this.relation + "$" + this.attribute + "$" + this.idx + "$" + this.type + "$";
		for(int i=0; i<this.data.length; i++)
			msg += this.data[i] + " ";
		msg = msg.trim();
		return msg;
	}
}
