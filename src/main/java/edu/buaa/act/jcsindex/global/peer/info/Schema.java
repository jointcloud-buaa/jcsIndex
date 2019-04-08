package edu.buaa.act.jcsindex.global.peer.info;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Hashtable;

/**
 * record the schema of the relations
 * @author Wu Sai
 * THIS CLASS is intended to be used for initial database keywords indexed in Lucence
 */
public class Schema {

	public static int STRING_TYPE = 0;
	public static int INTEGER_TYPE = 1;
	
	/**
	 * record <relation, attribue array>
	 */
	private Hashtable relationMap;
	
	/**
	 * record <attribute, domain value>
	 */
	private Hashtable domainMap;
	
	/**
	 * record <attribute, type>
	 */
	private Hashtable typeMap;
	
	/**
	 * file recording the schema file
	 */
	private String schemaFile;
	
	/**
	 * default domain for integer values
	 */
	private static int DEFAULT_INT_MIN = 0;
	
	private static int DEFAULT_INT_MAX = 1;
	
	/**
	 *
	 *default domain for string values
	 */
	private static String DEFAULT_STR_MIN = " ";
	
	private static String DEFAULT_STR_MAX = "zzzzzzzz";
	
	public Schema(String file){
		this.schemaFile = file;
		relationMap = new Hashtable();
		domainMap = new Hashtable();
		typeMap = new Hashtable();		
	}
	
	/**
	 * load the schema info from file
	 *
	 */
	public void load(){
		try{
			BufferedReader reader = new BufferedReader(new FileReader(schemaFile));
			String line = "";
			while((line=reader.readLine())!=null){
				if(line.startsWith("#")){
					//this is the table schema
					line = line.substring(1, line.length());
					int index = line.indexOf(":");
					String relationName = line.substring(0, index);
					line = line.substring(index+1, line.length());
					String[] attribute = line.split(",");
					this.relationMap.put(relationName, attribute);
				}
				else{
					//this is the attribute property
					String[] content = line.split(":");
					String attName = content[0];
					int type = Integer.parseInt(content[1]);
					if(type==Schema.INTEGER_TYPE){
						//int min = Integer.parseInt(content[2]);
						//int max = Integer.parseInt(content[3]);
						int[] domain = new int[2];
						domain[0] = Integer.parseInt(content[2]);
						domain[1] = Integer.parseInt(content[3]);
						this.typeMap.put(attName, type);
						this.domainMap.put(attName, domain);
					}
					else{
						//it is string type
						String[] domain = new String[2];
						domain[0] = content[2];
						domain[1] = content[3];
						this.typeMap.put(attName, type);
						this.domainMap.put(attName, domain);
					}
				}
			}
			reader.close();
		}
		catch(Exception e){
			System.out.println("Fail to load the schema file");
			e.printStackTrace();
		}
	}
	
	/**
	 * given a relation name return its attribute names
	 * @param relation
	 * @return
	 */
	public String[] getAttributeName(String relation){
		if(this.relationMap.containsKey(relation)){
			return (String[])this.relationMap.get(relation); 
		}
		return null;
	}
	
	/**
	 * given a relation and one of its attribute, return the type
	 * of the attribute
	 * @param relation
	 * @param attribute
	 * @return
	 */
	public int getType(String relation, String attribute){
		String namespace = relation + "." + attribute;
		if(this.typeMap.containsKey(namespace)){
			Integer I = (Integer)this.typeMap.get(namespace);
			return I.intValue();
		}
		else return -1;
	}
	
	/**
	 * given a relation and one of its attribute, return the domain
	 * of the attribute
	 * @param relation
	 * @param attribute
	 * @return
	 */
	public Object[] getDomain(String relation, String attribute){
		String namespace = relation + "." + attribute;
		if(this.domainMap.containsKey(namespace)){
			Object[] domain = (Object[])this.domainMap.get(namespace);
			return domain;
		}
		else return null;
	}
}
