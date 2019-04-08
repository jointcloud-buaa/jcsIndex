/**
 * Created on Sep 2, 2008
 */
package edu.buaa.act.jcsindex.global.utils;

/**
 * This class is used to generate a unique name for an event
 * @author David Jiang
 *
 */
public class NameUtil {
	public static String tableEventName(String tableName){
		return "%tb_idx%" + tableName;
	}
	
	public static String queryEventName(String sql){
		return "%query%" + Integer.toHexString(sql.hashCode());
	}
	
	public static void main(String argv[]){
		System.out.println(NameUtil.tableEventName("emp"));
	}
}
