package edu.buaa.act.jcsindex.global.utils;

import java.sql.*;
import java.util.Vector;

/**
 * Helper class for accessing meta data of Corporate BestPeer 
 * Now for storing global schema and access control management
 * Later should integrate index db into it...
 * 
 * @author VHTam
 *
 */
public class MetaDataAccess {
	
	// follows are for global schema
	public static final String TABLE_CORPORAT_DBNAME = "CORPORATE_DB_NAME";
	public static final String TABLE_SCHEMAS = "GLOBAL_SCHEMAS";
	public static final String TABLE_COLUMNS = "GLOBAL_SCHEMA_COLUMNS";
	public static final String SCHEMA_SEPERATOR = ";";
	public static final String KEYWORD_TABLE =" TABLE ";

	// follows are for access control
	public static final String TABLE_LOCAL_ADMINS = "LOCAL_ADMINS";
	public static final String TABLE_ROLE_HIER = "ROLE_HIERARCHY";
	public static final String TABLE_ROLE_PERM = "ROLE_PERMISSIONS";
	public static final String TABLE_USER_ROLE = "USER_ROLE_ASSIGNMENT";
	public static final String TABLE_ROLES = "ROLES";
	public static final String TABLE_USERS = "USERS";
	public static final String TABLE_PRIVILEGES = "PRIVILEGES";
	public static final String TABLE_USER_PERM = "USER_PERMISSIONS";

	public static String WHOLE_TABLE = "whole table";
	public static String ROW_LEVEL = "row level";
	public static String COLUMN_LEVEL = "column level";
	public static String DOT_SEPERATOR = ".";
	public static String WHERE = "WHERE";

	public static void printData(String[][] data){
		
		if (data==null)
			return;
		
		for (int i=0; i<data.length; i++){
			for (int j=0; j<data[i].length; j++){
				System.out.print(data[i][j]+"\t");
			}
			System.out.println();
		}
	}
	
	//
	public static String metaGetCorporateDbName(Connection con){
		String[] tables = new String[]{TABLE_CORPORAT_DBNAME};
		String[] columns = new String[]{"db_name"};
		
		String[][] result = getDataArray2DFromDB(con, tables, columns);
		if (result==null){
			return "Corporate Database";
		}	
		
		return result[0][0];
	}
	
	//for schema metadata update from schema string
	public static void updateSchema(Connection con, String schemaStr){
		// delete old metadata of global schema first
		String sql = "delete from " + TABLE_SCHEMAS;
		metaUpdate(con, sql);
		sql = "delete from " + TABLE_COLUMNS;
		metaUpdate(con, sql);
		
		// insert new metadata of new global schema
		//not suppport column with primary key
		String[] createSql = schemaStr.split(SCHEMA_SEPERATOR);
		for (int i=0; i<createSql.length; i++){
			String tableSql = createSql[i];
			//System.out.println(tableSql);

			int start = tableSql.indexOf(KEYWORD_TABLE) + KEYWORD_TABLE.length();
			int end = tableSql.indexOf(" ",start);
			String tableName = tableSql.substring(start, end);
			tableName = tableName.trim();
			metaAddNewTable(con, tableName);
			//System.out.println(tableName);
			
			start = tableSql.indexOf("(");
			end = tableSql.lastIndexOf(")");
			String columnStr = tableSql.substring(start+1, end);
			//System.out.println(columnStr);
			
			String[] columns = columnStr.split(",");
			for (int j=0; j<columns.length; j++){
				String colNameWithType = columns[j].trim();
				int spacePos = colNameWithType.indexOf(" ");
				String colName = colNameWithType.substring(0,spacePos);
				colName = colName.trim();
				String colType = colNameWithType.substring(spacePos, colNameWithType.length());
				colType= colType.trim();
				//System.out.println("'"+colName+"'"+colType+"'");
				metaAddNewColumn(con, tableName, colName, colType);
			}
		}
		
	}
	
	/*
	 *  update metadata when create in table in global schema
	 */
	public static void metaAddNewTable(Connection con, String tableName){
		try {
			Statement stm = con.createStatement();
			String sql = "insert into "+TABLE_SCHEMAS + " values('"+tableName+"','";
			sql +=tableName + " description" + "','";
			sql +="table')";
			//System.out.println(sql);
			
			stm.executeUpdate(sql);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/*
	 *  update metadata when add a column into table in global schema
	 */
	public static void metaAddNewColumn(Connection con, String tableName, String columnName, String columnType){
		try {
			Statement stm = con.createStatement();
			String sql = "insert into "+TABLE_COLUMNS + " values('"+tableName+"','"+columnName+"','";
			sql +=columnName + " description" + "','";
			sql +=columnType+"')";
			
			//System.out.println(sql);
			
			stm.executeUpdate(sql);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/*
	 * execute update query into database 
	 */
	private static void metaUpdate(Connection con, String sql){
		try {
			Statement stm = con.createStatement();
			
			stm.executeUpdate(sql);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/*
	 * check when local admin login
	 */
	public static boolean metaCheckLoginLocalAdmin(Connection con, String localAdminName, String password){
		String whereCond = " admin_name = '"+localAdminName+"' and password = '" + password+"'";
		String[] tables = new String[]{TABLE_LOCAL_ADMINS};
		String[] columns = new String[]{"admin_name"};
		
		String[][] result = getDataArray2DFromDB(con, tables, columns, whereCond);
		if (result==null){
			return false;
		}
		
		return true;
	}

	/*
	 * get local admins 
	 */
	public static String[] metaGetLocalAdmins(Connection con){
		String[] tables = new String[]{TABLE_LOCAL_ADMINS};
		String[] columns = new String[]{"admin_name"};
		
		String[][] temp = getDataArray2DFromDB(con, tables, columns);
		if (temp==null){
			return null;
		}
			
		String[] result = new String[temp.length];
		for (int i=0; i<temp.length; i++){
			result[i] = new String(temp[i][0]);
		}
		return result;
	}
	
	/*
	 *  
	 */
	public static void metaAddLocalAdmin(Connection con, String userName, String userDesc, String password){
		try {
			Statement stm = con.createStatement();
			String sql = "insert into "+TABLE_LOCAL_ADMINS + " values('"+userName+"','"+userDesc+"','";
			sql +=password+"')";
			
			//System.out.println(sql);
			
			stm.executeUpdate(sql);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}	

	/*
	 *  
	 */
	public static void metaDeleteLocalAdmin(Connection con, String userName){
		try {
			Statement stm = con.createStatement();
			String sql = "delete from "+TABLE_LOCAL_ADMINS + " where admin_name = '";
			sql +=userName+"'";
			
			//System.out.println(sql);
			
			stm.executeUpdate(sql);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	///// read meta data for bestpeer from metadatabestpeer
	public static String[][] getDataArray2DFromDB(Connection con, String[] tableNames, String[] columnNames){
		return getDataArray2DFromDB(con, tableNames, columnNames, null);
	}
	
	public static String[][] getDataArray2DFromDB(Connection con, String[] tableNames, String[] columnNames, String whereCond){
		String[][] dataArray2D = null;
		
		Vector<Vector<String>> rows = getDataVectorFromDB(con, tableNames, columnNames, whereCond);
		
		if (rows.size()==0){
			return null;
		}
		
		int rowCount = rows.size();
		int colCount = rows.get(0).size();
		
		dataArray2D = new String[rowCount][colCount];
		
		for (int r=0; r<rowCount; r++){
			
			Vector<String> row = rows.get(r);
			
			for (int c=0; c<colCount; c++){
				
				dataArray2D[r][c] =row.get(c);
				//System.out.print("Assign " + r +" "+ c);				
			}
			
			//System.out.println();
		}
		
		return dataArray2D;
	}
	
	public static Vector<Vector<String>> getDataVectorFromDB(Connection con, String[] tableNames, String[] columnNames){
		return getDataVectorFromDB(con, tableNames, columnNames, null);
	}
	
	public static Vector<Vector<String>> getDataVectorFromDB(Connection con, String[] tableNames, String[] columnNames, String whereCond){
			
		String sql = "SELECT ";
		for (int i=0; i<columnNames.length; i++){
			String seperator = (i==columnNames.length-1) ? "\n" : ", ";
			sql += columnNames[i] + seperator;
		}
		sql += "FROM ";
		for (int i=0; i<tableNames.length; i++){
			String seperator = (i==tableNames.length-1) ? "\n" : ", ";
			sql += tableNames[i] + seperator;
		}
		
		if (whereCond!=null){
			sql += "WHERE " + whereCond;
		}
		
		//System.out.println("GOING to EXECUTE: "+ sql);
		
		Statement st;
		ResultSet rs;
		try {
			st = con.createStatement();
			rs = st.executeQuery(sql);
			
			//DBConnector.printRS(rs);
			
			ResultSetMetaData rsmd = rs.getMetaData();
			int nCol = rsmd.getColumnCount();
			
			Vector<Vector<String>> rows = new Vector<Vector<String>>();
			
			while (rs.next()) {
				Vector<String> row = new Vector<String>();
				for (int j = 0; j < nCol; j++) {
					String item =rs.getString(j + 1);
					row.add(item);
					//System.out.print(item);
				}
				rows.add(row);
				//System.out.println();
			}
			
			return rows;
			
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("---CAN NOT EXECUTE: "+ sql);
		}
		
		return null;
	}
	
	public static String[] metaGetTables(Connection con){
				
		if (con==null){
			return null;
		}
		
		String[] tableNames = {MetaDataAccess.TABLE_SCHEMAS};
		String[] columnNames = {"schema_name"};
		String whereCond = "schema_type = 'table'";
		String[][] data = MetaDataAccess.getDataArray2DFromDB(con, tableNames, columnNames, whereCond);
		if (data==null){
			return null;
		}
		
		String[] result = new String[data.length];
		
	    for (int i=0; i<data.length; i++){
	    	result[i] = data[i][0];
	    }
	    		
		return result;
	}

	public static String[][] metaGetColumns(Connection con, String tableName){
			
		if (con==null){
			return null;
		}
		
		String[] tableNames = {MetaDataAccess.TABLE_COLUMNS};
		String[] columnNames = {"column_name"};
		String whereCond = "schema_name = '"+tableName+"'";
		String[][] result = MetaDataAccess.getDataArray2DFromDB(con, tableNames, columnNames, whereCond);
	    		
		return result;
	}

	public static String[][] metaGetColumnsWithType(Connection con, String tableName){
		
		if (con==null){
			return null;
		}
		
		String[] tableNames = {MetaDataAccess.TABLE_COLUMNS};
		String[] columnNames = {"column_name", "column_type"};
		String whereCond = "schema_name = '"+tableName+"'";
		String[][] result = MetaDataAccess.getDataArray2DFromDB(con, tableNames, columnNames, whereCond);
	    		
		return result;
	}
	public static String[] metaGetRoles(Connection con){
		
		if (con==null){
			return null;
		}
		
		String[] tableNames = {MetaDataAccess.TABLE_ROLES};
		String[] columnNames = {"role_name"};
		String[][] data = MetaDataAccess.getDataArray2DFromDB(con, tableNames, columnNames);
		String[] result = new String[data.length];
		
	    for (int i=0; i<data.length; i++){
	    	result[i] = data[i][0];
	    }
	    		
		return result;
	}

	public static String[][] metaGetUserGrantedRole(Connection con, String userName){
				
		if (con==null){
			return null;
		}
		
		String[] tableNames = {MetaDataAccess.TABLE_USER_ROLE, MetaDataAccess.TABLE_ROLES};
		String[] columnNames = {MetaDataAccess.TABLE_ROLES+".role_name", "role_desc"};
		String whereCond = MetaDataAccess.TABLE_ROLES+".role_name" + " = " +  MetaDataAccess.TABLE_USER_ROLE+".role_name" 
		+ " and " + MetaDataAccess.TABLE_USER_ROLE + ".user_name = '"+userName+"'";
		String[][] data = MetaDataAccess.getDataArray2DFromDB(con, tableNames, columnNames, whereCond);
				
		return data;
	}

	public static String[][] metaGetUserDescPasswd(Connection con, String userName){
		
		if (con==null){
			return null;
		}
		
		String[] tableNames = {MetaDataAccess.TABLE_USERS};
		String[] columnNames = {"user_desc", "password"};
		String whereCond = "user_name = '"+userName+"'";
		
		String[][] data = MetaDataAccess.getDataArray2DFromDB(con, tableNames, columnNames, whereCond);
		
		return data;
	}
	
	public static String[][] metaGetUserGrantedPrivilege(Connection con, String userName){
	
		if (con==null){
			return null;
		}
		String privilege_id = "select";
		
		String[] tableNames = {MetaDataAccess.TABLE_USER_PERM};
		String[] columnNames = {"object", "permission_type"};
		String whereCond = "user_name = '"+userName+"' and " + "lower(privilege_id) = '"+ privilege_id+"'" ;
		
		String[][] data = MetaDataAccess.getDataArray2DFromDB(con, tableNames, columnNames, whereCond);
				
		return data;
	}

	public static String[][] metaGetUserGrantedPrivilegeWithPrivilegeId(Connection con, String userName){
		
		if (con==null){
			return null;
		}
		String privilege_id = "select";
		
		String[] tableNames = {MetaDataAccess.TABLE_USER_PERM};
		String[] columnNames = {"privilege_id", "object", "permission_type"};
		String whereCond = "user_name = '"+userName+"' and " + "lower(privilege_id) = '"+ privilege_id+"'" ;
		
		String[][] data = MetaDataAccess.getDataArray2DFromDB(con, tableNames, columnNames, whereCond);
		
		return data;
	}
	
	///////// reading information of a role

	public static String[][] metaGetRoleGrantedRole(Connection con, String roleName){
		
		if (con==null){
			return null;
		}
		
		String[] tableNames = {MetaDataAccess.TABLE_ROLE_HIER, MetaDataAccess.TABLE_ROLES};
		String[] columnNames = {MetaDataAccess.TABLE_ROLES+".role_name", "role_desc"};
		String whereCond = MetaDataAccess.TABLE_ROLES+".role_name" + " = " +  MetaDataAccess.TABLE_ROLE_HIER+".sub_role_name" 
		+ " and " + MetaDataAccess.TABLE_ROLE_HIER + ".super_role_name = '"+roleName+"'";
		String[][] data = MetaDataAccess.getDataArray2DFromDB(con, tableNames, columnNames, whereCond);
				
		return data;
	}
	
	public static String[][] metaGetRoleDesc(Connection con, String roleName){
		
		if (con==null){
			return null;
		}
		
		String[] tableNames = {MetaDataAccess.TABLE_ROLES};
		String[] columnNames = {"role_desc"};
		String whereCond = "role_name = '"+roleName+"'";
		
		String[][] data = MetaDataAccess.getDataArray2DFromDB(con, tableNames, columnNames, whereCond);
		
		return data;
	}
	
	public static String[][] metaGetRoleGrantedPrivilege(Connection con, String roleName){
	
		if (con==null){
			return null;
		}
		String privilege_id = "select";
		
		String[] tableNames = {MetaDataAccess.TABLE_ROLE_PERM};
		String[] columnNames = {"object", "permission_type"};
		String whereCond = "role_name = '"+roleName+"' and " + "lower(privilege_id) = '"+ privilege_id+"'" ;
		
		String[][] data = MetaDataAccess.getDataArray2DFromDB(con, tableNames, columnNames, whereCond);
				
		return data;
	}

	public static String[][] metaGetRoleGrantedPrivilegeWithPrivilegeId(Connection con, String roleName){
		
		if (con==null){
			return null;
		}
		String privilege_id = "select";
		
		String[] tableNames = {MetaDataAccess.TABLE_ROLE_PERM};
		String[] columnNames = {"privilege_id", "object", "permission_type"};
		String whereCond = "role_name = '"+roleName+"' and " + "lower(privilege_id) = '"+ privilege_id+"'" ;
		
		String[][] data = MetaDataAccess.getDataArray2DFromDB(con, tableNames, columnNames, whereCond);
		
		return data;
	}
	
	/////////
	
	public static String[] metaGetAvailPrivileges(Connection con){
		
		if (con==null){
			return null;
		}
		
		String[] tableNames = {MetaDataAccess.TABLE_PRIVILEGES};
		String[] columnNames = {"privilege_id"};
		String[][] data = MetaDataAccess.getDataArray2DFromDB(con, tableNames, columnNames);
		String[] result = new String[data.length];
		
	    for (int i=0; i<data.length; i++){
	    	result[i] = data[i][0];
	    }
	    		
		return result;
	}

	public static String[][] metaGetRolesWithDescripton(Connection con){
			
		if (con==null){
			return null;
		}
		
		String[] tableNames = {MetaDataAccess.TABLE_ROLES};
		String[] columnNames = {"role_name", "role_desc"};
		String[][] data = MetaDataAccess.getDataArray2DFromDB(con, tableNames, columnNames);
	    		
		return data;
	}
	
	public static String[] metaGetUsers(Connection con){
		
		if (con==null){
			return null;
		}
		
		String[] tableNames = {MetaDataAccess.TABLE_USERS};
		String[] columnNames = {"user_name"};
		String[][] data = MetaDataAccess.getDataArray2DFromDB(con, tableNames, columnNames);
		String[] result = new String[data.length];
		
	    for (int i=0; i<data.length; i++){
	    	result[i] = data[i][0];
	    }
	    		
		return result;
	}

	/*
	 *  add new user
	 */
	public static void metaAddNewUser(Connection con, String userName, String userDesc, String pwd){
		try {
			Statement stm = con.createStatement();
			String sql = "insert into "+TABLE_USERS + " values('"+userName+"','";
			sql +=userDesc+ "','";
			sql +=pwd+"')";
			//System.out.println(sql);
			
			stm.executeUpdate(sql);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	// read and store role meta data
	
	// work with privilege table
	public static String[][] metaGetFullPrivileges(Connection con){
		
		if (con==null){
			return null;
		}
		
		String[] tableNames = {MetaDataAccess.TABLE_PRIVILEGES};
		String[] columnNames = {"*"};
		String[][] data = MetaDataAccess.getDataArray2DFromDB(con, tableNames, columnNames);
	    		
		return data;
	}	
	
	public static void metaStorePrivileges(Connection con, String[][] data) {

		// delete old data first then store new one
		String sql = "delete from " + TABLE_PRIVILEGES;
		metaUpdate(con, sql);
		
		for (int i = 0; i < data.length; i++) {
			String priId = data[i][0];
			String priName = data[i][1];
			String priDesc = data[i][2];
			
			metaInsertPrivilege(con, priId, priName, priDesc);
		}

	}

	public static void metaInsertPrivilege(Connection con, String priId,
			String priName, String priDesc) {
		try {
			Statement stm = con.createStatement();

			String sql = "insert into " + TABLE_PRIVILEGES + " values('"
					+ priId + "','";
			sql += priName + "','";
			sql += priDesc + "')";

			//System.out.println(sql);

			stm.executeUpdate(sql);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}	
	
	// work with role hierarchy
	public static String[][] metaGetFullRoleHierachy(Connection con){
		
		if (con==null){
			return null;
		}
		
		String[] tableNames = {MetaDataAccess.TABLE_ROLE_HIER};
		String[] columnNames = {"*"};
		String[][] data = MetaDataAccess.getDataArray2DFromDB(con, tableNames, columnNames);
	    		
		return data;
	}	
	
	public static void metaStoreRoleHierachy(Connection con, String[][] data) {

		// delete old data first then store new one
		String sql = "delete from " + TABLE_ROLE_HIER;
		metaUpdate(con, sql);
		
		for (int i = 0; i < data.length; i++) {
			String superRole = data[i][0];
			String subRole = data[i][1];
			
			metaInsertRoleHierarchy(con, superRole, subRole);
		}

	}

	public static void metaInsertRoleHierarchy(Connection con, String superRole, String subRole) {
		try {
			Statement stm = con.createStatement();

			String sql = "insert into " + TABLE_ROLE_HIER + " values('"
					+ superRole + "','";
			sql += subRole + "')";

			//System.out.println(sql);

			stm.executeUpdate(sql);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}	
	
	//work with role
	public static String[][] metaGetFullRole(Connection con){
		
		if (con==null){
			return null;
		}
		
		String[] tableNames = {MetaDataAccess.TABLE_ROLES};
		String[] columnNames = {"*"};
		String[][] data = MetaDataAccess.getDataArray2DFromDB(con, tableNames, columnNames);
	    		
		return data;
	}	
	
	public static void metaStoreRole(Connection con, String[][] data) {

		// delete old data first then store new one
		String sql = "delete from " + TABLE_ROLES;
		metaUpdate(con, sql);
		
		for (int i = 0; i < data.length; i++) {
			String role_name = data[i][0];
			String role_desc = data[i][1];
			
			metaInsertRole(con, role_name, role_desc);
		}

	}

	public static void metaInsertRole(Connection con, String role_name, String role_desc) {
		try {
			Statement stm = con.createStatement();

			String sql = "insert into " + TABLE_ROLES + " values('"
					+ role_name + "','";
			sql += role_desc + "')";

			//System.out.println(sql);

			stm.executeUpdate(sql);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}	

	// work with role permission
	public static String[][] metaGetFullRolePermission(Connection con){
		
		if (con==null){
			return null;
		}
		
		String[] tableNames = {MetaDataAccess.TABLE_ROLE_PERM};
		String[] columnNames = {"*"};
		String[][] data = MetaDataAccess.getDataArray2DFromDB(con, tableNames, columnNames);
	    		
		return data;
	}	
	
	public static void metaStoreRolePermission(Connection con, String[][] data) {

		// delete old data first then store new one
		String sql = "delete from " + TABLE_ROLE_PERM;
		metaUpdate(con, sql);
		
		for (int i = 0; i < data.length; i++) {
			String role_name = data[i][0];
			String privilege_id = data[i][1];
			String object = data[i][2];
			String permission_type = data[i][3];
			
			metaInsertRolePermission(con, role_name, privilege_id, object, permission_type);
		}

	}

	public static void metaInsertRolePermission(Connection con, String role_name, String privilege_id, String object, String permission_type) {
		try {
			Statement stm = con.createStatement();

			String sql = "insert into " + TABLE_ROLE_PERM + " values('"
					+ role_name + "','";
			sql += privilege_id + "','";
			sql += object + "','";
			sql += permission_type + "')";

			//System.out.println(sql);

			stm.executeUpdate(sql);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}	

}
