package DBuggers;

public class SQLTerm {
public String strTableName;
public String strColumnName;
public String strOperator;
public Object objValue;

	public SQLTerm() {
		
	}
	public SQLTerm(String tableName,String colName, String operator,Object obj) {
		this.strTableName=tableName;
		this.strColumnName=colName;
		this.strOperator=operator;
		this.objValue=obj;
	}
	
}
