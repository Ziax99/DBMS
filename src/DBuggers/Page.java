package DBuggers;

import java.io.Serializable;
import java.util.Collections;
import java.util.Vector;

public class Page implements Serializable {

    Vector<Tuple> records;
    String tableName;
    int pageNumber;

    public Page(String tableName, int pageNumber){
        records = new Vector<>();
        this.tableName = tableName;
        this.pageNumber = pageNumber;
    }
    public Vector<Tuple> getRecords(){
    	return records;
    }

    public int getSize(){
        return records.size();
    }
    public void printPage() {
    	System.out.println(pageNumber);
    	for(Tuple t:records) {
    		System.out.println(t);
    	}
    	System.out.println("----------------------------------");
    	
    }

    public Object[] insert(Tuple newRecord) {
        int low=0;
        int hi=records.size() - 1;
        int mid=-1;
        int pos=-1;
        while(low<=hi) {
            mid= low+hi>>1;
            if(records.get(mid).compareTo(newRecord) >= 0){
                hi=mid-1;
                pos=mid;
            }
            else {
                low=mid+1;
            }
        }
        if (pos==-1)
            pos = getSize();

        records.add(pos, newRecord);
        Object[] res = {pos, (getSize() == DBApp.MaximumRowsCountinPage + 1? records.remove(DBApp.MaximumRowsCountinPage) : null)};
        return res;
    }

    public Tuple getLast(){
        return records.isEmpty()?null:records.lastElement();
    }

    public Tuple getFirst(){
        return records.firstElement();
    }
    
}
