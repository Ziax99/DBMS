package bTree;
import java.io.Serializable;

public class Ref implements Serializable ,Comparable<Ref>{

    /* 
     * SThis class represents a pointer to the record. It is used at the leaves of the B+ tree 
     */
    private static final long serialVersionUID = 1L;
    private String pageAddress; 
    private int indexInPage;

    public Ref(String pageAddress, int indexInPage)
    {
        this.pageAddress = pageAddress;
        this.indexInPage = indexInPage;
    }

    /* 
     * @return the page at which the record is saved on the hard disk
     */
    public String getPage()
    {
        return pageAddress;
    }

    /**
     * @return the index at which the record is saved in the page
     */
    public int getIndexInPage()
    {
        return indexInPage;
    }
    @Override
    public boolean equals(Object obj) {
        Ref ref = (Ref)obj;
        // TODO Auto-generated method stub
        return pageAddress.equals(ref.pageAddress) && indexInPage == ref.indexInPage;
    }
	@Override
	public int compareTo(Ref o) {
		// TODO Auto-generated method stub
		return o.indexInPage-this.indexInPage;
			
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((pageAddress == null) ? 0 : pageAddress.hashCode());
		result = prime * result + (((Integer)indexInPage==null)?0:((Integer)indexInPage).hashCode());
		return result;
	}
	

    @Override
    public String toString() {
    	int x=-1;
    	for(int i=0;i<pageAddress.length();i++) {
    		if(pageAddress.charAt(i)>='0'&&pageAddress.charAt(i)<='9') {
    			x=i;
    			break;
    		}
    	}
        return "{" + pageAddress.substring(x) + ", " + indexInPage + "}";
    }

}