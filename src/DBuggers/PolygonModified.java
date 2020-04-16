 package DBuggers;
import java.awt.Dimension;
import java.awt.Polygon;
import java.io.Serializable;

public class PolygonModified implements Comparable<PolygonModified>, Serializable {

    public Polygon poly;

    public PolygonModified(Polygon poly){
        this.poly = poly;
    }

    @Override
    public int compareTo(PolygonModified p) {
        Dimension dim = poly.getBounds().getSize();
        int thisArea = dim.width * dim.height;
        dim = p.poly.getBounds().getSize();
        int pArea = dim.width * dim.height;
        return thisArea - pArea;
    }
    
    @Override
    public boolean equals(Object o) {
    	// TODO Auto-generated method stub
    	PolygonModified p = (PolygonModified)o;
    	return this.compareTo(p) == 0;
    }
    @Override
    public String toString() {
    	// TODO Auto-generated method stub
    	StringBuilder s = new StringBuilder("Points: ");
    	int[] x = poly.xpoints, y = poly.ypoints;
    	for (int i = 0; i < y.length; i++) {
			s.append("(" + x[i] + ", " + y[i] + ") ");
		}
    	Dimension dim = poly.getBounds().getSize();
        int thisArea = dim.width * dim.height;
    	s.append("Area: " + thisArea);
    	return s.toString();
    }
}