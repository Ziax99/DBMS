package RTree;

import java.util.Scanner;

public class TestRTree {

	public static void main(String[] args) 
	{
		RTree<Integer> tree = new RTree<Integer>("S",4);
		Scanner sc = new Scanner(System.in);
		while(true) 
		{
			int x = sc.nextInt();
			if(x == -1)
				break;
			tree.insert(x, null);
			System.out.println(tree.toString());
		}
		while(true) 
		{
			int x = sc.nextInt();
			if(x == -1)
				break;
			tree.delete(x,null);
			System.out.println(tree.toString());
		}
		sc.close();
	}	
}