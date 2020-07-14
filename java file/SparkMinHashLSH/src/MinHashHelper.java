import scala.Serializable;

public class MinHashHelper implements Serializable {
	
	private int numOfBuckets = 0; // this is also the number or rows of the characteristic matrix
	private int numOfHash = 0;    // how many permutation did we use so far
    
	public static MinHashHelper getInstance(int characteristicMatrixRowNumber) {
		return new MinHashHelper(characteristicMatrixRowNumber);
	}
	
	public MinHashHelper(int nb) {
		
		numOfBuckets = nb;
		numOfHash = 1;
		
	}
	
	public int[] getPermutation() {
		
		int a = 0;
		int b = 0;
		int[] permutation = new int[numOfBuckets];
		
		/* this is old
		if ( numOfHash == 1 ) {
			a = a1;
			b = b1;
		} else if ( numOfHash == 2 ) {
			a = a2;
			b = b2;
		} else {
			a = a1 + a2*numOfHash;
			b = b1 + b2*numOfHash;
		} */
		
		// a = numOfHash; 
		// b = 1;
		a = numOfHash; 
		b = 1;
				
		// System.out.println("permuation with numOfHash = " + numOfHash + ", a = " + a + ", b = " + b);
		for ( int i = 0; i < numOfBuckets; i ++ ) {
			permutation[i] = (a*i+b) % numOfBuckets;
		}
		
		numOfHash++;
		return permutation;
	}
	
	public void reset() {
		// reset it since this has to generate exactly the same permutation for every document (a set)
		numOfHash = 1;		
	}
}

