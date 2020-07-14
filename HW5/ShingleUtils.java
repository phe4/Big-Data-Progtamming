import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ShingleUtils implements Serializable {
	
    public static final int DEFAULT_SHINGLE_SIZE = 2;
    public static final int PRIME = 31; // why this is always prime
    
    // normal singles
    
    public static String[] getTextShingles(String text) {
        return getTextShingles(text, DEFAULT_SHINGLE_SIZE);
    }

    public static String[] getTextShingles(String text, int wordsInShingle) {
        if (wordsInShingle <= 0)
            throw new IllegalArgumentException("Shingle size must be bigger that 0");

        String[] words = Objects.requireNonNull(text).split("\\s+");
        int totalShingles = words.length - wordsInShingle + 1;

        if (totalShingles < 1)
            throw new IllegalArgumentException("Text contains not enough words to get even 1 shingle: " + text);

        StringBuilder stringBuffer = new StringBuilder();
        String[] shingles = new String[totalShingles];
        for (int i = 0; i < totalShingles; i++) {
        	
            for (int j = i; j < i + wordsInShingle; j++) {
                stringBuffer.append(words[j]).append(" ");
            }

            stringBuffer.setLength(Math.max(stringBuffer.length() - 1, 0));
            shingles[i] = stringBuffer.toString();
            stringBuffer.setLength(0);
        }
        return shingles;
    }

    public static Iterator<Integer> getHashCodes(Iterator<String> textShingles) {
    	ArrayList<Integer> hashCodes = new ArrayList<Integer>();
    	while (textShingles.hasNext()) {
    		hashCodes.add(textShingles.next().hashCode());
    	}
    	return hashCodes.iterator();
    }
    
    /*
    public static Set<String> getDistinctTextShingles(String text) {
        return new HashSet<>(getTextShingles(text));
    }

    public static Set<String> getDistinctTextShingles(String text, int wordsInShingle) {
        return new HashSet<>(getTextShingles(text, wordsInShingle));
    }
    
    // hashed singles
    
    public static List<Integer> getHashedShingles(String text){
        return getHashedShingles(text, DEFAULT_SHINGLE_SIZE);
    }

    public static List<Integer> getHashedShingles(String text, int wordsInShingle){
        List<String> textShingles = getTextShingles(text, wordsInShingle);

        return textShingles.stream()
                .map(textShingle -> Arrays.asList(textShingle.split("\\s+"))
                                          .stream()
                                          .map(String::hashCode)
                                          .reduce((hash1, hash2) -> hash1 + hash2)
                                          .get())
                .collect(Collectors.toList());
    }

    
    

    public static Set<Integer> getDistinctHashedShingles(String text){
        return new HashSet<>(getHashedShingles(text));
    }

    public Set<Integer> getDistinctHashedShingles(String text, int wordsInShingle){
        return new HashSet<>(getHashedShingles(text, wordsInShingle));
} */
    
}