import java.io.BufferedReader;
import java.io.FileReader;


public class MinHashUtils {

	public static String readFile(String fPath) {
		
		String everything = "";

		try {
		
			BufferedReader br = new BufferedReader(new FileReader(fPath));
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();

			while (line != null) {
				sb.append(line);
				sb.append(System.lineSeparator());
				line = br.readLine();
			}
			
			br.close();
			everything += sb.toString();
			
		} catch(Exception e) {
			System.out.println(e.getMessage());
		}
		finally {
			// 
		}
		
		return everything;
		
	}
		
}
