package TrackAction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class ReadFileLine {
    public ArrayList<String> readFileLine(String filename) {
        try {
            InputStream is = CreateTablePartition.class.getClassLoader().getResourceAsStream(filename);
            ArrayList<String> result = new ArrayList<String>();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String line = null;
            while ((line = reader.readLine()) != null) {
                result.add(line);
            }
            return result;
        } catch(IOException e) {
            System.out.println("IOException");
            return null;
        }
    }
}
