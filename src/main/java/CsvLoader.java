import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;

public class CsvLoader {

  public static void main(String[] args) {
    CsvParserSettings settings = new CsvParserSettings();
    settings.getFormat().setLineSeparator("\n");
    CsvParser parser = new CsvParser(settings);
    try {
      parser.beginParsing(getReader("salesrecords1_5m2.csv"));
    } catch (Exception uee) {
      System.out.println(uee);
    }
    String[] row;
    int i = 0;
    ArrayList<ArrayList<String>> arrayOfArrays = new ArrayList<ArrayList<String>>();

    while ((row = parser.parseNext()) != null) {
      ArrayList<String> r = new ArrayList<String>(Arrays.asList(row));
      if (i != 0) {
        arrayOfArrays.add(r);
      }

      if (i++ % 1000 == 0 && i >= 1000) {
        //persist and clear the array
        PersistToRedshift persist = new PersistToRedshift();
        boolean persisted = persist.persist(arrayOfArrays);
        if (persisted) {
          arrayOfArrays.clear();
        }
      }

    }
  }

  public static Reader getReader(String relativePath) throws UnsupportedEncodingException {
    return new InputStreamReader(CsvLoader.class.getResourceAsStream(relativePath), "UTF-8");
  }

}
