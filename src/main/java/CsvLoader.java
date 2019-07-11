import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CsvLoader {

  public static void main(String[] args) {

    CsvParserSettings settings = new CsvParserSettings();
    settings.getFormat().setLineSeparator("\n");
    CsvParser parser = new CsvParser(settings);
    startParsing(parser);
  }

  private static void startParsing(CsvParser parser) {
    String dirPath = "/home/krishnamrajug/workspace/krishnamg/github/csvloader/src/main/resources/";
    ArrayList<String> files = getFileNamesInDir(dirPath).get();

    Iterator<String> fileNamesIterator = files.iterator();
    while (fileNamesIterator.hasNext()) {
      System.out.println();
      String fileName = fileNamesIterator.next();
      try {
        System.out.println(files.indexOf(fileName)+1 + ":" + fileName);
        for (int u = 0; u < fileName.length(); u++) {
          System.out.print("-");
        }
        System.out.println();
        parser.beginParsing(getReader(fileName));
      } catch (Exception uee) {
        System.out.println(uee);
      }
      String[] row;
      int i = 0;
      ArrayList<ArrayList<String>> arrayOfArrays = new ArrayList<ArrayList<String>>();

      while ((row = parser.parseNext()) != null) {
        ArrayList<String> r = new ArrayList<String>(Arrays.asList(row));

        //gets all files headers
        if (getHeaders(i, r)) {
          parser.stopParsing();
          break;
        }

      /*if (i != 0) {
        arrayOfArrays.add(r);
      }


      if (i++ % 1000 == 0 && i >= 1000) {
        //persist and clear the array
        //persist(arrayOfArrays);
      }*/

      }
    }
  }

  private static boolean getHeaders(int i, ArrayList<String> r) {
    if (i == 0) {
      Iterator it = r.iterator();
      while (it.hasNext()) {
        System.out.print(it.next() + ", ");
      }
      System.out.println();
      return true;
    }
    return false;
  }

  private static void persist(ArrayList<ArrayList<String>> arrayOfArrays) {
    PersistToRedshift persist = new PersistToRedshift();
    boolean persisted = persist.persist(arrayOfArrays);
    if (persisted) {
      arrayOfArrays.clear();
    }
  }

  private static Optional<ArrayList<String>> getFileNamesInDir(String path) {
    Optional<ArrayList<String>> result = Optional.empty();
    try {
      Stream<Path> walk = Files.walk(Paths.get(path));
      List<String> res = walk.filter(Files::isRegularFile)
          .map(x -> {
            String actualPath = x.toString();
            return actualPath.substring(path.length(), actualPath.length());
          }).collect(Collectors.toList());
      Collections.sort(res);
      result = Optional.of(new ArrayList<String>(res));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return result;
  }

  public static Reader getReader(String relativePath) throws UnsupportedEncodingException {
    return new InputStreamReader(CsvLoader.class.getResourceAsStream(relativePath), "UTF-8");
  }

}
