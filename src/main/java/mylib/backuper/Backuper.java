package mylib.backuper;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Backuper
{
  public static void main( String arg[] )
  {
    if ( arg.length < 3 ) { usage(); return; }

    try ( DataBase db = new DataBase(Paths.get(arg[0])) )
    {
      db.readDB(arg[1]);
      db.storageMap.get(arg[1]).dump(System.out);
      db.scanFolder(arg[1]);
      db.storageMap.get(arg[1]).dump(System.out);
      db.writeDB(arg[1]);

      db.readDB(arg[2]);
      db.storageMap.get(arg[2]).dump(System.out);
      db.scanFolder(arg[2]);
      db.storageMap.get(arg[2]).dump(System.out);
      db.writeDB(arg[2]);
    } catch ( Exception ex ) {
      ex.printStackTrace();
    }
  }

  public static void usage()
  {
    System.err.println("usage : java -jar file.jar dbName fromDir toDir");
    System.err.println("    dbName  : specify database file name");
    System.err.println("    fromDir : source folder name");
    System.err.println("    toDir   : destination folder name");
  }
}
