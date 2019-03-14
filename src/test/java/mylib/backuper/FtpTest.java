package mylib.backuper;

import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import mylib.backuper.DataBase.Storage;

public class FtpTest
{
  @Rule
  public TemporaryFolder tempdir = new TemporaryFolder(new File("target"));

  // Test "ftp" using the following command.
  //	mvn -Dftp=comb:password@comb.sakura.ne.jp test
  @Test
  public void test()
  throws Exception
  {
    String ftpstr = System.getProperty("ftp");
    if ( ftpstr == null ) {
      System.err.println("PLEASE SPECYFY FTP PARAMETER");
      return;
    }
    //FtpStorage.connect();
    File dbdir = tempdir.newFolder("dic");
    File srcdir = tempdir.newFolder("src");
    try ( PrintStream out = new PrintStream(new File(dbdir,DataBase.CONFIGNAME)) ) {
      Arrays.stream(new String[]{
	  "test.src="+srcdir.getAbsolutePath(),
	  "test.dst=ftp://"+ftpstr+"/copytest",})
	.forEach(out::println);
    }

    DataBase db = new DataBase(dbdir.toPath());
    Storage dstStorage = db.get("test.dst");
    System.out.println("root = "+dstStorage.getRoot());
  }
}
