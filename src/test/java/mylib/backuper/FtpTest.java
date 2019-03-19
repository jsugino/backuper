package mylib.backuper;

import static mylib.backuper.BackuperTest.createFiles;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import mylib.backuper.DataBase.Storage;

// Test "ftp" using the following command.
//	mvn -Dftp=ftp://user:password@host.name/testdir test

public class FtpTest
{
  @Rule
  public TemporaryFolder tempdir = new TemporaryFolder(new File("target"));

  // ----------------------------------------------------------------------
  // テストプログラム

  @Test
  public void testSimple()
  throws Exception
  {
    if ( !initFTPClient() ) return;
    File root = tempdir.getRoot();
    File dbdir = new File(root,"dic");
    File srcdir = new File(root,"src");
    createFiles(root,new Object[]{
	"dic", new Object[]{
	  DataBase.CONFIGNAME, new String[]{
	    "test.src="+srcdir.getAbsolutePath(),
	    "test.dst="+ftpurl,
	  },
	},
	"src", new Object[]{
	  "a", "aaa",
	  "b", "bbbbb",
	},
      });

    //execute(root,dbdir);

    /*
    compareFtpFiles(new Object[]{
	"a", "aaa",
	"b", "bbbbb",
      });
    */
    createFtpFiles(new Object[]{
	"a", "aaa",
	"d1", new Object[]{
	  "x1", "xxx1",
	  "d2", new Object[]{
	    "x2", "xxx2",
	    "d3", new Object[]{
	      "x3", "xxx3",
	    },
	    "d4", new Object[]{},
	  },
	},
      });

    compareFtpFiles(new Object[]{
	"a", "aaa",
	"d1", new Object[]{
	  "x1", "xxx1",
	  "d2", new Object[]{
	    "x2", "xxx2",
	    "d3", new Object[]{
	      "x3", "xxx3",
	    },
	    "d4", new Object[]{},
	  },
	},
      });
  }

  @Test
  public void testGetPathHolderList()
  throws Exception
  {
    if ( ftpurl == null ) return;
    File root = tempdir.getRoot();
    File dbdir = new File(root,"dic");
    createFiles(root,new Object[]{
	"dic", new Object[]{
	  DataBase.CONFIGNAME, new String[]{
	    "test.dst="+ftpurl,
	  },
	},
      });
    DataBase db = new DataBase(dbdir.toPath());
    Storage sto = db.get("test.dst");
    sto.scanFolder();
    System.out.println("-- dump -- start");
    sto.dump(System.out);
    System.out.println("-- dump -- end");
  }

  // ----------------------------------------------------------------------
  // FTPするための準備

  public static String ftpurl;
  public static String ftphost;
  public static String ftpuser;
  public static String ftppass;
  public static String ftproot;

  @BeforeClass
  public static void setFtpUrl()
  {
    ftpurl = System.getProperty("ftp");
    if ( ftpurl == null ) {
      System.err.println("PLEASE SPECYFY FTP PARAMETER");
      return;
    }
    String args[] = FtpStorage.parseURL(ftpurl);
    ftpuser = args[0];
    ftppass = args[1];
    ftphost = args[2];
    ftproot = args[3];
    System.out.format("ftp parameter : host=%s, user=%s, pass=%s, root=%s",ftphost,ftpuser,ftppass,ftproot).println();
  }

  public static FTPClient ftpclient = null;

  // FTPを使ったテストをする場合は、このメソッドを呼ぶ。
  public static boolean initFTPClient()
  throws IOException
  {
    if ( ftpurl == null ) return false;
    ftpclient = new FTPClient();
    System.out.println("ftp connect");
    ftpclient.connect(ftphost);
    System.out.println("ftp login");
    assertTrue(ftpclient.login(ftpuser,ftppass));
    if ( !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) )
      fail("login failed : "+ftpclient.getReplyCode());
    System.out.println("ftp chdir");
    assertTrue(ftpclient.changeWorkingDirectory(ftproot));
    if ( !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) )
      fail("changeWorkingDirectory failed : "+ftpclient.getReplyCode());
    return true;
  }

  @After
  public void disconnectFTPClient()
  throws IOException
  {
    if ( ftpclient != null ) {
      System.out.println("ftp disconnect");
      ftpclient.disconnect();
    }
    ftpclient = null;
  }

  // ----------------------------------------------------------------------
  // ユーティリティメソッド

  // FTP先との比較
  public static void compareFtpFiles( Object data[] )
  throws IOException
  {
    List<Entry> expect = Entry.walkData(data);
    Map<String,Entry> actual = collectFtpFiles(".");

    expect = expect.stream()
      .filter(exp->{
	  String name = exp.path.toString();
	  Entry act = actual.get(name);
	  if ( act == null ) return true; // means it reamins to expect
	  assertEquals("entry of \""+name+"\"",exp,act);
	  actual.remove(name);
	  return false;
	})
      .collect(Collectors.toList());
    if ( expect.size() > 0 || actual.size() > 0 )
      fail(String.format("different expect=%s, actual=%s",expect,actual));
  }

  // FTP先のファイルを取得する。
  public static Map<String,Entry> collectFtpFiles( String directory )
  throws IOException
  {
    HashMap<String,Entry> actual = new HashMap<>();
    for ( FTPFile file : walkFtp(directory) ) {
      if ( file.isDirectory() ) continue;
      Entry ent = new Entry();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      assertTrue(ftpclient.retrieveFile(file.getName(),out));
      ent.contents = new String(out.toByteArray());
      ent.path = Paths.get(file.getName());
      ent.lastModified = new Date(file.getTimestamp().getTimeInMillis());
      actual.put(ent.path.toString(),ent);
    }
    return actual;
  }

  // FTP先のファイル・フォルダのリストを取得する。
  public static List<FTPFile> walkFtp( String directory )
  throws IOException
  {
    LinkedList<FTPFile> list = new LinkedList<>();
    walkFtp(Paths.get(directory),list);
    return list;
  }

  // FTP先のファイル・フォルダのリストを取得する。
  public static void walkFtp( Path directory, List<FTPFile> list )
  throws IOException
  {
    for ( FTPFile file : ftpclient.mlistDir(directory.toString()) ) {
      String name = file.getName();
      Path full = directory.resolve(name).normalize();
      if ( file.isDirectory() ) {
	if ( name.equals(".") || name.equals("..") ) continue;
	walkFtp(full,list);
      }
      file.setName(full.toString());
      list.add(file);
    }
  }

  // FTP先にファイルを生成する。
  public static void createFtpFiles( Object data[] )
  throws IOException
  {
    clearFtpFiles(".");
    for ( Entry ent : Entry.walkData(data,true) ) {
      if ( ent.contents == null ) {
	assertTrue(ftpclient.makeDirectory(ent.path.toString()));
	if ( !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) ) fail("makeDirectory failed : "+ftpclient.getReplyCode());
      } else {
	ByteArrayInputStream in = new ByteArrayInputStream(ent.contents.getBytes());
	assertTrue(ftpclient.storeFile(ent.path.toString(),in));
	if ( !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) ) fail("storeFile failed : "+ftpclient.getReplyCode());
      }
    }
  }

  // FTP先のファイル・フォルダを削除する。
  public static void clearFtpFiles( String directory )
  throws IOException
  {
    for ( FTPFile file : walkFtp(directory) ) {
      String name = file.getName();
      String full = Paths.get(directory).resolve(name).normalize().toString();
      if ( file.isDirectory() ) {
	System.out.println("removeDirectory "+full);
	assertTrue(ftpclient.removeDirectory(full));
	if ( !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) ) fail("removeDirectory failed : "+ftpclient.getReplyCode());
      } else {
	System.out.println("deleteFile "+full);
	assertTrue(ftpclient.deleteFile(full));
	if ( !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) ) fail("deleteFile failed : "+ftpclient.getReplyCode());
      }
    }
  }

  // ----------------------------------------------------------------------
  // ユーティリティメソッドのテスト

  //@Test
  public void clearFtpFilesTest()
  throws Exception
  {
    if ( !initFTPClient() ) return;
    clearFtpFiles(".");
  }

  @Test
  public void walkDataTest()
  {
    Object data[] = new Object[]{
      "a", "aaa",
      "d", new Object[] {
	"d1", "d1"
      },
      "b", "bbb",
    };
    List<Entry> actual;

    actual = Entry.walkData(data);
    assertEquals(3,actual.size());
    assertEquals(new Entry("a","aaa"),actual.get(0));
    assertEquals(new Entry("d/d1","d1"),actual.get(1));
    assertEquals(new Entry("b","bbb"),actual.get(2));

    actual = Entry.walkData(data,true);
    assertEquals(4,actual.size());
    assertEquals(new Entry("a","aaa"),actual.get(0));
    assertEquals(new Entry("d",1),actual.get(1));
    assertEquals(new Entry("d/d1","d1"),actual.get(2));
    assertEquals(new Entry("b","bbb"),actual.get(3));

    actual = Entry.walkData(data,false);
    assertEquals(4,actual.size());
    assertEquals(new Entry("a","aaa"),actual.get(0));
    assertEquals(new Entry("d/d1","d1"),actual.get(1));
    assertEquals(new Entry("d",1),actual.get(2));
    assertEquals(new Entry("b","bbb"),actual.get(3));
  }

  @Test
  public void testParse()
  {
    String result[] = FtpStorage.parseURL("ftp://comb:test@pass@comb.sakura.ne.jp/my/folder");
    assertEquals(4,result.length);
    assertEquals("comb",result[0]);
    assertEquals("test@pass",result[1]);
    assertEquals("comb.sakura.ne.jp",result[2]);
    assertEquals("my/folder",result[3]);
  }

  @Test
  public void testWalkFtp()
  throws Exception
  {
    if ( !initFTPClient() ) return;
    System.out.println("walkFtp - start");
    walkFtp(".").stream().forEach(f->System.out.println("FTPFile "+f.getName()+" "+f));
    System.out.println("walkFtp - end");
  }

  @Test
  public void pathTest()
  {
    Path path = Paths.get(".");
    assertEquals("a",path.resolve("a").normalize().toString());
  }

}
