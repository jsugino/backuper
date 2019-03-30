package mylib.backuper;

import static mylib.backuper.BackuperTest.FORM;
import static mylib.backuper.BackuperTest.checkContents;
import static mylib.backuper.BackuperTest.checkEvent;
import static mylib.backuper.BackuperTest.compareFiles;
import static mylib.backuper.BackuperTest.createFiles;
import static mylib.backuper.BackuperTest.event;
import static mylib.backuper.BackuperTest.execute;
import static mylib.backuper.BackuperTest.lastModified;
import static mylib.backuper.BackuperTest.line;
import static mylib.backuper.FtpStorage.FTPTIMEFORM;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.junit.Before;
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

  @BeforeClass public static void initLogger() { BackuperTest.initLogger(); }
  @Before public void initEvent() { event.list.clear(); }

  // ----------------------------------------------------------------------
  // テストプログラム

  @Test
  public void testSimple()
  throws Exception
  {
    if ( ftpurl == null ) return;
    File root = tempdir.getRoot();
    File dbdir = new File(root,"dic");
    File srcdir = new File(root,"src");
    long current = System.currentTimeMillis();
    Date current1 = new Date(current-10123L);
    Date current2 = new Date(current-20456L);
    Date current3 = new Date(current-30789L);

    createFiles(root,new Object[]{
	"dic", new Object[]{
	  Main.CONFIGNAME, new String[]{
	    "test.src="+srcdir.getAbsolutePath(),
	    "a",
	    "c1",
	    "test.dst="+ftpurl,
	    "x",
	    "y1",
	  },
	},
	"src", new Object[]{
	  "a", "aa", current1,
	  "b", "bbb", current2,
	  "@l", "b",
	  "@lc", "c",
	  "c", new Object[]{
	    "c1", "ccc111", current3,
	    "c2", "ccc222", current1,
	    "c3", "ccc333", current2,
	    "c4", "ccc444", current3,
	    "d", new Object[]{
	      "d", "ddd", current1,
	      "@lc", "../../c",
	      "@lc1", "../c1",
	      "@lx", "../x",
	    },
	  },
	  "f1", new Object[]{
	    "f2", new Object[]{
	      "f3", new Object[]{},
	    },
	  },
	  "g1", new Object[]{
	    "g2", new Object[]{
	      "g3", new Object[]{
		"g", "gggg", current2,
	      },
	      "g4", new Object[]{},
	    },
	  },
	},
      });

    try ( FTPClient ftpclient = initFTPClient() ) {
      createFtpFiles(ftpclient,new Object[]{
	  "x", "xx",
	  "y", new Object[]{
	    "y1", "y1data", current1,
	    "y2", "y2data", current2,
	  },
	  "c", new Object[]{
	    "c2", "ccc", current1,
	    "c3", "ccc333", current2,
	    "c4", "ccc444", current2,
	  },
	  "z", new Object[]{
	    "za", "za", current1,
	    "z1", new Object[]{
	      "zb", "zbzb", current2,
	      "z2", new Object[]{
		"zc", "zczc", current3,
	      },
	    },
	  },
	});
    }

    DataBase db = execute(root,dbdir);

    checkContents(db.get("test.src")::dump,new String[]{
	".	3",
	line(current2,"b","bbb"),
	"c	1",
	line(current1,"c/c2","ccc222"),
	line(current2,"c/c3","ccc333"),
	line(current3,"c/c4","ccc444"),
	"c/d	3",
	line(current1,"c/d/d","ddd"),
	"f1	0",
	"f1/f2	0",
	"f1/f2/f3	0",
	"g1	0",
	"g1/g2	0",
	"g1/g2/g3	0",
	line(current2,"g1/g2/g3/g","gggg"),
	"g1/g2/g4	0",
      });
    checkContents(db.get("test.dst")::dump,new String[]{
	".	1",
	line(zero(current2),"b","bbb"),
	"c	0",
	line(zero(current1),"c/c2","ccc222"),
	line(zero(current2),"c/c3","ccc333"),
	line(zero(current3),"c/c4","ccc444"),
	"c/d	0",
	line(zero(current1),"c/d/d","ddd"),
	"g1	0",
	"g1/g2	0",
	"g1/g2/g3	0",
	line(zero(current2),"g1/g2/g3/g","gggg"),
	"y	1",
      });

    try ( FTPClient ftpclient = initFTPClient() ) {
      compareFtpFiles(ftpclient,new Object[]{
	  "b", "bbb", zero(current2),
	  "c", new Object[]{
	    "c2", "ccc222", zero(current1),
	    "c3", "ccc333", zero(current2),
	    "c4", "ccc444", zero(current3),
	    "d", new Object[] {
	      "d", "ddd", zero(current1),
	    },
	    // 空のディレクトリは作成されない。
	    /*
	      "f1", new Object[]{
	      "f2", new Object[]{
	      "f3", new Object[]{
	      },
	      },
	      },
	    */
	  },
	  "g1", new Object[]{
	    "g2", new Object[]{
	      "g3", new Object[]{
		"g", "gggg", zero(current2),
	      },
	    },
	  },
	  "x", "xx",
	  "y", new Object[]{
	    "y1", "y1data",
	  },
	});
    }

    checkContents(new File(dbdir,"test.src.db"),new String[]{
	".",
	"CPjgJgxkQYUQzvsrBu7lzQ	*	3	b",
	"c",
	"puUcVpdndkyHnkmklAcKHA	*	6	c2",
	"nZySmjxYljiQjpaNIMXGZg	*	6	c3",
	"vopwnoEeXpfV3zpnf9hM4A	*	6	c4",
	"c/d",
	"d5Y7epMTd61Kta1qnNcYqg	*	3	d",
	"g1/g2/g3",
	"weu0kz4GzlYXSD9mXiZifA	*	4	g",
      });
    checkContents(new File(dbdir,"test.dst.db"),new String[]{
	".",
	"CPjgJgxkQYUQzvsrBu7lzQ	*	3	b",
	"c",
	"puUcVpdndkyHnkmklAcKHA	*	6	c2",
	"nZySmjxYljiQjpaNIMXGZg	*	6	c3",
	"vopwnoEeXpfV3zpnf9hM4A	*	6	c4",
	"c/d",
	"d5Y7epMTd61Kta1qnNcYqg	*	3	d",
	"g1/g2/g3",
	"weu0kz4GzlYXSD9mXiZifA	*	4	g",
      });

    int idx = ftpurl.lastIndexOf('@');
    String ftpstr = "ftp://"+ftpurl.substring(idx+1);
    System.out.println("ftpstr = "+ftpstr);
    checkEvent(new Object[]{
	"Start Backup test.src test.dst",
	"Read DataBase test.src "+dbdir+"/test.src.db", new String[]{
	  "java.nio.file.NoSuchFileException: "+dbdir+"/test.src.db",
	},
	"Scan Folder test.src "+srcdir.getAbsolutePath(), new String[]{
	  "Ignore file a",
	  "Ignore symlink l",
	  "Ignore symlink lc",
	  "Ignore file c/c1",
	  "Ignore symlink c/d/lc",
	  "Ignore symlink c/d/lc1",
	  "Ignore symlink c/d/lx",
	},
	"Read DataBase test.dst "+dbdir+"/test.dst.db", new String[]{
	  "java.nio.file.NoSuchFileException: "+dbdir+"/test.dst.db",
	},
	"Scan Folder test.dst "+ftpstr, new String[]{
	  "Ignore file x",
	  "Ignore file y/y1",
	},
	"Compare Files test.src test.dst", new String[]{
	  "calculate MD5 test.src c/c3",
	  "calculate MD5 test.dst c/c3",
	  "calculate MD5 test.src c/c4",
	  "calculate MD5 test.dst c/c4",
	  "copy b",
	  "copy g1/g2/g3/g",
	  "delete c/c2",
	  "copy c/c2",
	  "set lastModified c/c4",
	  "copy c/d/d",
	  "mkdir c/d",
	  "mkdir g1",
	  "mkdir g1/g2",
	  "mkdir g1/g2/g3",
	  "delete y/y2",
	  "rmdir z",
	  "rmdir z/z1",
	  "rmdir z/z1/z2",
	  "delete z/za",
	  "delete z/z1/zb",
	  "delete z/z1/z2/zc",
	},
	"Write DataBase test.src "+dbdir+"/test.src.db",
	"Write DataBase test.dst "+dbdir+"/test.dst.db",
	"End Backup test.src test.dst",
      });
  }

  @Test
  public void testReverse()
  throws Exception
  {
    if ( ftpurl == null ) return;
    File root = tempdir.getRoot();
    File dbdir = new File(root,"dic");
    File dstdir = new File(root,"dst");
    long current = System.currentTimeMillis()/1000L*1000L;
    Date current1 = new Date(current-10000L);
    Date current2 = new Date(current-20000L);
    Date current3 = new Date(current-30000L);

    createFiles(root,new Object[]{
	"dic", new Object[]{
	  Main.CONFIGNAME, new String[]{
	    "test.src="+ftpurl,
	    "a",
	    "c1",
	    "test.dst="+dstdir.getAbsolutePath(),
	    "x",
	    "y1",
	  },
	},
	"dst", new Object[]{
	  "x", "xx",
	  "@l", "x",
	  "@lc", "y",
	  "y", new Object[]{
	    "y1", "y1data", current1,
	    "y2", "y2data", current2,
	  },
	  "c", new Object[]{
	    "c2", "ccc", current1,
	    "c3", "ccc333", current2,
	    "c4", "ccc444", current2,
	  },
	  "z", new Object[]{
	    "za", "za",  current1,
	    "z1", new Object[]{
	      "@lc", "../../z",
	      "@lc1", "../za",
	      "@lx", "../z1",
	      "zb", "zbzb", current2,
	      "z2", new Object[]{
		"zc", "zczc", current3,
	      },
	    },
	  },
	},
      });

    try ( FTPClient ftpclient = initFTPClient() ) {
      createFtpFiles(ftpclient,new Object[]{
	  "a", "aa", current1,
	  "b", "bbb", current2,
	  "c", new Object[]{
	    "c1", "ccc111", current3,
	    "c2", "ccc222", current1,
	    "c3", "ccc333", current2,
	    "c4", "ccc444", current3,
	    "d", new Object[]{
	      "d", "ddd", current1,
	    },
	  },
	  "f1", new Object[]{
	    "f2", new Object[]{
	      "f3", new Object[]{},
	    },
	  },
	  "g1", new Object[]{
	    "g2", new Object[]{
	      "g3", new Object[]{
		"g", "gggg", current2,
	      },
	      "g4", new Object[]{},
	    },
	  },
	});
    }

    DataBase db = execute(root,dbdir);

    checkContents(db.get("test.src")::dump,new String[]{
	".	1",
	line(current2,"b","bbb"),
	"c	1",
	line(current1,"c/c2","ccc222"),
	line(current2,"c/c3","ccc333"),
	line(current3,"c/c4","ccc444"),
	"c/d	0",
	line(current1,"c/d/d","ddd"),
	"f1	0",
	"f1/f2	0",
	"f1/f2/f3	0",
	"g1	0",
	"g1/g2	0",
	"g1/g2/g3	0",
	line(current2,"g1/g2/g3/g","gggg"),
	"g1/g2/g4	0",
      });
    checkContents(db.get("test.dst")::dump,new String[]{
	".	3",
	line(current2,"b","bbb"),
	"c	0",
	line(current1,"c/c2","ccc222"),
	line(current2,"c/c3","ccc333"),
	line(current3,"c/c4","ccc444"),
	"c/d	0",
	line(current1,"c/d/d","ddd"),
	"g1	0",
	"g1/g2	0",
	"g1/g2/g3	0",
	line(current2,"g1/g2/g3/g","gggg"),
	"y	1",
	"z	0",
	"z/z1	3",
      });

    compareFiles(dstdir,new Object[]{
	"b", "bbb", current2,
	"@l", "x",
	"@lc", "y",
	"c", new Object[]{
	  "c2", "ccc222", current1,
	  "c3", "ccc333", current2,
	  "c4", "ccc444", current3,
	  "d", new Object[] {
	    "d", "ddd", current1,
	  },
	  // 空のディレクトリは作成されない。
	  /*
	  "f1", new Object[]{
	    "f2", new Object[]{
	      "f3", new Object[]{
	      },
	    },
	  },
	  */
	},
	"g1", new Object[]{
	  "g2", new Object[]{
	    "g3", new Object[]{
	      "g", "gggg", current2,
	    },
	  },
	},
	"x", "xx",
	"y", new Object[]{
	  "y1", "y1data",
	},
	"z", new Object[]{
	  "z1", new Object[]{
	    "@lc", "../../z",
	    "@lc1", "../za",
	    "@lx", "../z1",
	  },
	},
      });

    checkContents(new File(dbdir,"test.src.db"),new String[]{
	".",
	"CPjgJgxkQYUQzvsrBu7lzQ	*	3	b",
	"c",
	"puUcVpdndkyHnkmklAcKHA	*	6	c2",
	"nZySmjxYljiQjpaNIMXGZg	*	6	c3",
	"vopwnoEeXpfV3zpnf9hM4A	*	6	c4",
	"c/d",
	"d5Y7epMTd61Kta1qnNcYqg	*	3	d",
	"g1/g2/g3",
	"weu0kz4GzlYXSD9mXiZifA	*	4	g",
      });
    checkContents(new File(dbdir,"test.dst.db"),new String[]{
	".",
	"CPjgJgxkQYUQzvsrBu7lzQ	*	3	b",
	"c",
	"puUcVpdndkyHnkmklAcKHA	*	6	c2",
	"nZySmjxYljiQjpaNIMXGZg	*	6	c3",
	"vopwnoEeXpfV3zpnf9hM4A	*	6	c4",
	"c/d",
	"d5Y7epMTd61Kta1qnNcYqg	*	3	d",
	"g1/g2/g3",
	"weu0kz4GzlYXSD9mXiZifA	*	4	g",
      });

    int idx = ftpurl.lastIndexOf('@');
    String ftpstr = "ftp://"+ftpurl.substring(idx+1);
    System.out.println("ftpstr = "+ftpstr);
    checkEvent(new Object[]{
	"Start Backup test.src test.dst",
	"Read DataBase test.src "+dbdir+"/test.src.db", new String[]{
	  "java.nio.file.NoSuchFileException: "+dbdir+"/test.src.db",
	},
	"Scan Folder test.src "+ftpstr, new String[]{
	  "Ignore file a",
	  "Ignore file c/c1",
	},
	"Read DataBase test.dst "+dbdir+"/test.dst.db", new String[]{
	  "java.nio.file.NoSuchFileException: "+dbdir+"/test.dst.db",
	},
	"Scan Folder test.dst "+dstdir.getAbsolutePath(), new String[]{
	  "Ignore file x",
	  "Ignore file y/y1",
	  "Ignore symlink l",
	  "Ignore symlink lc",
	  "Ignore symlink z/z1/lc",
	  "Ignore symlink z/z1/lc1",
	  "Ignore symlink z/z1/lx",
	},
	"Compare Files test.src test.dst", new String[]{
	  "calculate MD5 test.src c/c3",
	  "calculate MD5 test.dst c/c3",
	  "calculate MD5 test.src c/c4",
	  "calculate MD5 test.dst c/c4",
	  "copy b",
	  "copy g1/g2/g3/g",
	  "delete c/c2",
	  "copy c/c2",
	  "set lastModified c/c4",
	  "copy c/d/d",
	  "mkdir c/d",
	  "mkdir g1",
	  "mkdir g1/g2",
	  "mkdir g1/g2/g3",
	  "delete y/y2",
	  "delete z/za",
	  "delete z/z1/zb",
	  "delete z/z1/z2/zc",
	  "rmdir z/z1/z2",
	},
	"Write DataBase test.src "+dbdir+"/test.src.db",
	"Write DataBase test.dst "+dbdir+"/test.dst.db",
	"End Backup test.src test.dst",
      });
  }

  @Test
  public void testScanRoot()
  throws Exception
  {
    if ( ftpurl == null ) return;
    File root = tempdir.getRoot();
    File dbdir = new File(root,"dic");

    String params[] = FtpStorage.parseURL(ftpurl);
    createFiles(root,new Object[]{
	"dic", new Object[]{
	  Main.CONFIGXML, new String[]{
	    "<database>",
	    "  <storage ftp=\""+params[2]+"\" user=\""+params[0]+"\" password=\""+params[1]+"\" name=\"comb\">",
	    "    <folder dir=\".\" name=\"comb\">",
	    "      <folder dir=\"www/blog\" name=\"blog\">",
	    "        <excludes>",
	    "          .htaccess",
	    "          .htpasswd",
	    "          index.cgi",
	    "          list.txt",
	    "          next.cgi",
	    "        </excludes>",
	    "      </folder>",
	    "    </folder>",
	    "  </storage>",
	    "</database>",
	  },
	},
      });
    try ( DataBase db = new DataBase(dbdir.toPath()) ) {
      db.initializeByXml(dbdir.toPath().resolve(Main.CONFIGXML));
      Iterator<Storage> itr = Main.listDB(db).iterator();
      assertEquals("blog.comb=ftp://"+params[2]+"/www/blog",itr.next().toString());
      assertEquals("comb.comb=ftp://"+params[2]+"/",itr.next().toString());
      assertFalse(itr.hasNext());
    }
  }

  // scanFolder 単体テスト
  //@Test
  public void testGetPathHolderList()
  throws Exception
  {
    if ( ftpurl == null ) return;
    File root = tempdir.getRoot();
    File dbdir = new File(root,"dic");
    createFiles(root,new Object[]{
	"dic", new Object[]{
	  Main.CONFIGNAME, new String[]{
	    "test.dst="+ftpurl,
	  },
	},
      });
    try ( DataBase db = new DataBase(dbdir.toPath()) ) {
      Storage sto = db.get("test.dst");
      sto.scanFolder();
      System.out.println("-- dump -- start");
      sto.dump(System.out);
      System.out.println("-- dump -- end");
    }
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

  public static class FTPClient extends org.apache.commons.net.ftp.FTPClient implements AutoCloseable
  {
    public void close()
    throws IOException
    {
      System.out.println("ftp disconnect");
      this.disconnect();
    }
  }

  public static FTPClient initFTPClient()
  throws IOException
  {
    FTPClient ftpclient = new FTPClient();
    /*
    FTPClientConfig config = new FTPClientConfig();
    config.setServerTimeZoneId("Asia/Tokyo");
    ftpclient.configure(config);
    */
    System.out.println("ftp connect");
    ftpclient.connect(ftphost);
    System.out.println("ftp login");
    boolean result = ftpclient.login(ftpuser,ftppass);
    if ( !result || !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) )
      fail("login failed : result = "+result+", code = "+ftpclient.getReplyCode());
    System.out.println("ftp chdir");
    result = ftpclient.changeWorkingDirectory(ftproot);
    if ( !result || !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) )
      fail("changeWorkingDirectory failed : result = "+result+", code = "+ftpclient.getReplyCode());
    ftpclient.enterLocalPassiveMode();
    if ( !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) )
      fail("changeWorkingDirectory failed : code = "+ftpclient.getReplyCode());

    return ftpclient;
  }

  // ----------------------------------------------------------------------
  // ユーティリティメソッド

  // FTP先との比較
  public static void compareFtpFiles( FTPClient ftpclient, Object data[] )
  throws IOException
  {
    List<Entry> expect = Entry.walkData(data);
    Map<String,Entry> actual = collectFtpFiles(ftpclient,".");

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
      fail(String.format("different expect=%s, actual=%s",expect,actual.values()));
  }

  // FTP先のファイルを取得する。
  public static Map<String,Entry> collectFtpFiles( FTPClient ftpclient, String directory )
  throws IOException
  {
    HashMap<String,Entry> actual = new HashMap<>();
    for ( FTPFile file : walkFtp(ftpclient,directory) ) {
      if ( file.isDirectory() ) continue;
      Entry ent = new Entry();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      boolean result = ftpclient.retrieveFile(file.getName(),out);
      if ( !result || !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) )
	fail("retrieveFile : "+file.getName()
	  +", result = "+result
	  +", code = "+ftpclient.getReplyCode()
	  +", message = "+ftpclient.getReplyString());
      ent.contents = new String(out.toByteArray());
      ent.path = Paths.get(file.getName());
      ent.lastModified = new Date(file.getTimestamp().getTimeInMillis());
      actual.put(ent.path.toString(),ent);
    }
    return actual;
  }

  // FTP先のファイル・フォルダのリストを取得する。
  public static List<FTPFile> walkFtp( FTPClient ftpclient, String directory )
  throws IOException
  {
    LinkedList<FTPFile> list = new LinkedList<>();
    walkFtp(ftpclient,Paths.get(directory),list);
    return list;
  }

  // FTP先のファイル・フォルダのリストを取得する。
  public static void walkFtp( FTPClient ftpclient, Path directory, List<FTPFile> list )
  throws IOException
  {
    FTPFile array[] = ftpclient.mlistDir(directory.toString());
    if ( array == null || !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) )
      fail("mlistDir failed : "+directory
	+", array = "+Arrays.asList(array)
	+", code = "+ftpclient.getReplyCode()
	+", message = "+ftpclient.getReplyString());
    for ( FTPFile file : array ) {
      String name = file.getName();
      Path full = directory.resolve(name).normalize();
      if ( file.isDirectory() ) {
	if ( name.equals(".") || name.equals("..") ) continue;
	walkFtp(ftpclient,full,list);
      }
      file.setName(full.toString());
      list.add(file);
    }
  }

  // FTP先にファイルを生成する。
  public static void createFtpFiles( FTPClient ftpclient, Object data[] )
  throws IOException
  {
    clearFtpFiles(ftpclient,".");
    for ( Entry ent : Entry.walkData(data,true) ) {
      if ( ent.contents == null ) {
	boolean result = ftpclient.makeDirectory(ent.path.toString());
	if ( !result || !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) )
	  fail("makeDirectory failed : path = "+ent.path+", result = "+result+", code = "+ftpclient.getReplyCode());
      } else {
	ByteArrayInputStream in = new ByteArrayInputStream(ent.contents.getBytes());
	boolean result = ftpclient.storeFile(ent.path.toString(),in);
	if ( !result || !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) )
	  fail("storeFile failed  : path = "+ent.path
	    +", result"+result
	    +", code = "+ftpclient.getReplyCode()
	    +", message = "+ftpclient.getReplyString());
	if ( ent.lastModified != null ) {
	  result = ftpclient.setModificationTime(ent.path.toString(),FtpStorage.FTPTIMEFORM.format(ent.lastModified));
	  if ( !result || !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) )
	    fail("setModificationTime failed : path = "+ent.path
	      +", time = "+FtpStorage.FTPTIMEFORM.format(ent.lastModified)
	      +", result = "+result
	      +", code = "+ftpclient.getReplyCode()
	      +", message = "+ftpclient.getReplyString());
	}
      }
    }
  }

  // FTP先のファイル・フォルダを削除する。
  public static void clearFtpFiles( FTPClient ftpclient, String directory )
  throws IOException
  {
    for ( FTPFile file : walkFtp(ftpclient,directory) ) {
      String name = file.getName();
      String full = Paths.get(directory).resolve(name).normalize().toString();
      if ( file.isDirectory() ) {
	System.out.println("removeDirectory "+full);
	boolean result = ftpclient.removeDirectory(full);
	if ( !result || !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) )
	  fail("removeDirectory failed : result = "
	    +result+", code = "
	    +ftpclient.getReplyCode()
	    +", message = "+ftpclient.getReplyString());
      } else {
	System.out.println("deleteFile "+full);
	boolean result = ftpclient.deleteFile(full);
	if ( !result || !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) )
	  fail("deleteFile failed : result = "+result
	    +", code = "+ftpclient.getReplyCode()
	    +", message = "+ftpclient.getReplyString());
      }
    }
  }

  // Date 型のミリ秒部分を削除する。
  public static Date zero( Date orig )
  {
    return new Date(orig.getTime()/1000L*1000L);
  }

  public static String dateftp( File dir, String filename )
  throws IOException
  {
    return BackuperTest.FORM.format(zero(lastModified(dir,filename)));
  }

  /*
  public static String linezero( File dir, String path, String contents )
  throws IOException
  {
    int idx = path.lastIndexOf('/');
    String name = idx < 0 ? path : path.substring(idx+1);
    return BackuperTest.MD5(contents)+'\t'+dateftp(dir,path)+'\t'+contents.length()+'\t'+name;
  }
  */

  // ----------------------------------------------------------------------
  // ユーティリティメソッドのテスト

  //@Test
  public void clearFtpFilesTest()
  throws Exception
  {
    if ( ftpurl == null ) return;
    try ( FTPClient ftpclient = initFTPClient() ) {
      clearFtpFiles(ftpclient,".");
    }
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
    if ( ftpurl == null ) return;
    try ( FTPClient ftpclient = initFTPClient() ) {
      System.out.println("walkFtp - start");
      walkFtp(ftpclient,".").stream().forEach(f->System.out.println("FTPFile "+f.getName()+" "+f));
      System.out.println("walkFtp - end");
    }
  }

  @Test
  public void pathTest()
  {
    Path path = Paths.get(".");
    assertEquals("a",path.resolve("a").normalize().toString());
  }

  /****
   * 時刻更新のテスト
   * テスト結果: (TimeZoneの設定に関係ない)
   *                    コンパネ   emacs+ftp   FTPFile   プログラム
   * コンパネで編集後   その時刻    その時刻    -9時間    その時刻
   * プログラム実行後	 +9時間      +9時間    その時刻    +9時間
   * Locale.ROOT	 +9時間      +9時間    その時刻    +9時間
   * setTimeZone("GMT") その時刻    その時刻    -9時間    その時刻
   ****/
  //@Test
  public void testTime()
  throws IOException
  {
    /*
    System.out.println("time zone for 0 - start");
    Stream.of(TimeZone.getAvailableIDs(0)).forEach(System.out::println);
    System.out.println("time zone for 0 - end");
    */
    if ( ftpurl == null ) return;
    try ( FTPClient ftpclient = initFTPClient() ) {
      System.out.println("testTime - srat");
      for ( FTPFile file : ftpclient.mlistDir("../www/share") ) {
	if ( file.getName().equals("test") ) {
	  System.out.println("file = "+file);
	  System.out.println("orig = "+FORM.format(new Date(file.getTimestamp().getTimeInMillis())));
	  String strtime = FTPTIMEFORM.format(new Date());
	  System.out.println("strtime = "+strtime);
	  boolean result = ftpclient.setModificationTime("../www/share/test",strtime);
	  System.out.println("result = "+result);
	}
      }
      System.out.println("testTime - end");
    }
  }
}
