package mylib.backuper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.read.ListAppender;

import mylib.backuper.Backup.Task;
import mylib.backuper.DataBase.Storage;

public class BackuperTest
{
  @Rule
  public TemporaryFolder tempdir = new TemporaryFolder(new File("target"));

  public static ListAppender<ILoggingEvent> event = new ListAppender<>();

  public final static String APPENDERNAME = "FORTEST";

  @BeforeClass
  // ログの内容をチェックするための準備
  public static void initLogger()
  {
    ch.qos.logback.classic.Logger log = (ch.qos.logback.classic.Logger)LoggerFactory.getLogger("root");
    if ( log.getAppender(APPENDERNAME) == null ) {
      System.out.println("add appenter : "+APPENDERNAME);
      event.setName(APPENDERNAME);
      event.setContext(log.getLoggerContext());
      event.start();
      log.addAppender(event);
    }
  }

  @Before
  public void initEvent()
  {
    event.list.clear();
  }

  // ----------------------------------------------------------------------
  // Backuper のテスト

  // 一般的なコピーのテスト
  @Test
  public void testSimple()
  throws IOException
  {
    File root = tempdir.getRoot();
    File dbdir = new File(root,"dic");
    File srcdir = new File(root,"src");
    File dstdir = new File(root,"dst");
    Date current = new Date(System.currentTimeMillis() - 10000L);
    Date next = new Date(current.getTime()+3333L);
    createFiles(root,new Object[]{
	"dic", new Object[]{
	  Main.CONFIGNAME, new String[]{
	    "test.src="+srcdir.getAbsolutePath(),
	    "a",
	    "c1",
	    "test.dst="+dstdir.getAbsolutePath(),
	    "x",
	    "y1",
	  },
	},
	"src", new Object[]{
	  "a", "aa",
	  "b", "bbb",
	  "@l", "b",
	  "@lc", "c",
	  "c", new Object[]{
	    "c1", "ccc111",
	    "c2", "ccc222",
	    "c3", "ccc333", current,
	    "c4", "ccc444", current,
	    "d", new Object[]{
	      "d", "ddd",
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
		"g", "gggg",
	      },
	      "g4", new Object[]{},
	    },
	  },
	},
	"dst", new Object[]{
	  "x", "xx",
	  "y", new Object[]{
	    "y1", "y1data",
	    "y2", "y2data",
	  },
	  "c", new Object[]{
	    "c2", "ccc",
	    "c3", "ccc333", current,
	    "c4", "ccc444", next,
	  },
	  "z", new Object[]{
	    "za", "za",
	    "z1", new Object[]{
	      "zb", "zbzb",
	      "z2", new Object[]{
		"zc", "zczc",
	      },
	    },
	  },
	},
      });

    DataBase db = execute(root,dbdir);

    checkContents(db.get("test.src")::dump,new String[]{
	".	3",
	"*	3	b",
	"c	1",
	"*	6	c2",
	"*	6	c3",
	"*	6	c4",
	"c/d	3",
	"*	3	d",
	"f1	0",
	"f1/f2	0",
	"f1/f2/f3	0",
	"g1	0",
	"g1/g2	0",
	"g1/g2/g3	0",
	"*	4	g",
	"g1/g2/g4	0",
      });

    checkContents(new File(dbdir,"test.src.db"),new String[]{
	".",
	line(srcdir,"b","bbb"),
	"c",
	line(srcdir,"c/c2","ccc222"),
	line(current,"c/c3","ccc333"),
	line(current,"c/c4","ccc444"),
	"c/d",
	line(srcdir,"c/d/d","ddd"),
	"g1/g2/g3",
	line(srcdir,"g1/g2/g3/g","gggg"),
      });
    checkContents(new File(dbdir,"test.dst.db"),new String[]{
	".",
	line(dstdir,"b","bbb"),
	"c",
	line(dstdir,"c/c2","ccc222"),
	line(current,"c/c3","ccc333"),
	line(current,"c/c4","ccc444"),
	"c/d",
	line(dstdir,"c/d/d","ddd"),
	"g1/g2/g3",
	line(dstdir,"g1/g2/g3/g","gggg"),
      });

    compareFiles(srcdir,new Object[]{
	"a", "aa",
	"b", "bbb",
	"@l", "b",
	"@lc", "c",
	"c", new Object[]{
	  "c1", "ccc111",
	  "c2", "ccc222",
	  "c3", "ccc333", current,
	  "c4", "ccc444", current,
	  "d", new Object[]{
	    "d", "ddd",
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
	      "g", "gggg",
	    },
	    "g4", new Object[]{},
	  },
	},
      });

    compareFiles(dstdir,new Object[]{
	"b", "bbb", lastModified(root,"src/b"),
	"c", new Object[]{
	  "c2", "ccc222", lastModified(root,"src/c/c2"),
	  "c3", "ccc333", lastModified(root,"src/c/c3"),
	  "c4", "ccc444", lastModified(root,"src/c/c4"),
	  "d", new Object[] {
	    "d", "ddd", lastModified(root,"src/c/d/d"),
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
	      "g", "gggg",
	    },
	  },
	},
	"x", "xx",
	"y", new Object[]{
	  "y1", "y1data",
	},
      });

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
	"Scan Folder test.dst "+dstdir.getAbsolutePath(), new String[]{
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

  // 同じ名前で、コピー元のディレクトリとコピー先ファイルがある場合のテスト
  @Test
  public void testSame()
  throws Exception
  {
    File root = tempdir.getRoot();
    File dbdir = new File(root,"dic");
    File srcdir = new File(root,"src");
    File dstdir = new File(root,"dst");
    Date current = new Date(System.currentTimeMillis() - 10000L);
    createFiles(root,new Object[]{
	"dic", new Object[]{
	  Main.CONFIGNAME, new String[]{
	    "test.src="+srcdir.getAbsolutePath(),
	    "test.dst="+dstdir.getAbsolutePath(),
	  },
	},
	"src", new Object[]{
	  "1", "1", current,
	  "2", "2", current,
	  "4", new Object[]{
	    "1", "4/1", current,
	  },
	  "5", "5", current,
	  "6", new Object[]{
	    "1", "6/1", current,
	    "2", "6/2", current,
	  },
	  "dir", new Object[] {
	    "a", "aaa",
	    "x", new Object[] {
	      "x1", "x/x1", current,
	    },
	    "y", "y", current,
	    "z", "z", current,
	  },
	},
	"dst", new Object[]{
	  "2", "2", current,
	  "3", "3", current,
	  "4", "4", current,
	  "5", new Object[]{
	    "1", "5/1", current,
	  },
	  "6", new Object[]{
	    "2", "6/2", current,
	    "3", "6/3", current,
	  },
	  "dir", new Object[] {
	    "b", "bbb",
	    "@x", "2",
	    "@y", "3",
	    "z", new Object[] {
	      "c", "ccc",
	      "@z1", "../4",
	    },
	  },
	},
      });

    DataBase db = execute(root,dbdir);

    compareFiles(dstdir,new Object[]{
	"1", "1", current,
	"2", "2", current,
	"4", new Object[]{
	  "1", "4/1", current,
	},
	"5", "5", current,
	"6", new Object[]{
	  "1", "6/1", current,
	  "2", "6/2", current,
	},
	"dir", new Object[] {
	  "a", "aaa",
	  "@x", "2",
	  "@y", "3",
	  "z", new Object[] {
	    "@z1", "../4",
	  },
	},
      });

    checkEvent(new Object[]{
	"Start Backup test.src test.dst",
	"Read DataBase test.src "+dbdir+"/test.src.db", new String[]{
	  "java.nio.file.NoSuchFileException: "+dbdir+"/test.src.db",
	},
	"Scan Folder test.src "+srcdir.getAbsolutePath(), new String[]{
	},
	"Read DataBase test.dst "+dbdir+"/test.dst.db", new String[]{
	  "java.nio.file.NoSuchFileException: "+dbdir+"/test.dst.db",
	},
	"Scan Folder test.dst "+dstdir.getAbsolutePath(), new String[]{
	  "Ignore symlink dir/x",
	  "Ignore symlink dir/y",
	  "Ignore symlink dir/z/z1",
	},
	"Compare Files test.src test.dst", new String[]{
	  "copy 1",
	  "delete 3",
	  "delete 4",
	  "mkdir 4",
	  "copy 4/1",
	  "delete 5/1",
	  "rmdir 5",
	  "copy 5",
	  "copy 6/1",
	  "delete 6/3",
	  "copy dir/a",
	  "delete dir/b",
	  "delete dir/z/c",
	  "calculate MD5 test.src 2",
	  "calculate MD5 test.src 6/2",
	  "calculate MD5 test.dst 2",
	  "calculate MD5 test.dst 6/2",
	  "CANNOT COPY dir/x/x1",
	  "CANNOT COPY dir/y",
	  "CANNOT COPY dir/z",
	},
	"Write DataBase test.src "+dbdir+"/test.src.db",
	"Write DataBase test.dst "+dbdir+"/test.dst.db",
	"End Backup test.src test.dst",
      });
  }

  // ルートディレクトリにファイルがない場合、Scanせずにコピー実行した場合。
  @Test
  public void testNoRoot()
  throws Exception
  {
    File root = tempdir.getRoot();
    File dbdir = new File(root,"dic");
    File srcdir = new File(root,"src");
    File dstdir = new File(root,"dst");
    Date current = new Date(System.currentTimeMillis() - 10000L);
    createFiles(root,new Object[]{
	"dic", new Object[]{
	  Main.CONFIGNAME, new String[]{
	    "test.src="+srcdir.getAbsolutePath(),
	    "test.dst="+dstdir.getAbsolutePath(),
	  },
	},
	"src", new Object[]{
	  "1", "1", current,
	  "2", "2",
	  "3", new Object[]{
	    "1", "3/1",
	    "2", "3/2",
	    "4", new Object[]{
	      "1", "3/4/1", current,
	    },
	  },
	},
	"dst", new Object[]{
	  "3", new Object[]{
	    "4", new Object[]{
	      "1", "3/4/1", current,
	    }
	  },
	},
      });

    try ( DataBase db = new DataBase(dbdir.toPath()) ) {
      db.initializeByFile(dbdir.toPath().resolve(Main.CONFIGNAME));
      DataBase.Storage srcStorage = db.get("test.src");
      DataBase.Storage dstStorage = db.get("test.dst");
      Main.refresh(srcStorage);
      Main.refresh(dstStorage);
    }

    try ( DataBase db = new DataBase(dbdir.toPath()) ) {
      db.initializeByFile(dbdir.toPath().resolve(Main.CONFIGNAME));
      Main.exCommand = Main.Command.BACKUP_SKIPSCAN;
      DataBase.Storage srcStorage = db.get("test.src");
      DataBase.Storage dstStorage = db.get("test.dst");
      Main.backup(srcStorage,dstStorage);
      Main.exCommand = Main.Command.BACKUP_OR_SCANONLY;
    }

    compareFiles(dstdir,new Object[]{
	"1", "1", current,
	"2", "2",
	"3", new Object[]{
	  "1", "3/1",
	  "2", "3/2",
	  "4", new Object[]{
	    "1", "3/4/1", current,
	  },
	},
      });
  }

  // 長さが異なる場合。
  @Test
  public void testLength()
  throws Exception
  {
    File root = tempdir.getRoot();
    File dbdir = new File(root,"dic");
    File srcdir = new File(root,"src");
    File dstdir = new File(root,"dst");
    Date current = new Date(System.currentTimeMillis() - 10000L);

    createFiles(root,new Object[]{
	"dic", new Object[]{
	  Main.CONFIGNAME, new String[]{
	    "test.src="+srcdir.getAbsolutePath(),
	    "test.dst="+dstdir.getAbsolutePath(),
	  },
	},
	"src", new Object[]{
	  "1", "1", current,
	  "2", "22", current,
	  "3", "333", current,
	},
	"dst", new Object[]{
	  "1", "1", current,
	  "2", "222", current,
	  "3", "333", current,
	},
      });

    DataBase db = execute(root,dbdir);

    compareFiles(dstdir,new Object[]{
	"1", "1", current,
	"2", "22", current,
	"3", "333", current,
      });

    checkEvent(new Object[]{
	"Compare Files test.src test.dst", new String[]{
	  "calculate MD5 test.src 1",
	  "calculate MD5 test.src 3",
	  "calculate MD5 test.dst 1",
	  "calculate MD5 test.dst 3",
	  "delete 2",
	  "copy 2",
	},
	"Write DataBase test.src "+dbdir+"/test.src.db",
      });
  }

  final long min = 60000L;

  // ディレクトリ内のファイル移動があった場合のテスト：単純コピー。
  @Test
  public void testMove()
  throws Exception
  {
    File root = tempdir.getRoot();
    File dbdir = new File(root,"dic");
    File srcdir = new File(root,"src");
    File dstdir = new File(root,"dst");
    Long current = System.currentTimeMillis() - 10000L;

    // prepareMove()
    createFiles(root,new Object[]{
	"dic", new Object[]{
	  Main.CONFIGNAME, new String[]{
	    "test.src="+srcdir.getAbsolutePath(),
	    "test.dst="+dstdir.getAbsolutePath(),
	  },
	},
	"src", new Object[]{
	  "1", new Object[]{
	    "1", "data 11", current,
	    "2", "data 2222", current,
	    "3", "data 333333", current,
	  },
	},
	"dst", new Object[]{
	  "2", new Object[]{
	    "1", "data 11", current, // 内容も時刻も同じ
	    "2", "data 1212", current, // 時刻は同じだが、内容は異なる
	    "3", "data 333333", current+1*min, // 時刻は違うが、内容は同じ
	  },
	  "3", new Object[]{
	    "0", "", current+2*min,
	  },
	},
      });

    DataBase db = execute(root,dbdir);

    compareFiles(dstdir,new Object[]{
	"1", new Object[]{
	  "1", "data 11", current,
	  "2", "data 2222", current,
	  "3", "data 333333", current,
	},
      });

    checkEvent(new Object[]{
	"Compare Files test.src test.dst", new String[]{
	  // 今は、次のようになってしまう。
	  "mkdir 1",
	  "copy 1/1",
	  "copy 1/2",
	  "copy 1/3",
	  "delete 2/1",
	  "delete 2/2",
	  "delete 2/3",
	  "rmdir 2",
	  "delete 3/0",
	  "rmdir 3",
	  // ToDo: 本来は、次のようになるべき。
	  /*
	  "mkdir 1",
	  "move 2/1 1/1",
	  "delete 2/2",
	  "copy 1/2",
	  "move 2/3 1/3",
	  "set lastModified 1/3",
	  */
	},
	"Write DataBase test.src "+dbdir+"/test.src.db",
      });
  }

  @Test
  public void testBackup1()
  throws Exception
  {
    File root = tempdir.getRoot();
    File dbdir = new File(root,"dic");
    File srcdir = new File(root,"src");
    File dstdir = new File(root,"dst");
    File hisdir = new File(root,"his");
    long current = System.currentTimeMillis() - 10000L;

    // prepareSimple()
    createFiles(root,new Object[]{
	"dic", new Object[]{
	  Main.CONFIGXML, new String[]{
	    "<database>",
	    "  <storage dir=\""+tempdir.getRoot()+"\" name=\"src\">",
	    "    <folder dir=\"src\" name=\"test\"/>",
	    "  </storage>",
	    "  <storage dir=\""+tempdir.getRoot()+"\" name=\"dst\">",
	    "    <folder dir=\"dst\" name=\"test\"/>",
	    "    <folder dir=\"his\" name=\"hist\"/>",
	    "  </storage>",
	    "  <backup name=\"test\">",
	    "    <original storage=\"src\" />",
	    "    <copy level=\"daily\" storage=\"dst\" history=\"hist\" />",
	    "  </backup>",
	    "</database>",
	  },
	},
	"src", new Object[]{
	  "1", "", current,
	  "2", "", current,
	  "4", new Object[]{
	    "1", "", current,
	  },
	  "5", "", current,
	  "6", new Object[]{
	    "1", "", current,
	    "2", "abc", current,
	  },
	},
	"dst", new Object[]{
	  "2", "", current,
	  "3", "", current+1*min,
	  "4", "", current+2*min,
	  "5", new Object[]{
	    "1", "", current+3*min,
	  },
	  "6", new Object[]{
	    "2", "def", current+4*min,
	    "3", "", current+5*min,
	  },
	},
	"his", new Object[]{},
      });

    DataBase db = execute(root,dbdir,true);

    compareFiles(dstdir,new Object[]{
	"1", "", current,
	"2", "", current,
	"4", new Object[]{
	  "1", "", current,
	},
	"5", "", current,
	"6", new Object[]{
	  "1", "", current,
	  "2", "abc", current,
	},
      });

    compareFiles(hisdir,new Object[]{
	"3-"+hist(current+1*min), "", current+1*min,
	"4-"+hist(current+2*min), "", current+2*min,
	"5", new Object[]{
	  "1-"+hist(current+3*min), "", current+3*min,
	},
	"6", new Object[]{
	  "2-"+hist(current+4*min), "def", current+4*min,
	  "3-"+hist(current+5*min), "", current+5*min,
	},
      });

    checkEvent(new Object[]{
	"Compare Files test.src test.dst",new String[]{
	  "calculate MD5 test.src 2",
	  "calculate MD5 test.src 6/2",
	  "calculate MD5 test.dst 2",
	  "calculate MD5 test.dst 6/2",
	  "copy 1",
	  "move 3 hist.dst 3-"+hist(current+1*min),
	  "move 4 hist.dst 4-"+hist(current+2*min),
	  "mkdir 4",
	  "copy 4/1",
	  "mkdir hist.dst 5",
	  "move 5/1 hist.dst 5/1-"+hist(current+3*min),
	  "rmdir 5",
	  "copy 5",
	  "copy 6/1",
	  "mkdir hist.dst 6",
	  "move 6/2 hist.dst 6/2-"+hist(current+4*min),
	  "copy 6/2",
	  "move 6/3 hist.dst 6/3-"+hist(current+5*min),
	},
	"Write DataBase test.src "+dbdir+"/test.src.db",
      });
  }

  public static String hist( long time )
  {
    return DataBase.TFORM.format(new Date(time));
  }

  @Test
  public void testBackup2()
  throws Exception
  {
    File root = tempdir.getRoot();
    File dbdir = new File(root,"dic");
    File srcdir = new File(root,"src");
    File dstdir = new File(root,"dst");
    File hisdir = new File(root,"his");
    long current = System.currentTimeMillis()/min*min - 3*min;

    // prepareMove()
    createFiles(root,new Object[]{
	"dic", new Object[]{
	  Main.CONFIGXML, new String[]{
	    "<database>",
	    "  <storage dir=\""+tempdir.getRoot()+"\" name=\"src\">",
	    "    <folder dir=\"src\" name=\"test\"/>",
	    "  </storage>",
	    "  <storage dir=\""+tempdir.getRoot()+"\" name=\"dst\">",
	    "    <folder dir=\"dst\" name=\"test\"/>",
	    "    <folder dir=\"his\" name=\"hist\"/>",
	    "  </storage>",
	    "  <backup name=\"test\">",
	    "    <original storage=\"src\" />",
	    "    <copy level=\"daily\" storage=\"dst\" history=\"hist\" />",
	    "  </backup>",
	    "</database>",
	  },
	},
	"src", new Object[]{
	  "1", new Object[]{
	    "1", "data 11", current,
	    "2", "data 2222", current,
	    "3", "data 333333", current,
	  },
	},
	"dst", new Object[]{
	  "2", new Object[]{
	    "1", "data 11", current, // 内容も時刻も同じ
	    "2", "data 1212", current, // 時刻は同じだが、内容は異なる
	    "3", "data 333333", current+1*min, // 時刻は違うが、内容は同じ
	  },
	  "3", new Object[]{
	    "0", "", current+2*min,
	  },
	},
	"his", new Object[]{},
      });

    DataBase db = execute(root,dbdir,true);

    compareFiles(dstdir,new Object[]{
	"1", new Object[] {
	  "1", "data 11",
	  "2", "data 2222",
	  "3", "data 333333",
	},
      });

    // ToDo: 別フォルダへの移動にも対応したら、次のがOKになる。
    /*
    compareFiles(hisdir,new Object[]{
	"2", new Object[]{
	  "2-"+hist(current), "data 1212", current,
	},
	"3", new Object[]{
	  "0-"+hist(current+2*min), "", current+2*min,
	},
      });
    */
  }

  @Test
  public void testBackup3()
  throws Exception
  {
    File root = tempdir.getRoot();
    File dbdir = new File(root,"dic");
    File srcdir = new File(root,"src");
    File dstdir = new File(root,"dst");
    File hisdir = new File(root,"his");
    long current = System.currentTimeMillis()/min*min - 3*min;

    createFiles(root,new Object[]{
	"dic", new Object[]{
	  Main.CONFIGXML, new String[]{
	    "<database>",
	    "  <storage dir=\""+tempdir.getRoot()+"\" name=\"src\">",
	    "    <folder dir=\"src\" name=\"test\"/>",
	    "  </storage>",
	    "  <storage dir=\""+tempdir.getRoot()+"\" name=\"dst\">",
	    "    <folder dir=\"dst\" name=\"test\"/>",
	    "    <folder dir=\"his\" name=\"hist\"/>",
	    "  </storage>",
	    "  <backup name=\"test\">",
	    "    <original storage=\"src\" />",
	    "    <copy level=\"daily\" storage=\"dst\" history=\"hist\" />",
	    "  </backup>",
	    "</database>",
	  },
	},
	"src", new Object[]{
	  "1.ext", "a/1.ext", current,
	},
	"dst", new Object[]{
	  "abc", "", current+1*min,
	  "def.ext", "", current+2*min,
	  "gh.ext.ext", "", current+3*min,
	  ".ijk", "", current+4*min,
	  ".lmn.ext", "", current+5*min,
	},
	"his", new Object[]{},
      });

    DataBase db = execute(root,dbdir,true);

    compareFiles(dstdir,new Object[]{
	"1.ext", "a/1.ext",
      });
    compareFiles(hisdir,new String[]{
	"abc-"+hist(current+1*min), "",
	"def-"+hist(current+2*min)+".ext", "",
	"gh.ext-"+hist(current+3*min)+".ext", "",
	".ijk-"+hist(current+4*min), "",
	".lmn-"+hist(current+5*min)+".ext", "",
      });
  }

  @Test
  public void testReject()
  throws Exception
  {
    File root = tempdir.getRoot();
    File dbdir = new File(root,"dic");
    File srcdir = new File(root,"src");
    long current = System.currentTimeMillis() - 10000L;

    createFiles(root,new Object[]{
	"src", new Object[]{
	  "dirx", new Object[]{
	    "fileax1", "data ax1", current,
	    "fileax2", "data ax2", current,
	    "diry", new Object[]{
	      "fileaxy1", "data axy1", current,
	      "fileaxy2", "data axy2", current,
	    },
	  },
	  "diry", new Object[]{
	    "fileay1", "data ay1", current,
	    "fileay2", "data ay2", current,
	  },
	  "dirz", new Object[]{
	    "fileaz1", "data az1", current,
	    "fileaz2", "data az2", current,
	    "dirx", new Object[]{
	      "fileazx1", "data azx1", current,
	      "fileazx2", "data azx2", current,
	      "dirx", new Object[]{
		"fileazxx1", "data azxx1", current,
		"fileazxx2", "data azxx2", current,
		"diry", new Object[]{
		  "fileazxxy1", "data azxxy1", current,
		  "fileazxxy2", "data azxxy2", current,
		},
	      },
	    },
	  },
	},
      });

    // --------------------
    createFiles(root,new Object[]{
	"dic", new Object[]{
	  Main.CONFIGNAME, new String[]{
	    "test.src="+srcdir.getAbsolutePath(),
	  },
	},
      });

    try ( DataBase db = new DataBase(dbdir.toPath()) ) {
      db.initializeByFile(dbdir.toPath().resolve(Main.CONFIGNAME));
      DataBase.Storage storage = db.get("test.src");
      storage.scanFolder();
      checkContents(storage::dump, new String[]{
	  ".	*",
	  "dirx	*",
	  "*	fileax1",
	  "*	fileax2",
	  "dirx/diry	*",
	  "*	fileaxy1",
	  "*	fileaxy2",
	  "diry	*",
	  "*	fileay1",
	  "*	fileay2",
	  "dirz	*",
	  "*	fileaz1",
	  "*	fileaz2",
	  "dirz/dirx	*",
	  "*	fileazx1",
	  "*	fileazx2",
	  "dirz/dirx/dirx	*",
	  "*	fileazxx1",
	  "*	fileazxx2",
	  "dirz/dirx/dirx/diry	*",
	  "*	fileazxxy1",
	  "*	fileazxxy2",
	});
    }

    // --------------------
    createFiles(root,new Object[]{
	"dic", new Object[]{
	  Main.CONFIGNAME, new String[]{
	    "test.src="+srcdir.getAbsolutePath(),
	    "/diry/",
	  },
	},
      });

    try ( DataBase db = new DataBase(dbdir.toPath()) ) {
      db.initializeByFile(dbdir.toPath().resolve(Main.CONFIGNAME));
      DataBase.Storage storage = db.get("test.src");
      storage.scanFolder();
      checkContents(storage::dump, new String[]{
	  ".	*",
	  "dirx	*",
	  "*	fileax1",
	  "*	fileax2",
	  "dirx/diry	*",
	  "*	fileaxy1",
	  "*	fileaxy2",
	  //"diry	*",
	  //"*	fileay1",
	  //"*	fileay2",
	  "dirz	*",
	  "*	fileaz1",
	  "*	fileaz2",
	  "dirz/dirx	*",
	  "*	fileazx1",
	  "*	fileazx2",
	  "dirz/dirx/dirx	*",
	  "*	fileazxx1",
	  "*	fileazxx2",
	  "dirz/dirx/dirx/diry	*",
	  "*	fileazxxy1",
	  "*	fileazxxy2",
	});
    }

    // --------------------
    createFiles(root,new Object[]{
	"dic", new Object[]{
	  Main.CONFIGNAME, new String[]{
	    "test.src="+srcdir.getAbsolutePath(),
	    "/dirx/diry/",
	  },
	},
      });

    try ( DataBase db = new DataBase(dbdir.toPath()) ) {
      db.initializeByFile(dbdir.toPath().resolve(Main.CONFIGNAME));
      DataBase.Storage storage = db.get("test.src");
      storage.scanFolder();
      checkContents(storage::dump, new String[]{
	  ".	*",
	  "dirx	*",
	  "*	fileax1",
	  "*	fileax2",
	  //"dirx/diry	*",
	  //"*	fileaxy1",
	  //"*	fileaxy2",
	  "diry	*",
	  "*	fileay1",
	  "*	fileay2",
	  "dirz	*",
	  "*	fileaz1",
	  "*	fileaz2",
	  "dirz/dirx	*",
	  "*	fileazx1",
	  "*	fileazx2",
	  "dirz/dirx/dirx	*",
	  "*	fileazxx1",
	  "*	fileazxx2",
	  "dirz/dirx/dirx/diry	*",
	  "*	fileazxxy1",
	  "*	fileazxxy2",
	});
    }

    // --------------------
    createFiles(root,new Object[]{
	"dic", new Object[]{
	  Main.CONFIGNAME, new String[]{
	    "test.src="+srcdir.getAbsolutePath(),
	    "diry/",
	  },
	},
      });

    try ( DataBase db = new DataBase(dbdir.toPath()) ) {
      db.initializeByFile(dbdir.toPath().resolve(Main.CONFIGNAME));
      DataBase.Storage storage = db.get("test.src");
      storage.scanFolder();
      checkContents(storage::dump, new String[]{
	  ".	*",
	  "dirx	*",
	  "*	fileax1",
	  "*	fileax2",
	  //"dirx/diry	*",
	  //"*	fileaxy1",
	  //"*	fileaxy2",
	  //"diry	*",
	  //"*	fileay1",
	  //"*	fileay2",
	  "dirz	*",
	  "*	fileaz1",
	  "*	fileaz2",
	  "dirz/dirx	*",
	  "*	fileazx1",
	  "*	fileazx2",
	  "dirz/dirx/dirx	*",
	  "*	fileazxx1",
	  "*	fileazxx2",
	  //"dirz/dirx/dirx/diry	*",
	  //"*	fileazxxy1",
	  //"*	fileazxxy2",
	});
    }

    // --------------------
    createFiles(root,new Object[]{
	"dic", new Object[]{
	  Main.CONFIGNAME, new String[]{
	    "test.src="+srcdir.getAbsolutePath(),
	    "file*x1",
	  },
	},
      });

    try ( DataBase db = new DataBase(dbdir.toPath()) ) {
      db.initializeByFile(dbdir.toPath().resolve(Main.CONFIGNAME));
      DataBase.Storage storage = db.get("test.src");
      storage.scanFolder();
      checkContents(storage::dump, new String[]{
	  ".	*",
	  "dirx	*",
	  //"*	fileax1",
	  "*	fileax2",
	  "dirx/diry	*",
	  "*	fileaxy1",
	  "*	fileaxy2",
	  "diry	*",
	  "*	fileay1",
	  "*	fileay2",
	  "dirz	*",
	  "*	fileaz1",
	  "*	fileaz2",
	  "dirz/dirx	*",
	  //"*	fileazx1",
	  "*	fileazx2",
	  "dirz/dirx/dirx	*",
	  //"*	fileazxx1",
	  "*	fileazxx2",
	  "dirz/dirx/dirx/diry	*",
	  "*	fileazxxy1",
	  "*	fileazxxy2",
	});
    }

    // --------------------
    createFiles(root,new Object[]{
	"dic", new Object[]{
	  Main.CONFIGNAME, new String[]{
	    "test.src="+srcdir.getAbsolutePath(),
	    "/*/file*x1",
	  },
	},
      });

    try ( DataBase db = new DataBase(dbdir.toPath()) ) {
      db.initializeByFile(dbdir.toPath().resolve(Main.CONFIGNAME));
      DataBase.Storage storage = db.get("test.src");
      storage.scanFolder();
      checkContents(storage::dump, new String[]{
	  ".	*",
	  "dirx	*",
	  "*	fileax1", // ToDo:本来なら、これが無視されるはず。
	  "*	fileax2",
	  "dirx/diry	*",
	  "*	fileaxy1",
	  "*	fileaxy2",
	  "diry	*",
	  "*	fileay1",
	  "*	fileay2",
	  "dirz	*",
	  "*	fileaz1",
	  "*	fileaz2",
	  "dirz/dirx	*",
	  "*	fileazx1",
	  "*	fileazx2",
	  "dirz/dirx/dirx	*",
	  "*	fileazxx1",
	  "*	fileazxx2",
	  "dirz/dirx/dirx/diry	*",
	  "*	fileazxxy1",
	  "*	fileazxxy2",
	});
    }

    // ToDo: 他のパターンもテストする。
    // ・Windows の場合、大文字小文字を無視するか。
    // "**/file1"
    // "ntuser.*"
    // "cygwinjunsei/work/*/target"
    // "cygwinjunsei/**/#*#"
    // "cygwinjunsei/**/*~"
  }

  // ----------------------------------------------------------------------
  // ユーティリティメソッド

  public static DataBase execute( File root, File dbdir )
  throws IOException
  {
    return execute(root,dbdir,false);
  }

  public static DataBase execute( File root, File dbdir, boolean useXML )
  throws IOException
  {
    try ( DataBase db = new DataBase(dbdir.toPath()) ) {
      if ( useXML ) {
	Backup backup = db.initializeByXml(dbdir.toPath().resolve(Main.CONFIGXML));
	for ( Task task : backup.get("daily") ) {
	  for ( Storage copy : task.copyStorages ) {
	    Storage his = task.historyStorages.get(copy.storageName);
	    Main.backup(task.origStorage,copy,his);
	  }
	}
      } else {
	db.initializeByFile(dbdir.toPath().resolve(Main.CONFIGNAME));
	DataBase.Storage srcStorage = db.get("test.src");
	DataBase.Storage dstStorage = db.get("test.dst");
	Main.backup(srcStorage,dstStorage);
      }
      return db;
    }
  }

  // ログの取得
  public static List<String> selectEvents( Iterator<ILoggingEvent> iterator, String startPat, String endPat )
  {
    LinkedList<String> result = new LinkedList<>();
    boolean inner = (startPat == null);
    while ( iterator.hasNext() ) {
      ILoggingEvent ev = iterator.next();
      if ( ev.getLevel().toInt() < Level.DEBUG_INT ) continue;
      String msg = ev.getFormattedMessage();
      if ( inner ) {
	if ( msg.equals(endPat) ) return result;
	result.add(msg);
      } else if ( msg.equals(startPat) ) {
	inner = true;
      }
    }
    if ( !inner ) fail("no start statement : "+startPat);
    if ( endPat != null ) fail("no end statement : "+endPat);
    return result;
  }

  // ログイベントの確認
  public static void checkEvent( Object expects[] )
  {
    Iterator<ILoggingEvent> iterator = event.list.iterator();
    if ( !(expects[0] instanceof String) ) fail("expects[0] must be String : "+expects[0]);
    String startPat = (String)expects[0];
    String prevPat = startPat;
    for ( int i = 0; i < expects.length-1; ++i ) {
      String expstrs[] = new String[0];
      if ( expects[i+1] instanceof String[] ) {
	expstrs = (String[])expects[i+1];
	++i;
      }
      String endPat = null;
      if ( i+1 < expects.length && expects[i+1] instanceof String ) {
	endPat = (String)expects[i+1];
      }
      HashSet<String> actual = new HashSet<>(selectEvents(iterator,startPat,endPat));
      List<String> remain = Arrays.stream(expstrs)
	.filter(exp->!actual.remove(exp))
	.collect(Collectors.toList());
      if ( remain.size() > 0 || actual.size() > 0 )
	fail(String.format("different events from \"%s\" to \"%s\" : expects = %s, actual = %s ",prevPat,endPat,remain,actual));
      startPat = null;
      prevPat = endPat;
    }
  }

  // 更新日時の取得と設定
  public static Date lastModified( File file, String name )
  throws IOException
  {
    return lastModified(new File(file,name));
  }

  public static Date lastModified( File file )
  throws IOException
  {
    try {
      return new Date(Files.getLastModifiedTime(file.toPath()).toMillis());
    } catch ( NoSuchFileException ex ) {
      System.err.println("NoSuchFileException "+ex.getMessage());
      return new Date(file.lastModified());
    }
  }

  public static void lastModified( File file, Date date )
  throws IOException
  {
    Files.setLastModifiedTime(file.toPath(),FileTime.fromMillis(date.getTime()));
  }

  // ファイルの生成
  public static void createFiles( File dir, Object data[] )
  throws IOException
  {
    for ( Entry ent : Entry.walkData(data,true) ) {
      File target = new File(dir,ent.path.toString());
      if ( ent.contents == null ) {
	target.mkdir();
      } else if ( ent.type == 2 ) {
	Files.createSymbolicLink(target.getAbsoluteFile().toPath(),Paths.get(ent.contents));
      } else {
	Files.write(target.toPath(),ent.contents.getBytes());
	if ( ent.lastModified != null ) lastModified(target,ent.lastModified);
      }
    }
  }

  // 実際に保存されているファイルの確認
  public static void compareFiles( File dir, Object data[] )
  throws IOException
  {
    Map<Path,Entry> actual = collectFiles(dir);
    List<Entry> expect = Entry.walkData(data,true);

    expect = expect.stream()
      .filter(exp->{
	  Entry act = actual.remove(exp.path);
	  if ( act == null ) return true;
	  assertEquals("content for "+exp.path,exp,act);
	  return false;})
      .collect(Collectors.toList());
    if ( expect.size() > 0 || actual.size() > 0 )
      fail(String.format("different files : expect = %s, actual = %s ",expect,actual.values()));
  }

  // ファイルを収集
  public static Map<Path,Entry> collectFiles( File dir )
  throws IOException
  {
    Path root = dir.toPath();
    HashMap<Path,Entry> actual = new HashMap<>();
    try ( Stream<Path> stream = Files.walk(root) ) {
      for ( Path path : stream.collect(Collectors.toList() ) ) {
	if ( root.relativize(path).toString().length() == 0 ) continue;
	Entry ent = new Entry(root.relativize(path));
	if ( Files.isDirectory(path) ) ent.type = 1;
	if ( Files.isSymbolicLink(path) ) {
	  ent.type = 2;
	  ent.contents = Files.readSymbolicLink(path).toString();
	}
	actual.put(ent.path,ent);
      }
    }
    for ( Entry ent : actual.values() ) {
      if ( ent.type != 0 ) continue;
      ent.contents = new String(Files.readAllBytes(root.resolve(ent.path)));
      ent.lastModified = lastModified(new File(dir,ent.path.toString()));
    }
    return actual;
  }

  // ファイルの内容の確認
  public static void checkContents( File file, String expects[] )
  throws IOException
  {
    checkContents(Files.readAllLines(file.toPath()),expects);
  }

  public static void checkContents( Consumer<PrintStream> func, String expects[] )
  throws IOException
  {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bout);
    func.accept(out);
    out.close();
    checkContents(bout.toByteArray(),expects);
  }

  public static void checkContents( byte buf[], String expects[] )
  throws IOException
  {
    BufferedReader in = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buf)));
    LinkedList<String> lines = new LinkedList<>();
    String line;
    while ( (line = in.readLine()) != null ) lines.add(line);
    in.close();
    checkContents(lines,expects);
  }

  public static void checkContents( List<String> actual, String expects[] )
  {
    int i = 0;
    for ( String line : actual ) {
      if ( i >= expects.length ) break;
      int idx = expects[i].indexOf('*');
      String msg = String.format("line %d",i+1);
      if ( idx >= 0 ) {
	int dif = line.length()-expects[i].length();
	line = line.substring(0,idx)+'*'+line.substring(idx+1+dif,line.length());
      }
      assertEquals(msg,expects[i],line);
      ++i;
    }
    List<String> act = actual.subList(i,actual.size());
    if ( act.size() > 0 ) fail(printToString(out->{
	  out.format("more actual %d lines. missing expects are...",act.size()).println();
	  act.stream().forEach(out::println);
	}));
    final int n = i;
    if ( i < expects.length ) fail(printToString(out->{
	  out.format("less actual %d lines. remaining expects are...",expects.length-n).println();
	  for ( int j = n; j < expects.length; ++j ) out.println(expects[j]);
	}));
  }

  public static MessageDigest digest;

  @BeforeClass
  public static void initMD5()
  throws NoSuchAlgorithmException
  {
    digest = MessageDigest.getInstance("MD5");
  }

  public static String MD5( String contents )
  {
    digest.reset();
    digest.update(contents.getBytes());
    String str = Base64.getEncoder().encodeToString(digest.digest());
    int idx = str.indexOf('=');
    if ( idx > 0 ) str = str.substring(0,idx);
    return str;
  }

  public static String date( File dir, String filename )
  throws IOException
  {
    return date(lastModified(dir,filename));
  }

  public static String date( Date time )
  throws IOException
  {
    return FORM.format(time);
  }

  public static String line( File dir, String path, String contents )
  throws IOException
  {
    int idx = path.lastIndexOf('/');
    String name = idx < 0 ? path : path.substring(idx+1);
    return MD5(contents)+'\t'+date(dir,path)+'\t'+contents.length()+'\t'+name;
  }

  public static String line( Date time, String path, String contents )
  throws IOException
  {
    int idx = path.lastIndexOf('/');
    String name = idx < 0 ? path : path.substring(idx+1);
    return MD5(contents)+'\t'+date(time)+'\t'+contents.length()+'\t'+name;
  }

  // ----------------------------------------------------------------------
  // デバッグ出力の為のユーティリティ

  public static SimpleDateFormat FORM = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

  public static void printFolders( File dir )
  throws IOException
  {
    for ( File file : dir.listFiles() ) {
      String pat =
	Files.isSymbolicLink(file.toPath()) ? "<link>" :
	file.isDirectory() ? "<dir>" : "";
      System.out
	.format("%s",FORM.format(lastModified(file)))
	.format(" %-6s",pat)
	.format(" %s",file.toString())
	.append(pat.equals("<link>")?" -> "+Files.readSymbolicLink(file.toPath()):"")
	.println();
      if ( pat.equals("<dir>") ) {
	printFolders(file);
      }
    }
  }

  public static void cat( File file )
  throws IOException
  {
    System.out.println("----------[ "+file+" ]----------");
    Files.readAllLines(file.toPath()).stream().forEach(System.out::println);
  }

  public static void printAppender( ch.qos.logback.classic.Logger log )
  {
    Iterator<Appender<ILoggingEvent>> itr = log.iteratorForAppenders();
    System.out.println("-- Logger Info (start) --");
    System.out.println("Context = "+log.getLoggerContext());
    for ( int i = 0; itr.hasNext(); ++i ) {
      Appender<ILoggingEvent> apder = itr.next();
      System.out.println("appender["+i+"] = "+apder);
    }
    System.out.println("-- Logger Info (end) --");
  }

  public static void checkLogger()
  throws Exception
  {
    Logger log;

    log = LoggerFactory.getLogger(DataBase.class);
    System.out.println("logger class for DataBase = "+log.getClass().getName());
    log.info("This is a test message for DataBase");

    log = LoggerFactory.getLogger(Main.class);
    System.out.println("logger class for Backuper = "+log.getClass().getName());
    log.info("This is a test message for Backuper");

    log = LoggerFactory.getLogger("noname");
    System.out.println("logger class for noname = "+log.getClass().getName());
    log.info("This is a test message for noname");
  }

  public static void printEvent()
  {
    System.out.println("-- log event (start) --");
    for ( ILoggingEvent event : event.list ) {
      System.out.println("event : "+event);
    }
    System.out.println("-- log event (end) --");
  }

  public static String printToString( Consumer<PrintStream> func )
  {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bout);
    func.accept(out);
    out.close();
    return new String(bout.toByteArray());
  }

  // ----------------------------------------------------------------------
  // 以下は、調査のためのテスト

  // Path の比較 compareTo() の挙動の調査
  // 各パスを手繰りながら比較するのではなく、単に全体を文字列として比較している。
  //@Test
  public void testPath()
  {
    System.out.println("Path.class="+Paths.get(".").getClass().getName());
    check("aa","aaa");
    check("aa","ab");
    check("a/b","a/c");
    check("a/b","a=b");
    check("a/b","a+b");

    String data[] = new String[]{ "a", "aa", "aaa", "ab", "a/b", "a/c", "a=b", "a+b"};
    Path paths[] = new Path[data.length];
    for ( int i = 0; i < data.length; ++i ) paths[i] = Paths.get(data[i]);
    Arrays.sort(data);
    Arrays.sort(paths);
    System.out.println("data="+Arrays.asList(data));
    System.out.println("paths="+Arrays.asList(paths));

    check("a/b/c");
    check("/a/b/c");
  }

  public void check( String a, String b ) 
  {
    Path pa = Paths.get(a);
    Path pb = Paths.get(b);
    System.out.println(a+cmp(pa,pb)+b);
    a = cvt(pa = pa.getParent());
    b = cvt(pb = pb.getParent());
    System.out.println(a+cmp(pa,pb)+b);
  }

  public String cvt( Path p )
  {
    return ( p == null ) ? "(null)" : p.toString();
  }

  public char cmp( Path pa, Path pb )
  {
    if ( pa == null || pb == null ) return '?';
    int c = pa.compareTo(pb);
    return
      c == 0 ? '=' :
      c <  0 ? '<' :
      '>';
  }

  public void check( String s )
  {
    Path p = Paths.get(s);
    int len = p.getNameCount();
    String sep = "";
    System.out.print(s+" = ");
    for ( int i = 0; i < len; ++i ) {
      System.out.print(sep+p.getName(i));
      sep = " / ";
    }
    System.out.println();
  }
}
