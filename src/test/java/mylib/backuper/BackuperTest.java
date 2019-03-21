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
import java.util.stream.IntStream;
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
	  DataBase.CONFIGNAME, new String[]{
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
	  "calculate MD5 b",
	  "Ignore symlink l",
	  "Ignore symlink lc",
	  "Ignore file c/c1",
	  "calculate MD5 c/c2",
	  "calculate MD5 c/c3",
	  "calculate MD5 c/c4",
	  "calculate MD5 c/d/d",
	  "calculate MD5 g1/g2/g3/g",
	  "Ignore symlink c/d/lc",
	  "Ignore symlink c/d/lc1",
	  "Ignore symlink c/d/lx",
	},
	"Write DataBase test.src "+dbdir+"/test.src.db",
	"Read DataBase test.dst "+dbdir+"/test.dst.db", new String[]{
	  "java.nio.file.NoSuchFileException: "+dbdir+"/test.dst.db",
	},
	"Scan Folder test.dst "+dstdir.getAbsolutePath(), new String[]{
	  "Ignore file x",
	  "Ignore file y/y1",
	  "calculate MD5 y/y2",
	  "calculate MD5 c/c2",
	  "calculate MD5 c/c3",
	  "calculate MD5 c/c4",
	  "calculate MD5 z/za",
	  "calculate MD5 z/z1/zb",
	  "calculate MD5 z/z1/z2/zc",
	},
	"Write DataBase test.dst "+dbdir+"/test.dst.db",
	"Compare Files test.src test.dst", new String[]{
	  "copy b",
	  "copy g1/g2/g3/g",
	  "copy override c/c2",
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
	"Write DataBase test.dst "+dbdir+"/test.dst.db",
	"End Backup test.src test.dst",
      });
  }

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
	  DataBase.CONFIGNAME, new String[]{
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
	  "calculate MD5 1",
	  "calculate MD5 2",
	  "calculate MD5 4/1",
	  "calculate MD5 5",
	  "calculate MD5 6/1",
	  "calculate MD5 6/2",
	  "calculate MD5 dir/a",
	  "calculate MD5 dir/x/x1",
	  "calculate MD5 dir/y",
	  "calculate MD5 dir/z",
	},
	"Write DataBase test.src "+dbdir+"/test.src.db",
	"Read DataBase test.dst "+dbdir+"/test.dst.db", new String[]{
	  "java.nio.file.NoSuchFileException: "+dbdir+"/test.dst.db",
	},
	"Scan Folder test.dst "+dstdir.getAbsolutePath(), new String[]{
	  "calculate MD5 2",
	  "calculate MD5 3",
	  "calculate MD5 4",
	  "calculate MD5 5/1",
	  "calculate MD5 6/2",
	  "calculate MD5 6/3",
	  "calculate MD5 dir/b",
	  "calculate MD5 dir/z/c",
	  "Ignore symlink dir/x",
	  "Ignore symlink dir/y",
	  "Ignore symlink dir/z/z1",
	},
	"Write DataBase test.dst "+dbdir+"/test.dst.db",
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
	  "CANNOT COPY dir/x/x1",
	  "CANNOT COPY dir/y",
	  "CANNOT COPY dir/z",
	},
	"Write DataBase test.dst "+dbdir+"/test.dst.db",
	"End Backup test.src test.dst",
      });
  }

  // ----------------------------------------------------------------------
  // ユーティリティメソッド

  public static DataBase execute( File root, File dbdir )
  throws IOException
  {
    try ( DataBase db = new DataBase(dbdir.toPath()) ) {
      DataBase.Storage srcStorage = db.get("test.src");
      DataBase.Storage dstStorage = db.get("test.dst");
      Backuper.backup(srcStorage,dstStorage);
      return db;
    }
  }

  // ログの取得
  public static List<String> selectEvents( Iterator<ILoggingEvent> iterator, String startPat, String endPat )
  {
    LinkedList<String> result = new LinkedList<>();
    Boolean inner = (startPat == null);
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
    for ( int i = 0; i < expects.length-1; ++i ) {
      String expstrs[] = new String[]{};
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
	fail(String.format("different events from \"%s\" to \"%s\" : expects = %s, actual = %s ",startPat,endPat,remain,actual));
      startPat = null;
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
      Appender apder = itr.next();
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

    log = LoggerFactory.getLogger(Backuper.class);
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
