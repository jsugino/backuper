package mylib.backuper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

public class BackuperTest
{
  @Rule
  public TemporaryFolder tempdir = new TemporaryFolder(new File("target"));

  public static ListAppender<ILoggingEvent> event = new ListAppender<>();

  @BeforeClass
  // ログの内容をチェックするための準備
  public static void initLogger()
  {
    ch.qos.logback.classic.Logger log = (ch.qos.logback.classic.Logger)LoggerFactory.getLogger("root");
    //printAppender(log);
    event.setName("FORTEST");
    event.setContext(log.getLoggerContext());
    event.start();
    log.addAppender(event);
    //printAppender(log);
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
	    "z1", "",
	  },
	},
      });

    //System.out.println("--- ORIG ---");
    //printFolders(root);

    execute(root,dbdir);

    //System.out.println("--- ANSWER ---");
    //printFolders(tempdir.getRoot());

    checkContents(new File(dbdir,"test.src.db"),new String[]{
	".",
	"CPjgJgxkQYUQzvsrBu7lzQ	*	3	b",
	"c",
	"puUcVpdndkyHnkmklAcKHA	*	6	c2",
	"nZySmjxYljiQjpaNIMXGZg	*	6	c3",
	"vopwnoEeXpfV3zpnf9hM4A	*	6	c4",
	"c/d",
	"d5Y7epMTd61Kta1qnNcYqg	*	3	d",
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
	  "calculate MD5 z/z1",
	},
	"Write DataBase test.dst "+dbdir+"/test.dst.db",
	"Compare Files test.src test.dst", new String[]{
	  "copy b",
	  "copy override c/c2",
	  "set lastModified c/c4",
	  "copy c/d/d",
	  "mkdir c/d",
	  "delete y/y2",
	  "rmdir z",
	  "delete z/z1",
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
	    "2", "6/2", current
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
	},
      });

    /*
    // 以下は、同一名でディレクトリとファイルが違っていた場合の対応ができてから実行する。

    System.out.println("--- ORIG ---");
    printFolders(root);

    execute(root,dbdir);

    System.out.println("--- ANSWER ---");
    printFolders(tempdir.getRoot());

    System.out.println("--- Database (start) ---");
    cat(new File(dbdir,"test.src.db"));
    cat(new File(dbdir,"test.dst.db"));
    System.out.println("--- Database (end) ---");

    compareFiles(dstdir,new Object[]{
	"1", "1", current,
	"2", "2", current,
	"4", new Object[]{
	  "1", "4/1", current,
	},
	"5", "5", current,
	"6", new Object[]{
	  "1", "6/1", current,
	  "2", "6/2", current
	},
      });
    */
  }

  // ----------------------------------------------------------------------
  // ユーティリティメソッド

  public static void execute( File root, File dbdir )
  throws IOException
  {
    DataBase db = new DataBase(dbdir.toPath());
    DataBase.Storage srcStorage = db.get("test.src");
    DataBase.Storage dstStorage = db.get("test.dst");
    Backuper.backup(srcStorage,dstStorage);
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
      } else if ( ent.isSymlink ) {
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
    List<Entry> expect = Entry.walkData(data);

    expect = expect.stream()
      .filter(exp->{
	  Entry act = actual.remove(exp.path);
	  if ( act == null ) return true;
	  assertEquals("content for "+exp.path,exp,act);
	  return false;})
      .collect(Collectors.toList());
    if ( expect.size() > 0 || actual.size() > 0 )
      fail(String.format("different files : expect = %s, actual = %s ",expect,actual));
  }

  // ファイルを収集
  public static Map<Path,Entry> collectFiles( File dir )
  throws IOException
  {
    Path root = dir.toPath();
    HashMap<Path,Entry> actual = new HashMap<>();
    try ( Stream<Path> stream = Files.walk(root) ) {
      stream
	.filter(p->!Files.isDirectory(p))
	.map(root::relativize)
	.map(Entry::new)
	.forEach(ent->actual.put(ent.path,ent));
    }
    for ( Entry ent : actual.values() ) {
      ent.contents = new String(Files.readAllBytes(root.resolve(ent.path)));
      ent.lastModified = lastModified(new File(dir,ent.path.toString()));
    }
    return actual;
  }

  // ファイルの内容の確認
  public static void checkContents( File file, String expects[] )
  throws IOException
  {
    int i = 0;
    List<String> actual = Files.readAllLines(file.toPath());
    for ( String line : actual ) {
      if ( i >= expects.length ) break;
      int idx = expects[i].indexOf('*');
      String msg = String.format("line %d",i+1);
      if ( idx > 0 ) {
	int dif = line.length()-expects[i].length();
	line = line.substring(0,idx)+'*'+line.substring(idx+1+dif,line.length());
      }
      assertEquals(msg,expects[i],line);
      ++i;
    }
    if ( i < actual.size() ) fail(String.format("more actual %d lines ",actual.size()-i));
    if ( i < expects.length ) fail(String.format("less actual %d lines ",expects.length-i));
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
}
