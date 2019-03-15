package mylib.backuper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.attribute.FileTime;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
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

  //@After
  public void printEvent()
  {
    System.out.println("-- log event (start) --");
    for ( ILoggingEvent event : event.list ) {
      System.out.println("event : "+event);
    }
    System.out.println("-- log event (end) --");
  }

  public List<String> selectEvents( String startPat, String endPat )
  {
    LinkedList<String> result = new LinkedList<>();
    Boolean inner = false;
    for ( ILoggingEvent ev : event.list ) {
      if ( ev.getLevel().toInt() < Level.INFO_INT ) continue;
      String msg = ev.getFormattedMessage();
      if ( inner ) {
	if ( msg.equals(endPat) ) break;
	result.add(msg);
      } else if ( msg.equals(startPat) ) {
	inner = true;
      }
    }
    return result;
  }

  public void checkEvent( Object expects[] )
  {
    for ( int i = 0; i < expects.length-1; ++i ) {
      if ( !(expects[i] instanceof String) ) fail("expects["+i+"] must be String : "+expects[i]);
      String startPat = (String)expects[i];
      String actstr[] = null;
      if ( expects[i+1] instanceof String[] ) {
	actstr = (String[])expects[i+1];
	++i;
      }
      String endPat = null;
      if ( i+1 < expects.length && expects[i+1] instanceof String ) {
	endPat = (String)expects[i+1];
      }
      HashSet<String> actual = new HashSet<>(selectEvents(startPat,endPat));
      if ( actstr != null ) {
	List<String> remain = Arrays.stream(actstr)
	  .filter(exp->!actual.remove(exp))
	  .collect(Collectors.toList());
	if ( remain.size() > 0 ) fail(String.format("less actuals from \"%s\" to \"%s\" : %s ",startPat,endPat,remain));
      }
      if ( actual.size() > 0 ) fail(String.format("more actuals from \"%s\" to \"%s\" : %s ",startPat,endPat,actual));
    }
  }

  // ----------------------------------------------------------------------
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
	    "y1", "",
	    "y2", "",
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
	  "y1", "",
	},
      });

    checkEvent(new Object[]{
	"Read DataBase test.src", new String[]{
	  "java.nio.file.NoSuchFileException: "+dbdir+"/test.src.db",
	},
	"Scan Folder test.src", new String[]{
	  "ignore file a",
	  "calculate MD5 b",
	  "ignore symlink l",
	  "ignore symlink lc",
	  "ignore file c/c1",
	  "calculate MD5 c/c2",
	  "calculate MD5 c/c3",
	  "calculate MD5 c/c4",
	  "calculate MD5 c/d/d",
	  "ignore symlink c/d/lc",
	  "ignore symlink c/d/lc1",
	  "ignore symlink c/d/lx",
	},
	"Write DataBase test.src",
	"Read DataBase test.dst", new String[]{
	  "java.nio.file.NoSuchFileException: "+dbdir+"/test.dst.db",
	},
	"Scan Folder test.dst", new String[]{
	  "ignore file x",
	  "ignore file y/y1",
	  "calculate MD5 y/y2",
	  "calculate MD5 c/c2",
	  "calculate MD5 c/c3",
	  "calculate MD5 c/c4",
	  "calculate MD5 z/z1",
	},
	"Write DataBase test.dst",
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
	"Write DataBase test.dst",
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

  public void execute( File root, File dbdir )
  throws IOException
  {
    DataBase db = new DataBase(dbdir.toPath());
    DataBase.Storage srcStorage = db.get("test.src");
    DataBase.Storage dstStorage = db.get("test.dst");
    Backuper.backup(srcStorage,dstStorage);
  }

  public Date lastModified( File file, String name )
  throws IOException
  {
    return lastModified(new File(file,name));
  }

  public Date lastModified( File file )
  throws IOException
  {
    try {
      return new Date(Files.getLastModifiedTime(file.toPath()).toMillis());
    } catch ( NoSuchFileException ex ) {
      System.err.println("NoSuchFileException "+ex.getMessage());
      return new Date(file.lastModified());
    }
  }

  public void lastModified( File file, Date date )
  throws IOException
  {
    Files.setLastModifiedTime(file.toPath(),FileTime.fromMillis(date.getTime()));
  }

  public void createFiles( File dir, Object data[] )
  throws IOException
  {
    for ( int i = 0; i < data.length; i += 2 ) {
      String name = (String)data[i];
      if ( name.charAt(0) == '@' ) {
	name = name.substring(1,name.length());
	Files.createSymbolicLink(
	  new File(dir,name).getAbsoluteFile().toPath(),
	  new File(dir,(String)data[i+1]).getAbsoluteFile().toPath());
	continue;
      }
      File target = new File(dir,name);
      if ( data[i+1] instanceof String ) {
	Files.write(target.toPath(),data[i+1].toString().getBytes());
	if ( i+2 < data.length && data[i+2] instanceof Date ) {
	  lastModified(target,(Date)data[i+2]);
	  ++i;
	}
      } else if ( data[i+1] instanceof String[] ) {
	Files.write(target.toPath(),Arrays.asList((String[])data[i+1]));
	/*
	try ( PrintStream out = new PrintStream(target) ) {
	  for ( String line : (String[])data[i+1] ) {
	    out.println(line);
	  }
	}
	*/
      } else if ( data[i+1] instanceof Object[] ) {
	assertTrue("mkdir "+target,target.mkdir());
	createFiles(target,(Object[])data[i+1]);
      } else {
	fail("unknown data type : "+data[i+1].getClass().getName());
      }
    }
  }

  SimpleDateFormat FORM = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

  public void compareFiles( File dir, Object data[] )
  throws IOException
  {
    HashMap<String,Object> map = new HashMap<>();
    HashMap<String,Date> dmap = new HashMap<>();
    for ( int i = 0; i < data.length; i += 2 ) {
      map.put((String)data[i],data[i+1]);
      if ( i+2 < data.length && data[i+2] instanceof Date ) {
	dmap.put((String)data[i],(Date)data[i+2]);
	++i;
      }
    }
    for ( File file : dir.listFiles() ) {
      Object exp = map.remove(file.getName());
      if ( exp == null ) fail("more file/folder in actual : "+file);
      if ( exp instanceof String ) {
	if ( !file.isFile() ) fail("not a file in actual "+file);
	assertEquals(file.toString(),
	  (String)exp,
	  new String(Files.readAllBytes(file.toPath())));
	Date dt = dmap.get(file.getName());
	if ( dt != null ) {
	  assertEquals("timestamp for "+file+" "+FORM.format(dt)+" <=> "+FORM.format(lastModified(file)),dt,lastModified(file));
	}
      } else {
	if ( !file.isDirectory() ) fail("not a directory in actual "+file);
	compareFiles(file,(Object[])exp);
      }
    }
    if ( map.size() > 0 ) fail("more file/folder in expects "+map);
  }

  public void checkContents( File file, String expects[] )
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

  public void printFolders( File dir )
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

  public void cat( File file )
  throws IOException
  {
    System.out.println("----------[ "+file+" ]----------");
    Files.readAllLines(file.toPath()).stream().forEach(System.out::println);
  }

  // ----------------------------------------------------------------------
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
}
