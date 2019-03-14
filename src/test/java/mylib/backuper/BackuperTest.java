package mylib.backuper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.stream.Stream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BackuperTest
{
  @Rule
  public TemporaryFolder tempdir = new TemporaryFolder(new File("target"));

  @Test
  public void testSimple()
  throws IOException
  {
    File root = tempdir.getRoot();
    File dbdir = new File(root,"dic");
    File srcdir = new File(root,"src");
    File dstdir = new File(root,"dst");
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
	  },
	},
      });

    execute(root,dbdir);

    compareFiles(dstdir,new Object[]{
	"b", "bbb", lastModified(root,"src/b"),
	"c", new Object[]{
	  "c2", "ccc222", lastModified(root,"src/c/c2"),
	  "d", new Object[] {
	    "d", "ddd", lastModified(root,"src/c/d/d"),
	  },
	},
	"x", "xx",
	"y", new Object[]{
	  "y1", "",
	},
      });
  }

  //@Test
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

    execute(root,dbdir);

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
  }

  public void execute( File root, File dbdir )
  throws IOException
  {
    System.out.println("--- ORIG ---");
    printFolders(root);

    DataBase db = new DataBase(dbdir.toPath());
    DataBase.Storage srcStorage = db.get("test.src");
    DataBase.Storage dstStorage = db.get("test.dst");
    Backuper.backup(srcStorage,dstStorage);

    System.out.println("--- ANSWER ---");
    printFolders(tempdir.getRoot());

    System.out.println("--- Database ---");
    cat(new File(dbdir,"test.src.db"));
    cat(new File(dbdir,"test.dst.db"));
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
	  System.out
	    .format("date check %s ",FORM.format(dt))
	    .format(" <=>  %s",FORM.format(lastModified(file)))
	    .println();
	  assertEquals("timestamp for "+file+" "+FORM.format(dt)+" <=> "+FORM.format(lastModified(file)),dt,lastModified(file));
	}
      } else {
	if ( !file.isDirectory() ) fail("not a directory in actual "+file);
	compareFiles(file,(Object[])exp);
      }
    }
    if ( map.size() > 0 ) fail("more file/folder in expects "+map);
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
}
