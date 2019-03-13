package mylib.backuper;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
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
    File dbdir = tempdir.newFolder("dic");
    File srcdir = tempdir.newFolder("src");
    File dstdir = tempdir.newFolder("dst");
    try ( PrintStream out = new PrintStream(new File(dbdir,DataBase.CONFIGNAME)) ) {
      out.println("test.src="+srcdir.getAbsolutePath());
      out.println("a");
      out.println("c1");
      out.println("test.dst="+dstdir.getAbsolutePath());
      out.println("x");
      out.println("y1");
    }
    createFiles(srcdir,new Object[]{
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
      });
    createFiles(dstdir,new Object[]{
	"x", "xx",
	"y", new Object[]{
	  "y1", "",
	  "y2", "",
	},
	"c", new Object[]{
	  "c2", "ccc",
	},
      });
    System.out.println("--- ORIG ---");
    printFolders(tempdir.getRoot());

    try ( Backuper.Logger log = new Backuper.Logger(dbdir.toPath().resolve("backup.log")) ) {
      Backuper.log = log;
      DataBase db = new DataBase(dbdir.toPath());
      DataBase.Storage srcStorage = db.get("test.src");
      DataBase.Storage dstStorage = db.get("test.dst");
      Backuper.backup(srcStorage,dstStorage);
    }
    LinkedList<String> answer = new LinkedList<>();
    try ( Stream<Path> stream = Files.walk(dstdir.toPath()) ) {
      stream
	.map(p->dstdir.toPath().relativize(p).toString())
	.map(p->p.length()==0 ? "." : p)
	.forEach(answer::add);
    }
    System.out.println("--- ANSWER ---");
    printFolders(tempdir.getRoot());
    compareFiles(dstdir,new Object[]{
	"b", "bbb", lastModified(srcdir,"b"),
	"c", new Object[]{
	  "c2", "ccc222", lastModified(srcdir,"c/c2"),
	  "d", new Object[] {
	    "d", "ddd", lastModified(srcdir,"c/d/d"),
	  },
	},
	"x", "xx",
	"y", new Object[]{
	  "y1", "",
	},
      });
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
	try ( FileOutputStream out = new FileOutputStream(target) ) {
	  out.write(data[i+1].toString().getBytes());
	}
      } else {
	assertTrue("mkdir "+target,target.mkdir());
	createFiles(target,(Object[])data[i+1]);
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
