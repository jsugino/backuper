package mylib.backuper;

import static java.nio.file.StandardOpenOption.*;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class DataBase implements Closeable
{
  // ======================================================================
  public Logger log;

  public class Logger
  {
    PrintStream out;

    public Logger()
    {
      try {
	this.out = new PrintStream(Files.newOutputStream(dbFolder.resolve("backup.log"),CREATE,APPEND));
	info("Start Logging");
      } catch ( IOException ex ) {
	ex.printStackTrace();
      }
    }

    public void info( String str )
    {
      out.println(STDFORMAT.format(new Date())+" INFO  "+str);
    }

    public void info( Exception ex )
    {
      System.err.println(ex.getClass().getName()+": "+ex.getMessage());
      ex.printStackTrace(out);
    }

    public void error( String str )
    {
      System.err.println(str);
      out.println(STDFORMAT.format(new Date())+" ERROR "+str);
    }

    public void error( Exception ex )
    {
      ex.printStackTrace();
      ex.printStackTrace(out);
    }
  }

  public void close()
  {
    log.info("Stop Logging");
    log.out.close();
  }

  // ======================================================================
  public static SimpleDateFormat STDFORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

  public static class Storage
  {
    Path rootFolder;
    HashMap<Path,Folder> folders = null;
    LinkedList<Pattern> ignorePats = new LinkedList<>();

    public String toString()
    {
      StringBuffer buf = new StringBuffer(rootFolder.toString());
      if ( folders != null ) {
	buf.append(" (");
	int cnt = 0;
	for ( Folder folder : folders.values() ) {
	  cnt += folder.files.size();
	}
	buf.append(cnt);
	buf.append(" files)");
      }
      return buf.toString();
    }

    public void dump( PrintStream out )
    {
      Path parray[] = new Path[folders.size()];
      parray = folders.keySet().toArray(parray);
      Arrays.sort(parray);
      for ( Path path : parray ) {
	out.println(path);
	Folder folder = folders.get(path);
	String farray[] = new String[folder.files.size()];
	farray = folder.files.keySet().toArray(farray);
	Arrays.sort(farray);
	for ( String name : farray ) {
	  File file = folder.files.get(name);
	  out.println(file.hashValue+'\t'+STDFORMAT.format(new Date(file.lastModified))+'\t'+file.length+'\t'+name);
	}
      }
    }
  }

  public static class Folder
  {
    public HashMap<String,File> files = new HashMap<>();
  }

  public static class File
  {
    public String hashValue;
    public long lastModified;
    public long length;
  }

  // ======================================================================
  public final static String CONFIGNAME = "folders.conf";

  public Path dbFolder;
  public HashMap<String,Storage> storageMap;
  public MessageDigest digest;

  public DataBase( Path dbFolder )
  {
    this.dbFolder = dbFolder;
    this.log = new Logger();
    log.info("Initialize DataBase "+dbFolder+'/'+CONFIGNAME);

    try {
      this.storageMap = new HashMap<>();
      try {
	digest = MessageDigest.getInstance("MD5");
      } catch ( NoSuchAlgorithmException ex ) {
	throw new IOException("Cannot initialize 'digest MD5'",ex);
      }
      LinkedList<String> list = new LinkedList<>();
      try (
	Stream<String> stream = Files.lines(dbFolder.resolve(CONFIGNAME))
      ) { stream.forEach(list::add); }
      Storage storage = null;
      for ( String line : list ) {
	if ( line.length() == 0 || line.startsWith("#") ) return;
	int idx = line.indexOf('=');
	if ( idx <= 0 ) {
	  line = line.replaceAll("\\.","\\\\.");
	  line = line.replaceAll("\\*\\*",".+");
	  line = line.replaceAll("\\*","[^/]+");
	  if ( line.charAt(line.length()-1) == '/' ) line = line.substring(0,line.length()-1)+".*";
	  storage.ignorePats.add(Pattern.compile(line));
	  continue;
	}
	String key = line.substring(0,idx).trim();
	Path path = Paths.get(line.substring(idx+1).trim());
	if ( !path.isAbsolute() ) {
	  log.error("Not Absolute Path : "+line);
	  return;
	}
	storage = new Storage();
	storage.rootFolder = path;
	storageMap.put(key,storage);
	log.info("Read Config "+key+"="+path);
      }
    } catch ( IOException ex ) {
      log.error(ex);
    }
  }

  public void readDB( String storageName )
  throws IOException
  {
    log.info("Read DataBase "+storageName);

    Storage storage = storageMap.get(storageName);
    if ( storage == null ) {
      throw new IOException("Illegal Storage Name : "+storageName);
    }

    LinkedList<String> list = new LinkedList<>();
    try {
      try (
	Stream<String> stream = Files.lines(dbFolder.resolve(storageName+".db"))
      ) { stream.forEach(list::add); }
    } catch ( NoSuchFileException ex ) {
      log.info(ex);
    }
    storage.folders = new HashMap<Path,Folder>();
    Folder folder = null;
    for ( String line : list ) {
      int i1;
      if ( (i1 = line.indexOf('\t')) < 0 ) {
	folder = storage.folders.get(line);
	if ( folder == null ) {
	  folder = new Folder();
	  storage.folders.put(Paths.get(line),folder);
	}
      } else {
	int i2 = line.indexOf('\t',i1+1);
	int i3 = line.indexOf('\t',i2+1);
	File file = new File();
	file.hashValue = line.substring(0,i1);
	try {
	  file.lastModified = STDFORMAT.parse(line.substring(i1+1,i2)).getTime();
	} catch ( ParseException ex ) {
	  log.error("Cannot Parse Time : "+line);
	  file.lastModified = 0L;
	}
	file.length = Long.parseLong(line.substring(i2+1,i3));
	folder.files.put(line.substring(i3+1),file);
      }
    }
  }

  public void scanFolder( String storageName )
  throws IOException
  {
    log.info("Scan Folder "+storageName);

    Storage storage = storageMap.get(storageName);
    if ( storage == null ) {
      throw new IOException("Illegal Storage Name : "+storageName);
    }

    HashMap<Path,Folder> origFolders = storage.folders;
    storage.folders = new HashMap<Path,Folder>();
    LinkedList<Path> list = new LinkedList<>();
    try ( Stream<Path> stream = Files.walk(storage.rootFolder) )
    {
      stream
	.filter(path -> {
	    if ( Files.isDirectory(path) ) return false;
	    for ( Pattern pat : storage.ignorePats ) {
	      String str = storage.rootFolder.relativize(path).toString();
	      if ( pat.matcher(str).matches() ) return false;
	    }
	    return true;
	  })
	.forEach(list::add);
    }
    for ( Path path : list ) {
      Path reltiv = storage.rootFolder.relativize(path);
      Path parent = reltiv.getParent();
      if ( parent == null ) parent = Paths.get(".");
      String name = reltiv.getFileName().toString();
      //System.err.println("reltiv = "+reltiv+' '+parent+' '+name);
      Folder folder = null;
      File origfile = null;
      File newfile = new File();
      if ( (folder = storage.folders.get(parent)) == null ) {
	folder = new Folder();
	storage.folders.put(parent,folder);
      }
      if ( folder.files.get(name) != null ) {
	log.error("Duplicate : "+path);
	return;
      }
      folder.files.put(name,newfile);
      newfile.length = Files.size(path);
      newfile.lastModified = Files.getLastModifiedTime(path).toMillis();
      if ( origFolders != null &&
	   (folder = origFolders.get(parent)) != null &&
	   (origfile = folder.files.get(name)) != null &&
	   newfile.length == origfile.length &&
	   newfile.lastModified == origfile.lastModified
      ) {
	newfile.hashValue = origfile.hashValue;
      } else {
	newfile.hashValue = getMD5(path);
      }
    }
  }

  public void writeDB( String storageName )
  throws IOException
  {
    log.info("Write DataBase "+storageName);

    Storage storage = storageMap.get(storageName);
    if ( storage == null ) {
      throw new IOException("Illegal Storage Name : "+storageName);
    }

    try ( PrintStream out = new PrintStream(Files.newOutputStream(dbFolder.resolve(storageName+".db")))
    ) { storage.dump(out); }
  }

  // --------------------------------------------------
  public String getMD5( Path path )
  throws IOException
  {
    log.info("Calculate MD5 "+path);

    digest.reset();
    try ( InputStream in = Files.newInputStream(path) )
    {
      byte buf[] = new byte[1024*64];
      int len;
      while ( (len = in.read(buf)) > 0 ) {
	digest.update(buf,0,len);
      }
    }
    String str = Base64.getEncoder().encodeToString(digest.digest());
    int idx = str.indexOf('=');
    if ( idx > 0 ) str = str.substring(0,idx);
    return str;
  }
}
