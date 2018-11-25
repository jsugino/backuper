package mylib.backuper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import static mylib.backuper.Backuper.log;
import static mylib.backuper.Backuper.STDFORMAT;

public class DataBase extends HashMap<String,DataBase.Storage>
{
  // ======================================================================
  public class Storage
  {
    String storageName;
    Path rootFolder;
    LinkedList<Folder> folders = null;
    LinkedList<Pattern> ignorePats = new LinkedList<>();

    public Storage( String storageName )
    {
      this.storageName = storageName;
    }

    public Folder getFolder( Path path )
    {
      Folder folder = find(folders,path);
      if ( folder == null ) {
	folder = new Folder(path);
	register(folders,folder);
      }
      return folder;
    }

    // --------------------------------------------------
    public void readDB()
    throws IOException
    {
      log.info("Read DataBase "+storageName);

      LinkedList<String> list = new LinkedList<>();
      try {
	try (
	  Stream<String> stream = Files.lines(dbFolder.resolve(storageName+".db"))
	) { stream.forEach(list::add); }
      } catch ( NoSuchFileException ex ) {
	log.info(ex);
      }
      folders = new LinkedList<Folder>();
      Folder folder = null;
      for ( String line : list ) {
	int i1;
	if ( (i1 = line.indexOf('\t')) < 0 ) {
	  folder = new Folder(Paths.get(line));
	  folders.add(folder);
	} else {
	  int i2 = line.indexOf('\t',i1+1);
	  int i3 = line.indexOf('\t',i2+1);
	  File file = new File(folder.folderPath.resolve(line.substring(i3+1)));
	  folder.files.add(file);
	  log.debug("read "+file.filePath);
	  file.hashValue = line.substring(0,i1);
	  try {
	    file.lastModified = STDFORMAT.parse(line.substring(i1+1,i2)).getTime();
	  } catch ( ParseException ex ) {
	    log.error("Cannot Parse Time : "+line);
	    file.lastModified = 0L;
	  }
	  file.length = Long.parseLong(line.substring(i2+1,i3));
	}
      }
    }

    public void writeDB()
    throws IOException
    {
      log.info("Write DataBase "+storageName);

      try ( PrintStream out = new PrintStream(Files.newOutputStream(dbFolder.resolve(storageName+".db")))
      ) { dump(out); }
    }

    // --------------------------------------------------
    public void copyFile( Path filePath, Storage dstStorage )
    throws IOException
    {
      Folder srcFolder = find(this.folders,filePath.getParent());
      File srcFile = find(srcFolder.files,filePath);
      Folder dstFolder = dstStorage.getFolder(filePath.getParent());
      File dstFile = find(dstFolder.files,filePath);
      String command = "copy override ";
      if ( dstFile == null ) {
	dstFile = new File(filePath);
	register(dstFolder.files,dstFile);
	command = "copy ";
      }
      dstFile.hashValue = srcFile.hashValue;
      dstFile.length = srcFile.length;
      log.info(command+filePath+" from "+this.rootFolder+" to "+dstStorage.rootFolder);
      Path dstPath = dstStorage.rootFolder.resolve(dstFile.filePath);
      try ( 
	InputStream  in  = Files.newInputStream(rootFolder.resolve(srcFile.filePath));
	OutputStream out = Files.newOutputStream(dstPath) )
      {
	byte buf[] = new byte[1024*64];
	int len;
	while ( (len = in.read(buf)) > 0 ) {
	  out.write(buf,0,len);
	}
      }
      dstFile.lastModified = Files.getLastModifiedTime(dstPath).toMillis();
    }

    public void deleteFile( Path delPath )
    throws IOException
    {
      log.info("delete "+delPath+" from "+this.rootFolder);
      Files.delete(rootFolder.resolve(delPath));
    }

    // --------------------------------------------------
    public void scanFolder()
    throws IOException
    {
      log.info("Scan Folder "+storageName);

      LinkedList<Folder> origFolders = folders;
      folders = new LinkedList<Folder>();
      LinkedList<Path> list = new LinkedList<>();
      try ( Stream<Path> stream = Files.walk(rootFolder) )
	  {
	    stream
	      .filter(path -> {
		  if ( Files.isDirectory(path) ) return false;
		  if ( Files.isSymbolicLink(path) ) {
		    log.info("ignore symbolic link : "+path);
		    return false;
		  }
		  Path rel = rootFolder.relativize(path);
		  String str = rel.toString();
		  if ( rel.getParent() == null ) str = "./"+str;
		  for ( Pattern pat : ignorePats ) {
		    if ( pat.matcher(str).matches() ) {
		      log.info("ignore file : "+path);
		      return false;
		    }
		  }
		  return true;
		})
	      .forEach(list::add);
	  }
      for ( Path path : list ) {
	log.debug("scan "+path);
	Path reltiv = rootFolder.relativize(path);
	Path parent = reltiv.getParent();
	if ( parent == null ) {
	  parent = Paths.get(".");
	  reltiv = parent.resolve(reltiv);
	}
	Folder folder = this.getFolder(parent);

	File newfile = new File(reltiv);
	register(folder.files,newfile);
	newfile.length = Files.size(path);
	newfile.lastModified = Files.getLastModifiedTime(path).toMillis();

	File origfile;
	if ( origFolders != null &&
	     (folder = find(origFolders,parent)) != null &&
	     (origfile = find(folder.files,reltiv)) != null &&
	     newfile.length == origfile.length &&
	     newfile.lastModified == origfile.lastModified
	) {
	  newfile.hashValue = origfile.hashValue;
	} else {
	  newfile.hashValue = getMD5(path);
	}
      }
    }

    // --------------------------------------------------
    public String toString()
    {
      StringBuffer buf = new StringBuffer(rootFolder.toString());
      if ( folders != null ) {
	buf.append(" (");
	int cnt = 0;
	for ( Folder folder : folders ) {
	  cnt += folder.files.size();
	}
	buf.append(cnt);
	buf.append(" files)");
      }
      return buf.toString();
    }

    public void dump( PrintStream out )
    {
      for ( Folder folder : folders ) {
	out.println(folder.folderPath.toString());
	for ( File file : folder.files ) {
	  file.dump(out);
	}
      }
    }
  }

  public static class Folder implements PathHolder
  {
    public Path folderPath;
    public LinkedList<File> files = new LinkedList<>();

    public Folder( Path folderPath )
    {
      this.folderPath = folderPath;
    }

    public Path getPath()
    {
      return folderPath;
    }
  }

  public static class File implements PathHolder
  {
    public Path filePath;
    public String hashValue;
    public long lastModified;
    public long length;

    public File( Path filePath )
    {
      this.filePath = filePath;
    }

    public Path getPath()
    {
      return filePath;
    }

    public void dump( PrintStream out )
    {
      out.println(hashValue+'\t'+STDFORMAT.format(new Date(lastModified))+'\t'+length+'\t'+filePath.getFileName());
    }
  }

  public static interface PathHolder
  {
    public Path getPath();
  }

  public static <T extends PathHolder> void register( LinkedList<T> list, T item )
  {
    ListIterator<T> itr = list.listIterator();
    while ( itr.hasNext() ) {
      int cmp = item.getPath().compareTo(itr.next().getPath());
      if ( cmp == 0 ) throw new RuntimeException("exist same entry "+item.getPath());
      if ( cmp < 0 ) {
	itr.previous();
	itr.add(item);
	item = null;
	break;
      }
    }
    if ( item != null ) list.add(item);
  }

  public static <T extends PathHolder> T find( LinkedList<T> list, Path path )
  {
    for ( T item : list ) {
      if ( item.getPath().equals(path) ) return item;
    }
    return null;
  }

  // ======================================================================
  public final static String CONFIGNAME = "folders.conf";

  public Path dbFolder;
  public MessageDigest digest;

  public DataBase( Path dbFolder )
  {
    this.dbFolder = dbFolder;
    log.info("Initialize DataBase "+dbFolder+'/'+CONFIGNAME);

    try {
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
	if ( line.length() == 0 || line.startsWith("##") ) return;
	int idx = line.indexOf('=');
	if ( idx <= 0 ) {
	  if ( line.charAt(0) == '/' ) {
	    line = line.substring(1);
	    line = line.replaceAll("\\.","\\\\.");
	    line = line.replaceAll("\\*\\*",".+");
	    line = line.replaceAll("\\*","[^/]+");
	  } else {
	    line = line.replaceAll("\\.","\\\\.");
	    line = line.replaceAll("\\*","[^/]+");
	    line = ".*/"+line;
	  }
	  if ( line.charAt(line.length()-1) == '/' ) line = line+".*";
	  Pattern pat = Pattern.compile(line);
	  log.info("ignore pattern : "+pat);
	  storage.ignorePats.add(pat);
	  continue;
	}
	String key = line.substring(0,idx).trim();
	Path path = Paths.get(line.substring(idx+1).trim());
	if ( !path.isAbsolute() ) {
	  log.error("Not Absolute Path : "+line);
	  return;
	}
	storage = new Storage(key);
	this.put(storage.storageName,storage);
	storage.rootFolder = path;
	storage.storageName = key;
	log.info("Read Config "+key+"="+path);
      }
    } catch ( IOException ex ) {
      log.error(ex);
    }
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
