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
import java.util.stream.Collectors;
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
    LinkedList<Pattern> ignoreFilePats = new LinkedList<>();
    LinkedList<Pattern> ignoreFolderPats = new LinkedList<>();

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
      Path curPath = Paths.get(".");
      for ( String line : list ) {
	int i1;
	if ( (i1 = line.indexOf('\t')) < 0 ) {
	  folder = new Folder(Paths.get(line));
	  log.debug("new Folder("+folder.folderPath+")");
	  folders.add(folder);
	} else {
	  int i2 = line.indexOf('\t',i1+1);
	  int i3 = line.indexOf('\t',i2+1);
	  File file = new File(
	    folder.folderPath.equals(curPath)
	    ? Paths.get(line.substring(i3+1))
	    : folder.folderPath.resolve(line.substring(i3+1)));
	  log.debug("new File("+file.filePath+")");
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
      log.debug("copyFile("+filePath+")");
      Path parentPath = filePath.getParent();
      if ( parentPath == null ) parentPath = Paths.get(".");
      Folder srcFolder = find(this.folders,parentPath);
      File srcFile = find(srcFolder.files,filePath);
      Folder dstFolder = dstStorage.getFolder(parentPath);
      File dstFile = find(dstFolder.files,filePath);
      String command = "copy override ";
      if ( dstFile == null ) {
	dstFile = new File(filePath);
	register(dstFolder.files,dstFile);
	command = "copy ";
      }
      dstFile.hashValue = srcFile.hashValue;
      dstFile.length = srcFile.length;
      log.info(command+filePath);
      Path dstPath = dstStorage.rootFolder.resolve(dstFile.filePath);
      Path dstParent = dstPath.getParent();
      if ( !Files.isDirectory(dstParent) ) {
	log.info("mkdir "+dstParent);
	Files.createDirectories(dstParent);
      }
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
      log.info("delete "+delPath);
      Files.delete(rootFolder.resolve(delPath));
      Path parentPath = delPath.getParent();
      if ( parentPath == null ) parentPath = Paths.get(".");
      Folder folder = find(this.folders,parentPath);
      delete(folder.files,delPath);
    }

    public void cleanupFolder( boolean doRemove )
    throws IOException
    {
      LinkedList<Path> empties = new LinkedList<>();
      folders = folders.stream()
	.filter(folder -> {
	    if ( folder.files.size() != 0 ) return true;
	    empties.add(folder.folderPath);
	    return false;
	  })
	.collect(Collectors.toCollection(LinkedList::new));

      if ( !doRemove || empties == null ) return;

      for ( Path path : empties.stream()
	      .sorted((x,y) -> -x.compareTo(y))
	      .collect(Collectors.toList())
      ) {
	Path full = rootFolder.resolve(path);
	if ( Files.list(full).count() == 0 ) {
	  log.info("rmdir "+path);
	  Files.delete(full);
	}
      }
    }

    // --------------------------------------------------
    public void scanFolder()
    throws IOException
    {
      log.info("Scan Folder "+storageName+" "+rootFolder);

      LinkedList<Folder> origFolders = folders;
      folders = new LinkedList<Folder>();
      LinkedList<Path> folderList = new LinkedList<>();
      LinkedList<Path> pathList = new LinkedList<>();
      folderList.add(rootFolder);
      while ( folderList.size() > 0 ) {
	Path folderpath = folderList.remove();
	Path rel = rootFolder.relativize(folderpath);
	if ( rel.toString().length() == 0 ) rel = Paths.get(".");
	log.debug("new Folder("+rel+")");
	Folder folder = new Folder(rel);
	register(folders,folder);
	pathList.clear();
	try ( Stream<Path> stream = Files.list(folderpath) ) {
	  stream.forEach(pathList::add);
	}
	nextPath:
	for ( Path path : pathList ) {
	  Path relpath = rootFolder.relativize(path);
	  if ( Files.isSymbolicLink(path) ) {
	    log.info("ignore symbolic link : "+path);
	    continue nextPath;
	  } else if ( Files.isDirectory(path) ) {
	    log.debug("scan folder "+relpath);
	    for ( Pattern pat : ignoreFolderPats ) {
	      if ( pat.matcher(relpath.toString()).matches() ) {
		log.info("ignore folder "+relpath);
		continue nextPath;
	      }
	    }
	    folderList.add(path);
	  } else {
	    log.debug("scan file "+relpath);
	    for ( Pattern pat : ignoreFilePats ) {
	      if ( pat.matcher(relpath.getFileName().toString()).matches() ) {
		log.info("ignore file "+relpath);
		continue nextPath;
	      }
	    }
	    log.debug("new File("+relpath+")");
	    File file = new File(relpath);
	    register(folder.files,file);
	    file.length = Files.size(path);
	    file.lastModified = Files.getLastModifiedTime(path).toMillis();
	    Folder origfolder = null;
	    File origfile = null;
	    if ( 
	      origFolders != null &&
	      (origfolder = find(origFolders,folder.folderPath)) != null &&
	      (origfile = find(origfolder.files,relpath)) != null &&
	      file.length == origfile.length &&
	      file.lastModified == origfile.lastModified
	    ) {
	      file.hashValue = origfile.hashValue;
	    } else {
	      file.hashValue = getMD5(path);
	    }
	  }
	}
      }
      /*
      folders = folders.stream()
	.filter(folder -> folder.files.size() != 0)
	.collect(Collectors.toCollection(LinkedList::new));
      */
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

  public static <T extends PathHolder> T delete( LinkedList<T> list, Path path )
  {
    ListIterator<T> itr = list.listIterator();
    while ( itr.hasNext() ) {
      T orig;
      if ( (orig = itr.next()).getPath().equals(path) ) {
	itr.remove();
	return orig;
      }
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
	  if ( line.charAt(line.length()-1) == '/' ) {
	    line = line.substring(0,line.length()-1);
	    line = line.replaceAll("\\.","\\\\.");
	    line = line.replaceAll("\\*\\*",".+");
	    line = line.replaceAll("\\*","[^/]+");
	    if ( line.charAt(0) == '/' ) {
	      line = line.substring(1);
	    } else {
	      line = "(.+/)?"+line;
	    }
	    Pattern pat = Pattern.compile(line);
	    log.info("ignore folder pattern : "+pat);
	    storage.ignoreFolderPats.add(pat);
	  } else {
	    line = line.replaceAll("\\.","\\\\.");
	    line = line.replaceAll("\\*","[^/]+");
	    if ( line.charAt(0) == '/' ) {
	      line = line.substring(1);
	    } else {
	      line = "(.+/)?"+line;
	    }
	    Pattern pat = Pattern.compile(line);
	    log.info("ignore file pattern : "+pat);
	    storage.ignoreFilePats.add(pat);
	  }
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
