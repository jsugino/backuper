package mylib.backuper;

import static mylib.backuper.Backuper.STDFORMAT;
import static mylib.backuper.Backuper.log;

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
import java.util.List;
import java.util.ListIterator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataBase extends HashMap<String,DataBase.Storage>
{
  // ======================================================================
  public abstract class Storage
  {
    public String storageName;
    public LinkedList<Folder> folders = null;		// 全てのフォルダのリスト
    public LinkedList<Pattern> ignoreFilePats = new LinkedList<>();
    public LinkedList<Pattern> ignoreFolderPats = new LinkedList<>();

    public Storage( String storageName )
    {
      this.storageName = storageName;
    }

    public abstract String getRoot();

    public abstract boolean mkParentDir( Path path ) throws IOException;

    public abstract InputStream newInputStream( Path path ) throws IOException;

    public abstract OutputStream newOutputStream( Path path ) throws IOException;

    public abstract void setLastModified( Path path, long time ) throws IOException;

    public abstract long getLastModified( Path path ) throws IOException;

    public abstract long getSize( Path relpath ) throws IOException;

    public abstract List<Path> pathList( Path rel ) throws IOException;

    public abstract boolean deleteRealFile( Path path ) throws IOException;

    public abstract boolean isSymbolicLink( Path relpath );

    public abstract boolean isDirectory( Path relpath );

    // --------------------------------------------------
    public Folder getFolder( Path path )
    {
      Folder folder = findFromList(folders,path);
      if ( folder == null ) {
	folder = new Folder(path);
	registerToList(folders,folder);
      }
      return folder;
    }

    // --------------------------------------------------
    public void readDB()
    throws IOException
    {
      log.info("Read DataBase "+storageName);

      LinkedList<String> list = new LinkedList<>();
      try (
	Stream<String> stream = Files.lines(dbFolder.resolve(storageName+".db"))
      ) {
	stream.forEach(list::add);
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
      Storage srcStorage = this;
      Path parentPath = filePath.getParent();
      if ( parentPath == null ) parentPath = Paths.get(".");
      Folder srcFolder = findFromList(srcStorage.folders,parentPath);
      File   srcFile   = findFromList(srcFolder.files,filePath);
      Folder dstFolder = dstStorage.getFolder(parentPath);
      File   dstFile   = findFromList(dstFolder.files,filePath);
      String command = "copy override ";
      if ( dstFile == null ) {
	dstFile = new File(filePath);
	registerToList(dstFolder.files,dstFile);
	command = "copy ";
      }
      dstFile.hashValue = srcFile.hashValue;
      dstFile.length = srcFile.length;
      dstFile.lastModified = srcFile.lastModified;
      
      log.message(command+filePath);
      if ( dstStorage.mkParentDir(dstFile.filePath) ) {
	log.message("mkdir "+dstFile.filePath.getParent());
      }
      try ( 
	InputStream  in  = this.newInputStream(srcFile.filePath);
	OutputStream out = dstStorage.newOutputStream(dstFile.filePath);
      ) {
	byte buf[] = new byte[1024*64];
	int len;
	while ( (len = in.read(buf)) > 0 ) {
	  out.write(buf,0,len);
	}
      }
      dstStorage.setLastModified(dstFile.filePath,dstFile.lastModified);
    }

    public void setLastModified( Path filePath, Storage srcStorage )
    throws IOException
    {
      Storage dstStorage = this;
      Path parentPath = filePath.getParent();
      if ( parentPath == null ) parentPath = Paths.get(".");
      Folder srcFolder = findFromList(srcStorage.folders,parentPath);
      File   srcFile   = findFromList(srcFolder.files,filePath);
      Folder dstFolder = findFromList(dstStorage.folders,parentPath);
      File   dstFile   = findFromList(dstFolder.files,filePath);

      if ( dstFile.lastModified == srcFile.lastModified ) return;

      log.info("set last modified "+dstFile.filePath);
      dstFile.lastModified = srcFile.lastModified;
      dstStorage.setLastModified(dstFile.filePath,dstFile.lastModified);
    }

    public void deleteFile( Path delPath )
    throws IOException
    {
      log.message("delete "+delPath);
      deleteRealFile(delPath);
      Path parentPath = delPath.getParent();
      if ( parentPath == null ) parentPath = Paths.get(".");
      Folder folder = findFromList(this.folders,parentPath);
      deleteFromList(folder.files,delPath);
    }

    /**
     * folders から空のフォルダを削除する。
     *
     * @param doRemove true の場合、物理的にも削除する。
     *
     * @throws IOException
     */
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
	if ( deleteRealFile(path) ) log.message("rmdir "+path);
      }
    }

    // --------------------------------------------------
    public void scanFolder()
    throws IOException
    {
      log.info("Scan Folder "+storageName/*+" "+rootFolder*/);

      LinkedList<Folder> origFolders = folders;
      folders = new LinkedList<Folder>();
      LinkedList<Path> folderList = new LinkedList<>();
      folderList.add(Paths.get("."));
      while ( folderList.size() > 0 ) {
	Path rel = folderList.remove();

	log.debug("new Folder("+rel+")");
	Folder folder = new Folder(rel);
	System.out.println("folder = "+folder.folderPath);
	registerToList(folders,folder);
	nextPath:
	for ( Path relpath : pathList(rel) ) {
	  if ( isSymbolicLink(relpath) ) {
	    log.info("ignore symbolic link : "+relpath);
	    continue nextPath;
	  } else if ( isDirectory(relpath) ) {
	    log.debug("scan folder "+relpath);
	    for ( Pattern pat : ignoreFolderPats ) {
	      if ( pat.matcher(relpath.toString()).matches() ) {
		//log.info("ignore folder "+relpath);
		continue nextPath;
	      }
	    }
	    folderList.add(relpath);
	  } else {
	    log.debug("scan file "+relpath);
	    for ( Pattern pat : ignoreFilePats ) {
	      if ( pat.matcher(relpath.getFileName().toString()).matches() ) {
		//log.info("ignore file "+relpath);
		continue nextPath;
	      }
	    }
	    log.debug("new File("+relpath+")");
	    File file = new File(relpath);
	    registerToList(folder.files,file);
	    file.length = getSize(relpath);
	    file.lastModified = getLastModified(relpath);
	    Folder origfolder = null;
	    File origfile = null;
	    if ( 
	      origFolders != null &&
	      (origfolder = findFromList(origFolders,folder.folderPath)) != null &&
	      (origfile = findFromList(origfolder.files,relpath)) != null &&
	      file.length == origfile.length &&
	      file.lastModified == origfile.lastModified
	    ) {
	      file.hashValue = origfile.hashValue;
	    } else {
	      file.hashValue = getMD5(relpath);
	    }
	  }
	}
      }
    }
    
    // --------------------------------------------------
    public void dump( PrintStream out )
    {
      for ( Folder folder : folders ) {
	out.println(folder.folderPath.toString());
	for ( File file : folder.files ) {
	  file.dump(out);
	}
      }
    }

    // --------------------------------------------------
    public String getMD5( Path path )
    throws IOException
    {
      log.info("Calculate MD5 "+path);

      digest.reset();
      try ( InputStream in = newInputStream(path) )
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

  public static <T extends PathHolder> void registerToList( LinkedList<T> list, T item )
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

  public static <T extends PathHolder> T findFromList( LinkedList<T> list, Path path )
  {
    for ( T item : list ) {
      if ( item.getPath().equals(path) ) return item;
    }
    return null;
  }

  public static <T extends PathHolder> T deleteFromList( LinkedList<T> list, Path path )
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
	storage = new LocalStorage(this,key,path);
	this.put(storage.storageName,storage);
	storage.storageName = key;
	log.info("Read Config "+key+"="+path);
      }
    } catch ( IOException ex ) {
      log.error(ex);
    }
  }
}
