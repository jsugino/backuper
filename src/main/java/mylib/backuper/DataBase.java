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
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataBase extends HashMap<String,DataBase.Storage>
{
  // ======================================================================
  private final static Logger log = LoggerFactory.getLogger(DataBase.class);

  public final static SimpleDateFormat STDFORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

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

    public abstract List<PathHolder> getPathHolderList( Path path ) throws IOException;

    public abstract boolean deleteRealFile( Path path ) throws IOException;

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

      List<String> list;
      try {
	list = Files.readAllLines(dbFolder.resolve(storageName+".db"));
      } catch ( NoSuchFileException ex ) {
	log.warn(ex.getClass().getName()+": "+ex.getMessage());
	return;
      }
      this.folders = new LinkedList<Folder>();
      Folder folder = null;
      Path curPath = Paths.get(".");
      for ( String line : list ) {
	int i1;
	if ( (i1 = line.indexOf('\t')) < 0 ) {
	  folder = new Folder(Paths.get(line));
	  log.debug("new folder "+folder.folderPath);
	  this.folders.add(folder);
	} else {
	  int i2 = line.indexOf('\t',i1+1);
	  int i3 = line.indexOf('\t',i2+1);
	  File file = new File(
	    folder.folderPath.equals(curPath)
	    ? Paths.get(line.substring(i3+1))
	    : folder.folderPath.resolve(line.substring(i3+1)));
	  log.debug("new file "+file.filePath);
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
      log.debug("copyFile "+filePath);
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

      log.info(command+filePath);
      if ( dstStorage.mkParentDir(dstFile.filePath) ) {
	log.info("mkdir "+dstFile.filePath.getParent());
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

      log.info("set lastModified "+dstFile.filePath);
      dstFile.lastModified = srcFile.lastModified;
      dstStorage.setLastModified(dstFile.filePath,dstFile.lastModified);
    }

    public void deleteFile( Path delPath )
    throws IOException
    {
      log.info("delete "+delPath);
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
	if ( deleteRealFile(path) ) log.info("rmdir "+path);
      }
    }

    // --------------------------------------------------
    public void scanFolder()
    throws IOException
    {
      log.info("Scan Folder "+storageName);
      log.debug("folder name "+getRoot());

      LinkedList<Folder> origFolders = folders;
      folders = new LinkedList<Folder>();
      LinkedList<Folder> folderList = new LinkedList<>();
      folderList.add(new Folder(Paths.get(".")));
      while ( folderList.size() > 0 ) {
	Folder folder = folderList.remove();
	log.debug("new Folder "+folder.folderPath);
	registerToList(folders,folder);
	nextPath:
	for ( PathHolder holder : getPathHolderList(folder.folderPath) ) {
	  Path relpath = holder.getPath();
	  if ( holder instanceof Folder ) {
	    log.debug("scan folder "+relpath);
	    for ( Pattern pat : ignoreFolderPats ) {
	      if ( pat.matcher(relpath.toString()).matches() ) {
		log.info("Ignore folder "+relpath);
		continue nextPath;
	      }
	    }
	    folderList.add((Folder)holder);
	  } else if ( holder instanceof File ) {
	    File file = (File)holder;
	    if ( file.type == File.FileType.SYMLINK ) {
	      log.info("ignore symlink "+holder.getPath());
	      continue nextPath;
	    }
	    log.debug("scan file "+relpath);
	    for ( Pattern pat : ignoreFilePats ) {
	      if ( pat.matcher(relpath.getFileName().toString()).matches() ) {
		log.info("ignore file "+relpath);
		continue nextPath;
	      }
	    }
	    log.debug("new File "+relpath);
	    registerToList(folder.files,file);
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
      log.info("calculate MD5 "+path);

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
    public FileType type;
    public String hashValue;
    public long lastModified;
    public long length;

    public enum FileType {
      NORMAL,
      SYMLINK;
    }

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
	return;
      }
    }
    list.add(item);
  }

  public static <T extends PathHolder> T findFromList( LinkedList<T> list, Path path )
  {
    for ( T item : list ) {
      if ( item.getPath().equals(path) ) return item;
    }
    return null;
  }

  public static <T extends PathHolder> boolean deleteFromList( LinkedList<T> list, Path path )
  {
    ListIterator<T> itr = list.listIterator();
    while ( itr.hasNext() ) {
      if ( itr.next().getPath().equals(path) ) {
	itr.remove();
	return true;
      }
    }
    return false;
  }

  // ======================================================================
  public final static String CONFIGNAME = "folders.conf";

  public Path dbFolder;
  public MessageDigest digest;

  public DataBase( Path dbFolder )
  {
    this.dbFolder = dbFolder;
    log.debug("Initialize DataBase "+dbFolder+'/'+CONFIGNAME);

    try {
      try {
	digest = MessageDigest.getInstance("MD5");
      } catch ( NoSuchAlgorithmException ex ) {
	throw new IOException("Cannot initialize 'digest MD5'",ex);
      }
      Storage storage = null;
      for ( String line : Files.readAllLines(dbFolder.resolve(CONFIGNAME)) ) {
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
	    log.debug("ignore folder pattern : "+pat);
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
	    log.debug("ignore file pattern : "+pat);
	    storage.ignoreFilePats.add(pat);
	  }
	  continue;
	}
	String key = line.substring(0,idx).trim();
	String defstr = line.substring(idx+1).trim();
	if ( defstr.startsWith("ftp://") ) {
	  int idx0 = "ftp://".length();
	  int idx1 = defstr.indexOf(':',idx0);
	  int idx3 = defstr.indexOf('/',idx1);
	  int idx2 = defstr.lastIndexOf('@',idx3);
	  String userid   = defstr.substring(idx0,idx1);
	  String password = defstr.substring(idx1+1,idx2);
	  String hostname = defstr.substring(idx2+1,idx3);
	  Path rootFolder = Paths.get(defstr.substring(idx3));
	  storage = new FtpStorage(this,key,userid,password,hostname,rootFolder);
	} else {
	  Path path = Paths.get(defstr);
	  if ( !path.isAbsolute() ) {
	    log.error("Not Absolute Path : "+line);
	    return;
	  }
	  storage = new LocalStorage(this,key,path);
	}
	this.put(storage.storageName,storage);
	log.debug("Read Config "+key+"="+defstr);
      }
    } catch ( IOException ex ) {
      log.error(ex.getMessage(),ex);
    }
  }
}
