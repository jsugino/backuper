package mylib.backuper;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

@SuppressWarnings("serial")
public class DataBase extends HashMap<String,DataBase.Storage> implements Closeable
{
  // ======================================================================
  private final static Logger log = LoggerFactory.getLogger(DataBase.class);

  public final static SimpleDateFormat STDFORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
  public final static SimpleDateFormat TFORM = new SimpleDateFormat("yyyyMMddHHmmss");

  // ======================================================================
  public abstract class Storage implements Closeable
  {
    //private String driveName;
    public String storageName;
    private LinkedList<Folder> folders = null;		// 全てのフォルダのリスト
    private LinkedList<Pattern> ignoreFilePats = new LinkedList<>();
    private LinkedList<Pattern> ignoreFolderPats = new LinkedList<>();
    public String selfHash = null;

    public Storage( String storageName )
    {
      this.storageName = storageName;
    }

    public abstract long timeUnit();

    public abstract String getRoot();

    public abstract void makeRealDirectory( Path path ) throws IOException;

    public abstract InputStream newInputStream( Path path ) throws IOException;

    public abstract OutputStream newOutputStream( Path path ) throws IOException;

    public abstract void setRealLastModified( Path path, long time ) throws IOException;

    public abstract List<PathHolder> getPathHolderList( Path path ) throws IOException;

    public abstract void deleteRealFile( Path path ) throws IOException;

    public abstract void deleteRealFolder( Path path ) throws IOException;

    public abstract void moveRealFile( Path fromPath, Path toPath ) throws IOException;

    // --------------------------------------------------
    public Folder getFolder( Path path )
    throws IOException
    {
      return getFolder(path,null);
    }

    public Folder getFolder( Path path, String name )
    throws IOException
    {
      log.trace("getFolder path = "+path+", name = "+name);
      if ( path == null ) path = Paths.get(".");
      Folder folder = findFromList(folders,path);
      if ( folder == null ) {
	if ( name == null ) return null;
	Folder parent = getFolder(path.getParent(),name);
	if ( parent == null ) return null;
	if ( parent.ignores.contains(path) ) return null;
	folder = new Folder(path);
	registerToList(folders,folder);
	log.info("mkdir "+name+" "+path);
	makeRealDirectory(path);
      }
      return folder;
    }

    public int folderSize()
    {
      return this.folders.size();
    }

    public int fileSize()
    {
      return this.folders.stream().mapToInt(f->f.files.size()).sum();
    }

    public LinkedList<File> getAllFiles()
    {
      return this.folders.stream().flatMap(f->f.files.stream()).collect(Collectors.toCollection(LinkedList::new));
    }

    public void addIgnore( String line )
    {
      line = line.trim();
      if ( line.length() == 0 ) return;
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
	log.trace("ignore folder pattern : "+pat);
	this.ignoreFolderPats.add(pat);
      } else {
	line = line.replaceAll("\\.","\\\\.");
	line = line.replaceAll("\\*","[^/]+");
	if ( line.charAt(0) == '/' ) {
	  line = line.substring(1);
	} else {
	  line = "(.+/)?"+line;
	}
	Pattern pat = Pattern.compile(line);
	log.trace("ignore file pattern : "+pat);
	this.ignoreFilePats.add(pat);
      }
    }

    // --------------------------------------------------
    public void readDB()
    throws IOException
    {
      Path dbpath = dbFolder.resolve(storageName+".db");
      log.debug("Read DataBase "+storageName+' '+dbpath);

      List<String> list;
      try {
	list = Files.readAllLines(dbpath);
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
	  log.trace("readDB new folder "+folder.folderPath);
	  this.folders.add(folder);
	} else {
	  int i2 = line.indexOf('\t',i1+1);
	  int i3 = line.indexOf('\t',i2+1);
	  File file = new File(
	    folder.folderPath.equals(curPath)
	    ? Paths.get(line.substring(i3+1))
	    : folder.folderPath.resolve(line.substring(i3+1)));
	  log.trace("readDB new file "+file.filePath);
	  folder.files.add(file);
	  log.trace("readDB read "+file.filePath);
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
      this.selfHash = calcSelfMD5();
    }

    public void writeDB()
    throws IOException
    {
      Path dbpath = dbFolder.resolve(storageName+".db");
      log.debug("Write DataBase "+storageName+' '+dbpath);
      if ( calcSelfMD5().equals(this.selfHash) ) {
	log.info("Unchanged DataBase "+storageName+' '+dbpath);
	return;
      }

      Path fpath = findDBFilePath();
      long now = System.currentTimeMillis();
      Path dpath = Paths.get(this.getRoot())
	.relativize(
	  DataBase.this.dbFolder.resolve(this.storageName+".db")
	  .toAbsolutePath())
	.normalize();
      if ( !dpath.startsWith("..") ) {
	Path dp = dpath;
	folders.stream()
	  .flatMap(folder->folder.files.stream())
	  .filter(file->file.filePath.equals(dp))
	  .forEach(file->file.lastModified = now);
      } else {
	dpath = null;
      }
      this.selfHash = calcSelfMD5();

      try ( PrintStream out = new PrintStream(Files.newOutputStream(dbpath)) ) {
	for ( Folder folder : folders ) {
	  if ( folder.files.size() == 0 ) continue;
	  out.println(folder.folderPath.toString());
	  for ( File file : folder.files ) {
	    if ( file.hashValue == null ) continue;
	    if ( file.filePath.equals(fpath) ) {
	      String orig = file.hashValue;
	      file.hashValue = "0000000000000000000000";
	      file.dump(out);
	      file.hashValue = orig;
	    } else {
	      file.dump(out);
	    }
	  }
	}
      }
    }

    // --------------------------------------------------
    public String copyRealFile( Path filePath, Storage dstStorage )
    throws IOException
    {
      @SuppressWarnings("resource")
      Storage srcStorage = this;
      log.trace("copyRealFile "+filePath
	+", srcStorage = "+srcStorage.storageName
	+", dstStorage = "+dstStorage.storageName);

      digest.reset();
      try ( 
	InputStream  in  = srcStorage.newInputStream(filePath);
	OutputStream out = dstStorage.newOutputStream(filePath);
      ) {
	byte buf[] = new byte[1024*64];
	int len;
	while ( (len = in.read(buf)) > 0 ) {
	  out.write(buf,0,len);
	  digest.update(buf,0,len);
	}
      }
      return getDigestString();
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

    public void moveFile( Path srcPath, Path dstPath )
    throws IOException
    {
      log.trace("moveFile "+this.storageName+", srcPath = "+srcPath+", dstPath = "+dstPath);

      Path srcParent = srcPath.getParent();
      if ( srcParent == null ) srcParent = Paths.get(".");
      Folder srcFolder = this.getFolder(srcParent,this.storageName);
      if ( srcFolder == null ) { log.error("CANNOT FIND DIR "+srcParent); return; }
      File srcFile = findFromList(srcFolder.files,srcPath);

      Path dstParent = dstPath.getParent();
      if ( dstParent == null ) dstParent = Paths.get(".");
      Folder dstFolder = this.getFolder(dstParent,this.storageName);
      if ( dstFolder == null ) { log.error("CANNOT FIND DIR "+dstParent); return; }
      File dstFile = findFromList(dstFolder.files,dstPath);
      if ( dstFile != null ) throw new IOException("already existed "+dstFile.filePath);

      deleteFromList(srcFolder.files,srcPath);
      (dstFile = srcFile).filePath = dstPath;
      registerToList(dstFolder.files,dstFile);

      log.info("move "+srcPath+" "+dstPath);
      this.moveRealFile(srcPath,dstPath);
    }

    public void moveHistoryFile( Path filePath, Storage dstStorage )
    throws IOException
    {
      @SuppressWarnings("resource")
      Storage srcStorage = this;
      log.trace("moveHistoryFile "+filePath
	+", srcStorage = "+srcStorage.storageName
	+", dstStorage = "+dstStorage.storageName);
      Path parentPath = filePath.getParent();
      if ( parentPath == null ) parentPath = Paths.get(".");
      Folder srcFolder = findFromList(srcStorage.folders,parentPath);
      File   srcFile   = findFromList(srcFolder.files,filePath);
      Folder dstFolder = dstStorage.getFolder(parentPath,dstStorage.storageName);
      Path   histPath  = toHistPath(filePath,srcFile.lastModified);
      File   dstFile   = findFromList(dstFolder.files,histPath);
      if ( dstFile != null ) throw new IOException("already existed "+dstFile.filePath);

      deleteFromList(srcFolder.files,srcFile.filePath);
      (dstFile = srcFile).filePath = histPath;
      registerToList(dstFolder.files,dstFile);

      Path newPath = Paths.get(srcStorage.getRoot()).relativize(Paths.get(dstStorage.getRoot())).resolve(histPath);
      log.info("move "+filePath+" "+dstStorage.storageName+" "+histPath);
      srcStorage.moveRealFile(filePath,newPath);
    }

    public Path toHistPath( Path filePath, long lastModified )
    {
      Path parentPath = filePath.getParent();
      String name = filePath.getFileName().toString();
      int idx = name.lastIndexOf('.');
      String time = TFORM.format(new Date(lastModified));
      name = ( idx <= 0 ) ? name+'-'+time : name.substring(0,idx)+'-'+time+name.substring(idx);
      return parentPath == null ? Paths.get(name) : parentPath.resolve(name);
    }

    /**
     * folders から空のフォルダを削除する。
     * @throws IOException
     */
    public void cleanupFolder()
    throws IOException
    {
      HashMap<Path,Integer> map = new HashMap<>();
      for ( Folder folder : this.folders ) {
	int n = folder.ignores.size() + folder.files.size();
	for ( Path p = folder.folderPath; p != null; p = p.getParent() ) {
	  map.put(p,map.getOrDefault(p,0)+n);
	}
      }
      for ( ListIterator<Folder> itr = this.folders.listIterator(this.folders.size()); itr.hasPrevious(); ) {
	Folder folder = itr.previous();
	Path path = folder.folderPath;
	if ( map.get(path) == 0 && !path.equals(Paths.get(".")) ) {
	  log.info("rmdir "+this.storageName+" "+path);
	  deleteRealFolder(path);
	  itr.remove();
	}
      }
    }

    // --------------------------------------------------
    /**
     * フォルダを Scan する。
     * - 次のものは無視され、その Folder ignores に追加される。
     *   - ignoreFolderPats にマッチするフォルダ
     *   - ignoreFilePats にマッチするファイル
     *   - シンボリックリンク
     * - 次のいずれかの場合、元の MD5 値は用いられない。(nullとなる)
     *   - *.db 上にファイルが無い
     *   - ファイル長が異なる
     *   - checkLastModified でかつ、最終更新日時が異なる (厳密一致)
     * - 次の全てがそろった場合、例外的に元の MD5 値が用いられる。
     *   - useOldHashValue のとき
     *   - *.db 上にはあるが、実ファイルは削除されている。(移動されたのかも)
     *   - 次のファイルプロパティが一致。(移動されたとする)
     *     - ファイル名(フォルダパスは無視)
     *     - 最終更新日時
     *     - ファイル長
     **/
    public void scanFolder( boolean checkLastModified, boolean useOldHashValue )
    throws IOException
    {
      log.info("Scan Folder "+storageName+' '+getRoot());

      LinkedList<Folder> origFolders = this.folders;
      this.folders = new LinkedList<Folder>();
      LinkedList<Folder> folderList = new LinkedList<>();
      folderList.add(new Folder(Paths.get(".")));
      PeriodicLogger period = new PeriodicLogger();
      while ( folderList.size() > 0 ) {
	Folder folder = folderList.remove();
	log.trace("scanFolder new folder "+folder.folderPath);
	registerToList(this.folders,folder);
	nextPath:
	for ( PathHolder holder : getPathHolderList(folder.folderPath) ) {
	  Path relpath = holder.getPath();
	  period.logging(relpath);
	  if ( holder instanceof Folder ) {
	    log.trace("scanFolder scan folder "+relpath);
	    for ( Pattern pat : ignoreFolderPats ) {
	      if ( pat.matcher(relpath.toString()).matches() ) {
		log.debug("Ignore folder "+relpath);
		folder.ignores.add(relpath);
		continue nextPath;
	      }
	    }
	    folderList.add((Folder)holder);
	  } else if ( holder instanceof File ) {
	    File file = (File)holder;
	    if ( file.type == File.FileType.SYMLINK ) {
	      log.debug("Ignore symlink "+holder.getPath());
	      folder.ignores.add(holder.getPath());
	      continue nextPath;
	    }
	    log.trace("scanFolder scan file "+relpath);
	    for ( Pattern pat : ignoreFilePats ) {
	      if ( pat.matcher(relpath.getFileName().toString()).matches() ) {
		log.debug("Ignore file "+relpath);
		folder.ignores.add(relpath);
		continue nextPath;
	      }
	    }
	    log.trace("scanFolder new file "+relpath);
	    registerToList(folder.files,file);
	    Folder origfolder = null;
	    File origfile = null;
	    if ( 
	      origFolders != null &&
	      (origfolder = findFromList(origFolders,folder.folderPath)) != null &&
	      (origfile = findFromList(origfolder.files,relpath)) != null &&
	      file.length == origfile.length &&
	      (!checkLastModified || file.lastModified == origfile.lastModified)
	    ) {
	      file.hashValue = origfile.hashValue;
	      origfile.hashValue = null;
	    }
	  } else {
	    log.error("MUST NOT OCCUR : "+holder);
	  }
	}
      }
      if ( origFolders != null && useOldHashValue ) {
	HashMap<Datum,String> map = new HashMap<>();
	origFolders.stream()
	  .flatMap(f->f.files.stream())
	  .filter(f->f.hashValue != null)
	  .forEach(f->map.put(new Datum(f),f.hashValue));
	this.folders.stream()
	  .flatMap(f->f.files.stream())
	  .filter(f->f.hashValue == null)
	  .filter(f->(f.hashValue = map.get(new Datum(f))) != null)
	  .forEach(f->log.debug("Reuse MD5 "+f.filePath));
      }
      period.last();
    }

    /**
     * 親フォルダを補完する。
     * *.db の書き出し時には、ファイルを持たないフォルダは保存されない。
     * そのため、readDB() 後には、scanFolder() か complementFolders() を呼び出す必要がある。
     **/
    public void complementFolders()
    {
      HashSet<Path> exists = new HashSet<>();
      LinkedList<Folder> newFolders = new LinkedList<>();
      for ( Folder folder : folders ) {
	Path path = folder.folderPath;
	while ( true ) {
	  exists.add(path);
	  path = path.getParent();
	  if ( path == null || exists.contains(path) ) break;
	  newFolders.add(new Folder(path));
	}
      }
      if ( findFromList(folders,Paths.get(".")) == null )
	newFolders.add(new Folder(Paths.get(".")));
      for ( Folder folder : newFolders ) {
	log.trace("complement folder : "+folder.folderPath);
	registerToList(folders,folder);
      }
    }

    /**
     * ハッシュ値がnullのファイルを読み込み、MD5値を計算する。
     **/
    public void updateHashvalue( boolean forceRead )
    throws IOException
    {
      PeriodicLogger period = new PeriodicLogger();
      for ( Folder folder : folders ) {
	for ( File file : folder.files ) {
	  if ( forceRead ) period.logging(file.filePath);
	  if ( file.hashValue == null || forceRead ) {
	    String orig = file.hashValue;
	    file.hashValue = getMD5(file.filePath,forceRead);
	    if ( orig != null && !file.hashValue.equals(orig) ) {
	      log.warn("MD5 is different "+file.filePath);
	    }
	  }
	}
      }
      if ( forceRead ) period.last();
    }

    // --------------------------------------------------
    public void dump( PrintStream out )
    {
      for ( Folder folder : folders ) {
	out.println(folder.folderPath.toString()+'\t'+folder.ignores.size());
	for ( File file : folder.files ) {
	  file.dump(out);
	}
      }
    }

    // --------------------------------------------------
    public String getMD5( Path path, boolean forceRead )
    throws IOException
    {
      if ( forceRead ) {
	log.trace("calculate MD5 "+storageName+" "+path);
      } else {
	log.info("calculate MD5 "+storageName+" "+path);
      }
      digest.reset();
      try ( InputStream in = newInputStream(path) )
      {
	byte buf[] = new byte[1024*64];
	int len;
	while ( (len = in.read(buf)) > 0 ) {
	  digest.update(buf,0,len);
	}
      }
      return getDigestString();
    }

    public String calcSelfMD5()
    {
      Path root = Paths.get(this.getRoot());
      Path dbpath = findDBFilePath();
      /*
      HashSet<Path> dbpaths = new HashSet<>();
      DataBase.this.forEach((k,v)->dbpaths.add(
	  root.relativize(dbFolder.resolve(k+".db").toAbsolutePath())));
      */
      //System.out.println("dbpaths = "+dbpaths);
      //System.out.println("root = "+this.getRoot());
      digest.reset();
      try ( PrintStream out = new PrintStream(new OutputStream(){
	  public void write( int b ) { digest.update((byte)b); }
	}) ) {
	for ( Folder folder : folders ) {
	  if ( folder.files.size() == 0 ) continue;
	  out.println(folder.folderPath.toString());
	  for ( File file : folder.files ) {
	    if ( file.hashValue == null ) continue;
	    //System.out.println("file = "+file);
	    if ( file.filePath.equals(dbpath) ) {
	      String orig = file.hashValue;
	      //System.out.println("replace ("+file.filePath+") = "+orig);
	      file.hashValue = "0000000000000000000000";
	      file.dump(out);
	      file.hashValue = orig;
	    } else {
	      file.dump(out);
	    }
	  }
	}
      }
      return getDigestString();
    }

    public String getDigestString()
    {
      String str = Base64.getEncoder().encodeToString(digest.digest());
      int idx = str.indexOf('=');
      if ( idx > 0 ) str = str.substring(0,idx);
      return str;
    }

    @Override
    public String toString()
    {
      StringBuffer buf = new StringBuffer(storageName);
      buf.append('=').append(getRoot());
      if ( folders != null )
	buf.append(" (").append(fileSize()).append(" files)");
      return buf.toString();
    }
  }

  public static class Datum
  {
    public String filename;
    public long lastModified;
    public long length;

    public Datum( File file )
    {
      this(file.filePath.getFileName().toString(),file.lastModified,file.length);
    }

    public Datum( String filename, long lastModified, long length )
    {
      this.filename = filename;
      this.lastModified = lastModified;
      this.length = length;
    }

    @Override
    public boolean equals( Object other )
    {
      Datum oth;
      return
	other instanceof Datum &&
	(oth = (Datum)other).filename.equals(this.filename) &&
	oth.lastModified == this.lastModified &&
	oth.length == this.length;
    }

    @Override
    public int hashCode()
    {
      return filename.hashCode() + (int)lastModified + (int)length;
    }
  }

  public static class Folder implements PathHolder
  {
    public Path folderPath;
    public LinkedList<File> files = new LinkedList<>();
    public LinkedList<Path> ignores = new LinkedList<>();

    public Folder( Path folderPath )
    {
      this.folderPath = folderPath;
    }

    @Override
    public Path getPath()
    {
      return folderPath;
    }

    @Override
    public String toString()
    {
      return folderPath.toString()+'('+files.size()+','+ignores.size()+')';
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
      this.type = FileType.NORMAL;
    }

    public File( Path filePath, long lastModified, long length )
    {
      this.filePath = filePath;
      this.type = FileType.NORMAL;
      this.lastModified = lastModified;
      this.length = length;
    }

    public File( File orig )
    {
      this.filePath = orig.filePath;
      this.type = orig.type;
      this.hashValue = orig.hashValue;
      this.lastModified = orig.lastModified;
      this.length = orig.length;
    }

    @Override
    public Path getPath()
    {
      return filePath;
    }

    public void dump( PrintStream out )
    {
      out.println(hashValue+'\t'+STDFORMAT.format(new Date(lastModified))+'\t'+length+'\t'+filePath.getFileName());
    }

    @Override
    public String toString()
    {
      return filePath.toString()+'('+hashValue+','+STDFORMAT.format(new Date(lastModified))+','+length+')';
    }

    @Override
    public boolean equals( Object obj )
    {
      if ( obj == null || !(obj instanceof File) ) return false;
      File oth = (File)obj;
      return
	this.filePath.equals(oth.filePath) &&
	this.type == oth.type &&
	this.hashValue.equals(oth.hashValue) &&
	this.lastModified == oth.lastModified &&
	this.length == oth.length;
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
  public Path dbFolder;
  public MessageDigest digest;

  public DataBase( Path dbFolder )
  throws IOException
  {
    this.dbFolder = dbFolder;
    try {
      digest = MessageDigest.getInstance("MD5");
    } catch ( NoSuchAlgorithmException ex ) {
      throw new IOException("Cannot initialize 'digest MD5'",ex);
    }
  }

  @Override
  public void close()
  throws IOException
  {
    for ( Storage storage : values() ) storage.close();
  }

  // --------------------------------------------------
  public DoubleKeyHashMap<String,String,String> toMap()
  {
    DoubleKeyHashMap<String,String,String> map = new DoubleKeyHashMap<>();
    this.forEach((key,val)->{
	int idx = key.lastIndexOf('.');
	String key1 = key.substring(0,idx);
	String key2 = key.substring(idx+1);
	map.put(key1,key2,val.getRoot());
      });

    /**
     * データ構造
     *
     *    ドライブ名 → key2[0]   key2[1]  ...  key2[m]
     * フォルダの先頭→ map2[0]   map2[1]  ...  map2[m]
     * key1[0] map1[0] arr[0][0] arr[0][1] ... arr[0][m]
     * key1[1] map1[1] arr[1][0] arr[1][1] ... arr[1][m]
     *  ...     ...      ...       ...     ...   ...
     * key1[n] map1[n] arr[n][0] arr[n][1] ... arr[n][m]
     *   ↑     ↑
     *   ↑     フォルダの後尾
     *   Storage の共通名 (Common, myhome.junsei など)
     **/
    HashMap<String,String> map1  = new HashMap<>();
    HashMap<String,String> map2  = new HashMap<>();

    map.forEach((key,val)->{
	String key1 = key.key1;
	String val1 = map1.get(key1);
	val = commonTail(val1,val);
	int idx = val.indexOf('/');
	val = idx < 0 ? "" : val.substring(idx);
	/*
	idx = val.indexOf("home/"); // TODO : home だけを特別扱いしない。
	if ( idx > 0 ) val = val.substring(idx+4);
	*/
	int len = val.length();
	map1.put(key1,val);
      });
    map.forEach((key,val)->map.put(key,val.substring(0,val.length()-map1.get(key.key1).length())));

    map.forEach((key,val)->{
	String key2 = key.key2;
	String val2 = map2.get(key2);
	val = commonHead(val2,val);
	int len = val.length();
	if ( len > 0 && val.charAt(len-1) == '/' ) val = val.substring(0,--len);
	map2.put(key2,val);
      });
    map.forEach((key,val)->map.put(key,val.substring(map2.get(key.key2).length())));

    map1.forEach((key1,val1)->map.put(key1,"",val1));
    map2.forEach((key2,val2)->map.put("",key2,val2));
    return map;
  }

  public void printDataBaseAsCsv( PrintStream out )
  {
    DoubleKeyHashMap<String,String,String> map = this.toMap();
    for ( String key2 : map.key2Set() ) {
      out.print(","+key2);
    }
    out.println();
    for ( String key1 : map.key1Set() ) {
      out.print(key1);
      for ( String key2 : map.key2Set() ) {
	String val = map.get(key1,key2);
	val =
	  val == null ? "" :
	  val.length() == 0 ? "/." :
	  val;
	out.print(","+val);
      }
      out.println();
    }
  }

  public static String commonHead( String str1, String str2 )
  {
    if ( str1 == null ) return str2;
    if ( str2 == null ) return str1;
    int len1 = str1.length();
    int len2 = str2.length();
    if ( len2 < len1 ) {
      String str = str1; str1 = str2; str2 = str;
      int    len = len1; len1 = len2; len2 = len;
    }
    for ( int i = 0; i < len1; ++i ) {
      if ( str1.charAt(i) != str2.charAt(i) ) {
	str1 = str1.substring(0,i);
	break;
      }
    }
    return str1;
  }

  public static String commonTail( String str1, String str2 )
  {
    if ( str1 == null ) return str2;
    if ( str2 == null ) return str1;
    int len1 = str1.length();
    int len2 = str2.length();
    if ( len2 < len1 ) {
      String str = str1; str1 = str2; str2 = str;
      int    len = len1; len1 = len2; len2 = len;
    }
    for ( int i = 0; i < len1; ++i ) {
      if ( str1.charAt(len1-i-1) != str2.charAt(len2-i-1) ) {
	str1 = str1.substring(len1-i);
	break;
      }
    }
    return str1;
  }

  // --------------------------------------------------

  public void printStorages( PrintStream out )
  {
    String keys[] = this.keySet().toArray(new String[0]);
    Arrays.sort(keys);
    for ( String key : keys ) {
      Storage storage = this.get(key);
      if ( storage.folders == null ) {
	out.format("%-20s (no *.db file) %s",
	  storage.storageName,
	  storage.getRoot()).println();
      } else {
	out.format("%-20s %6d %7d %s",
	  storage.storageName,
	  storage.folderSize(),
	  storage.fileSize(),
	  storage.getRoot()).println();
      }
    }
  }

  // ======================================================================
  public void initializeByFile( Path descFilePath )
  throws IOException
  {
    log.trace("initializeByFile : "+descFilePath);

    Storage storage = null;
    for ( String line : Files.readAllLines(descFilePath) ) {
      if ( line.length() == 0 || line.startsWith("##") ) return;
      int idx = line.indexOf('=');
      if ( idx <= 0 ) { storage.addIgnore(line); continue; }
      String key = line.substring(0,idx).trim();
      String defstr = line.substring(idx+1).trim();
      if ( defstr.startsWith("ftp://") ) {
	storage = new FtpStorage(this,key,defstr);
      } else {
	Path path = Paths.get(defstr);
	if ( !path.isAbsolute() ) {
	  log.error("Not Absolute Path : "+line);
	  return;
	}
	storage = new LocalStorage(this,key,path);
      }
      this.put(storage.storageName,storage);
      log.trace("Read Config "+key+"="+defstr);
    }
  }

  public Backup initializeByXml( Path descFilePath )
  throws IOException
  {
    Backup backup = new Backup();
    try {
      log.trace("initializeByXml : "+descFilePath);

      HashMap<String,String[]> folderdefMap = new HashMap<>();

      Document xmldoc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(descFilePath.toFile());
      NodeList children = xmldoc.getChildNodes().item(0).getChildNodes();
      log.trace("children.length = "+children.getLength());
      for ( int i = 0; i < children.getLength(); ++i ) {
	Element elem = selectElement(children.item(i));
	if ( elem == null ) continue;
	String tagname = elem.getTagName();
	if ( tagname.equals("folderdef") ) {
	  String id = getAttr(elem,"id");
	  log.trace("find folderdef : id = "+id);
	  if ( id == null ) { nodeerror("no id for folderdef",elem); continue; }
	  NodeList deflist = elem.getChildNodes();
	  LinkedList<String> list = new LinkedList<>();
	  for ( int j = 0; j < deflist.getLength(); ++j ) {
	    Element folder = selectElement(deflist.item(j));
	    if ( folder == null ) continue;
	    if ( !folder.getTagName().equals("folder") ) { unknown(folder); continue; }
	    String dir = getAttr(folder,"dir");
	    if ( dir == null ) { attrerror("dir",folder); continue; }
	    String name = getAttr(folder,"name");
	    list.add(dir);
	    list.add(name);
	  }
	  folderdefMap.put(id,list.toArray(new String[0]));
	} else if ( tagname.equals("storage") ) {
	  log.trace("storage = "+serialize(elem));
	  if ( elem.getAttributeNode("ftp") != null ) {
	    String userid = getAttr(elem,"user");
	    if ( userid == null ) { attrerror("user",elem); continue; }
	    String password = getAttr(elem,"password");
	    if ( password == null ) { attrerror("password",elem); continue; }
	    String hostname = getAttr(elem,"ftp");
	    if ( hostname == null ) { attrerror("ftp",elem); continue; }
	    String name = getAttr(elem,"name");
	    if ( name == null ) { attrerror("name",elem); continue; }
	    parseFolders(elem.getChildNodes(),name,"",null,folderdefMap,null,(n,p)->
	      new FtpStorage(this,n,userid,password,hostname,p.normalize().toString()));
	  } else {
	    String dir = getAttr(elem,"dir");
	    if ( dir == null || dir.length() == 0 ) { attrerror("dir",elem); continue; }
	    String name = getAttr(elem,"name");
	    if ( name == null || name.length() == 0 ) { attrerror("name",elem); continue; }
	    log.trace(name+'='+dir);
	    if ( dir.charAt(dir.length()-1) != '/' ) dir = dir+'/';
	    parseFolders(elem.getChildNodes(),name,dir,null,folderdefMap,null,(n,p)->new LocalStorage(this,n,p));
	  }
	} else if ( tagname.equals("backup") ) {
	  backup.registerElem(this,elem,folderdefMap);
	} else {
	  unknown(elem);
	}
      }
    } catch ( ParserConfigurationException | SAXException | TransformerException ex ) {
      log.error(ex.getMessage(),ex);
      throw new IOException(ex.getMessage(),ex);
    }
    return backup;
  }

  public int parseFolders(
    NodeList list, String driveName, String parentDir, String parentName,
    HashMap<String,String[]> folderdefMap, Storage curentStorage, BiFunction<String,Path,Storage> newFunc
  )
  throws IOException, TransformerException
  {
    int subfolders = 0;
    for ( int i = 0; i < list.getLength(); ++i ) {
      Element folder = selectElement(list.item(i));
      if ( folder == null ) continue;
      if ( folder.getTagName().equals("excludes") ) {
	BufferedReader in = new BufferedReader(new StringReader(folder.getTextContent()));
	String line;
	while ( (line = in.readLine()) != null ) curentStorage.addIgnore(line);
	in.close();
	continue;
      }
      if ( !folder.getTagName().equals("folder") ) { unknown(folder); continue; }
      log.trace("folder = "+serialize(folder));
      if ( folder.getAttributeNode("ref") != null ) {
	String defs[] = folderdefMap.get(getAttr(folder,"ref"));
	if ( defs == null ) { nodeerror("No reference",folder); continue; }
	for ( int j = 0; j < defs.length; j += 2 ) {
	  String newName = calcName(parentName,defs[j],defs[j+1]);
	  log.trace("calcName-1("+parentName+","+defs[j]+","+defs[j+1]+")="+newName);
	  if ( newName != null ) {
	    String strName = newName+'.'+driveName;
	    this.put(strName,newFunc.apply(strName,Paths.get(parentDir+defs[j])));
	  }
	}
	if ( folder.getChildNodes().getLength() > 0 )
	  log.error("Ignore sub nodes : "+serialize(folder));
      } else {
	String dir = getAttr(folder,"dir");
	if ( dir == null ) { attrerror("dir",folder); continue; }
	String name = getAttr(folder,"name");
	log.trace("parentName = "+parentName+", name = "+name
	  +", driveName = "+driveName
	  +", parentDir = "+parentDir+", dir = "+dir);
	String newName = calcName(parentName,dir,name);
	log.trace("calcName-2("+parentName+","+dir+","+name+")="+newName);
	String newDir = parentDir+dir;
	String strName = null;
	Storage storage = null;
	if ( newName != null ) {
	  strName = newName+'.'+driveName;
	  storage = newFunc.apply(strName,Paths.get(newDir));
	  log.trace("register new storage : "+strName+"="+storage.getRoot());
	  //this.put(strName,storage);
	}
	int num = parseFolders(folder.getChildNodes(),driveName,newDir+'/',newName,folderdefMap,storage,newFunc);
	if ( num == 0 && newName != null ) {
	  this.put(strName,storage);
	  subfolders += 1;
	} else {
	  if ( name != null ) log.warn("Ignore name attribute ("+name+") of dir="+dir,new Exception("Ignore name attribute"));
	  if ( storage != null && (storage.ignoreFilePats.size() > 0 || storage.ignoreFolderPats.size() > 0) ) {
	    log.warn("Ignore name <excludes> tag of dir="+dir,new Exception("Ignore name <excludes> tag"));
	  }
	  subfolders += num;
	}
      }
    }
    return subfolders;
  }

  public static String calcName( String parentName, String dir, String name )
  {
    if ( name == null ) {
      String strDir = dir.replace('/','.');
      return parentName == null ? strDir : parentName+'.'+strDir;
    } else if ( name.length() == 0 ) {
      log.error("name length == 0");
    } else if ( name.equals(".") ) {
      log.error("name == .");
    } else if ( name.charAt(0) == '.' ) {
      if ( parentName == null ) {
	log.error("parent name is null");
      } else {
	return parentName+name;
      }
    } else {
      return name;
    }
    return null;
  }

  public static String getAttr( Element elem, String attrName )
  {
    Attr attr = elem.getAttributeNode(attrName);
    return attr == null ? null : attr.getValue();
  }

  public static void attrerror( String attr, Node node )
  throws IOException, TransformerException
  {
    nodeerror("no attribute '"+attr+"'",node);
  }

  public static void unknown( Node node )
  throws IOException, TransformerException
  {
    nodeerror("unknown element",node);
  }

  public static void nodeerror( String message, Node node )
  throws IOException, TransformerException
  {
    log.error(message+" : "+serialize(node),new Exception(message));
  }

  public static Element selectElement( Node node )
  throws IOException, TransformerException
  {
    switch (node.getNodeType()) {
    case Node.ELEMENT_NODE:
      log.trace("find Element : "+serialize(node));
      return (Element)node;
    case Node.TEXT_NODE: break;
    case Node.COMMENT_NODE: break;
    default: unknown(node); break;
    }
    return null;
  }

  public static String serialize( Node node )
  throws IOException, TransformerException
  {
    Transformer tf = TransformerFactory.newInstance().newTransformer();
    tf.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,"yes");
    StringWriter out = new StringWriter();
    tf.transform(new DOMSource(node.cloneNode(false)), new StreamResult(out));
    return out.toString();
  }

  // --------------------------------------------------

  public boolean checkConsistency()
  {
    String roots[] = this.keySet().toArray(new String[0]);

    HashMap<String,Integer> counts = new HashMap<>();
    for ( int i = 0; i < roots.length; ++i ) {
      String str = roots[i].substring(0,roots[i].lastIndexOf('.'));
      Integer num = counts.get(str);
      counts.put(str,(num == null) ? 1 : num + 1);
      roots[i] = this.get(roots[i]).getRoot();
    }
    for ( Map.Entry<String,Integer> ent : counts.entrySet() ) {
      if ( ent.getValue() <= 1 ) {
	log.warn("Only one storage name : "+ent.getKey());
      }
    }

    Arrays.sort(roots,Comparator.comparingInt(root->root.length()));
    for ( int i = 0; i < roots.length-1; ++i ) {
      for ( int j = i+1; j < roots.length; ++j ) {
	if ( roots[j].startsWith(roots[i]) ) {
	  log.error("find nested storage: "+roots[j]+" in "+roots[i]);
	  return false;
	}
      }
    }

    return true;
  }

  public Path findDBFilePath()
  {
    return this.values().stream()
      .filter(storage->storage instanceof LocalStorage)
      .map(storage->
	Paths.get(storage.getRoot())
	.relativize(
	  dbFolder.resolve(storage.storageName+".db")
	  .toAbsolutePath())
	.normalize())
      .filter(p->!p.startsWith(".."))
      .reduce(null,(p1,p2)->p2);
  }

  // ==================================================
  public static class PeriodicLogger
  {
    public int counter;
    public int lastcnt;
    public long startTime;

    public PeriodicLogger()
    {
      counter = 0;
      lastcnt = -1;
      startTime = System.currentTimeMillis();
    }

    public void logging( Path path )
    {
      ++counter;
      if ( (System.currentTimeMillis()-startTime) > 5000L ) {
	if ( lastcnt < 0 ) lastcnt = 0;
	System.err.println(""+counter+" (+"+(counter-lastcnt)+") : "+path);
	startTime += 5000L;
	lastcnt = counter;
      }
    }

    public void last()
    {
      if ( lastcnt > 0 ) System.err.println(""+counter+" (last)");
    }
  }
}
