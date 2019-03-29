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
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

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

public class DataBase extends HashMap<String,DataBase.Storage> implements Closeable
{
  // ======================================================================
  private final static Logger log = LoggerFactory.getLogger(DataBase.class);

  public final static SimpleDateFormat STDFORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
  public final static SimpleDateFormat TFORM = new SimpleDateFormat("yyyyMMddHHmmss");

  // ======================================================================
  public abstract class Storage implements Closeable
  {
    public String driveName;
    public String storageName;
    public LinkedList<Folder> folders = null;		// 全てのフォルダのリスト
    public LinkedList<Pattern> ignoreFilePats = new LinkedList<>();
    public LinkedList<Pattern> ignoreFolderPats = new LinkedList<>();

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
      log.trace("getFolder : "+path);
      if ( path == null ) path = Paths.get(".");
      Folder folder = findFromList(folders,path);
      if ( folder == null ) {
	folder = new Folder(path);
	registerToList(folders,folder);
	getFolder(path.getParent());
	name = name == null ? "" : name+" ";
	log.info("mkdir "+name+path);
	makeRealDirectory(path);
      }
      return folder;
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
	  log.trace("new folder "+folder.folderPath);
	  this.folders.add(folder);
	} else {
	  int i2 = line.indexOf('\t',i1+1);
	  int i3 = line.indexOf('\t',i2+1);
	  File file = new File(
	    folder.folderPath.equals(curPath)
	    ? Paths.get(line.substring(i3+1))
	    : folder.folderPath.resolve(line.substring(i3+1)));
	  log.trace("new file "+file.filePath);
	  folder.files.add(file);
	  log.trace("read "+file.filePath);
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
      Path dbpath = dbFolder.resolve(storageName+".db");
      log.debug("Write DataBase "+storageName+' '+dbpath);

      try ( PrintStream out = new PrintStream(Files.newOutputStream(dbpath)) ) {
	for ( Folder folder : folders ) {
	  if ( folder.files.size() == 0 ) continue;
	  out.println(folder.folderPath.toString());
	  for ( File file : folder.files ) {
	    if ( file.hashValue == null ) continue;
	    file.dump(out);
	  }
	}
      }
    }

    // --------------------------------------------------
    public String copyRealFile( Path filePath, Storage dstStorage )
    throws IOException
    {
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

    public void moveHistoryFile( Path filePath, Storage dstStorage )
    throws IOException
    {
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
      int i = 0;
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
	  log.info("rmdir "+path);
	  deleteRealFolder(path);
	  itr.remove();
	}
      }
    }

    // --------------------------------------------------
    public void scanFolder()
    throws IOException
    {
      log.info("Scan Folder "+storageName+' '+getRoot());

      LinkedList<Folder> origFolders = folders;
      folders = new LinkedList<Folder>();
      LinkedList<Folder> folderList = new LinkedList<>();
      folderList.add(new Folder(Paths.get(".")));
      int counter = 0;
      int lastcnt = 0;
      long startTime = System.currentTimeMillis();
      while ( folderList.size() > 0 ) {
	Folder folder = folderList.remove();
	log.trace("new Folder "+folder.folderPath);
	registerToList(folders,folder);
	nextPath:
	for ( PathHolder holder : getPathHolderList(folder.folderPath) ) {
	  ++counter;
	  Path relpath = holder.getPath();
	  if ( (System.currentTimeMillis()-startTime) > 5000L ) {
	    System.err.println(""+counter+" (+"+(counter-lastcnt)+") : "+relpath);
	    startTime += 5000L;
	    lastcnt = counter;
	  }
	  if ( holder instanceof Folder ) {
	    log.trace("scan folder "+relpath);
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
	    log.trace("scan file "+relpath);
	    for ( Pattern pat : ignoreFilePats ) {
	      if ( pat.matcher(relpath.getFileName().toString()).matches() ) {
		log.debug("Ignore file "+relpath);
		folder.ignores.add(relpath);
		continue nextPath;
	      }
	    }
	    log.trace("new File "+relpath);
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
	    }
	  }
	}
      }
    }

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

    public void updateHashvalue()
    throws IOException
    {
      for ( Folder folder : folders ) {
	for ( File file : folder.files ) {
	  if ( file.hashValue == null ) file.hashValue = getMD5(file.filePath);
	}
      }
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
    public String getMD5( Path path )
    throws IOException
    {
      log.info("calculate MD5 "+storageName+" "+path);

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

    public String getDigestString()
    {
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

    @Override
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
	  backup.registerElem(this,elem);
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

  public void parseFolders(
    NodeList list, String driveName, String parentDir, String parentName,
    HashMap<String,String[]> folderdefMap, Storage curentStorage, BiFunction<String,Path,Storage> newFunc
  )
  throws IOException, TransformerException
  {
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
	Storage storage = null;
	if ( newName != null ) {
	  String strName = newName+'.'+driveName;
	  storage = newFunc.apply(strName,Paths.get(newDir));
	  log.trace("register new storage : "+strName+"="+storage.getRoot());
	  this.put(strName,storage);
	}
	parseFolders(folder.getChildNodes(),driveName,newDir+'/',newName,folderdefMap,storage,newFunc);
      }
    }
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
    log.error(message+" : "+serialize(node));
    new Exception(message).printStackTrace();
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

  @Override
  public void close()
  throws IOException
  {
    for ( Storage storage : values() ) storage.close();
  }
}
