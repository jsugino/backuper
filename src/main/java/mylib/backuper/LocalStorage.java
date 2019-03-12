package mylib.backuper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
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

import mylib.backuper.DataBase.Storage;
import mylib.backuper.DataBase.Folder;
import mylib.backuper.DataBase.File;
import static mylib.backuper.DataBase.registerToList;
import static mylib.backuper.DataBase.findFromList;

public class LocalStorage extends Storage
{
  public Path rootFolder;				// 絶対パスでのルートフォルダー

  public String getRoot()
  {
    return rootFolder.toString();
  }

  public LocalStorage( DataBase db, String storageName, Path path )
  {
    db.super(storageName);
    this.rootFolder = path;
  }

  public InputStream newInputStream( Path path )
  throws IOException
  {
    return Files.newInputStream(rootFolder.resolve(path));
  }

  public boolean mkParentDir( Path path )
  throws IOException
  {
    Path parent = rootFolder.resolve(path).getParent();
    if ( !Files.isDirectory(parent) ) {
      Files.createDirectories(parent);
      return true;
    }
    return false;
  }

  public OutputStream newOutputStream( Path path )
  throws IOException
  {
    return Files.newOutputStream(rootFolder.resolve(path));
  }

  public void setLastModifiedTime( Path path, long time )
  throws IOException
  {
    path = rootFolder.resolve(path);
    Files.setLastModifiedTime(path,FileTime.fromMillis(time));
  }

  public boolean deleteRealFile( Path path )
  throws IOException
  {
    Path full = rootFolder.resolve(path);
    if (
      Files.isDirectory(full) &&
      Files.list(full).count() != 0
    ) return false;
    Files.delete(full);
    return true;
  }

  public void scanFolder()
  throws IOException
  {
    //log.info("Scan Folder "+storageName/*+" "+rootFolder*/);

    LinkedList<Folder> origFolders = folders;
    folders = new LinkedList<Folder>();
    LinkedList<Path> folderList = new LinkedList<>();
    LinkedList<Path> pathList = new LinkedList<>();
    folderList.add(rootFolder);
    while ( folderList.size() > 0 ) {
      Path folderpath = folderList.remove();
      Path rel = rootFolder.relativize(folderpath);
      if ( rel.toString().length() == 0 ) rel = Paths.get(".");
      //log.debug("new Folder("+rel+")");
      Folder folder = new Folder(rel);
      registerToList(folders,folder);
      pathList.clear();
      try ( Stream<Path> stream = Files.list(folderpath) ) {
	stream.forEach(pathList::add);
      }
      nextPath:
      for ( Path path : pathList ) {
	Path relpath = rootFolder.relativize(path);
	if ( Files.isSymbolicLink(path) ) {
	  //log.info("ignore symbolic link : "+path);
	  continue nextPath;
	} else if ( Files.isDirectory(path) ) {
	  //log.debug("scan folder "+relpath);
	  for ( Pattern pat : ignoreFolderPats ) {
	    if ( pat.matcher(relpath.toString()).matches() ) {
	      //log.info("ignore folder "+relpath);
	      continue nextPath;
	    }
	  }
	  folderList.add(path);
	} else {
	  //log.debug("scan file "+relpath);
	  for ( Pattern pat : ignoreFilePats ) {
	    if ( pat.matcher(relpath.getFileName().toString()).matches() ) {
	      //log.info("ignore file "+relpath);
	      continue nextPath;
	    }
	  }
	  //log.debug("new File("+relpath+")");
	  File file = new File(relpath);
	  registerToList(folder.files,file);
	  file.length = Files.size(path);
	  file.lastModified = Files.getLastModifiedTime(path).toMillis();
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

  @Override
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
}
