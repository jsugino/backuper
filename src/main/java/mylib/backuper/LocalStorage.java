package mylib.backuper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import mylib.backuper.DataBase.File;
import mylib.backuper.DataBase.Folder;
import mylib.backuper.DataBase.PathHolder;
import mylib.backuper.DataBase.Storage;

public class LocalStorage extends Storage
{
  public Path rootFolder;				// 絶対パスでのルートフォルダー

  public LocalStorage( DataBase db, String storageName, Path rootFolder )
  {
    db.super(storageName);
    this.rootFolder = rootFolder;
  }

  @Override
  public void close()
  throws IOException
  {
    // nothing to do
  }

  public String getRoot()
  {
    return rootFolder.toString();
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

  public InputStream newInputStream( Path path )
  throws IOException
  {
    return Files.newInputStream(rootFolder.resolve(path));
  }

  public OutputStream newOutputStream( Path path )
  throws IOException
  {
    return Files.newOutputStream(rootFolder.resolve(path));
  }

  public void setLastModified( Path path, long time )
  throws IOException
  {
    Files.setLastModifiedTime(rootFolder.resolve(path),FileTime.fromMillis(time));
  }

  public long getLastModified( Path path )
  throws IOException
  {
    return Files.getLastModifiedTime(rootFolder.resolve(path)).toMillis();
  }

  public long getSize( Path relpath )
  throws IOException
  {
    return Files.size(rootFolder.resolve(relpath));
  }

  public List<Path> pathList( Path rel )
  throws IOException
  {
    try ( Stream<Path> stream =
      Files.list(rel.equals(Paths.get(".")) ? rootFolder : rootFolder.resolve(rel))
    ) {
      return stream.map(rootFolder::relativize).collect(Collectors.toList());
    }
  }

  public List<PathHolder> getPathHolderList( Path rel )
  throws IOException
  {
    LinkedList<PathHolder> list = new LinkedList<>();
    try ( Stream<Path> stream =
      Files.list(rel.equals(Paths.get(".")) ? rootFolder : rootFolder.resolve(rel))
    ) {
      for ( Path path : stream.collect(Collectors.toList()) ) {
	if ( Files.isSymbolicLink(path) ) {
	  File file = new File(rootFolder.relativize(path));
	  file.type = File.FileType.SYMLINK;
	  list.add(file);
	} else if ( Files.isDirectory(path) ) {
	  list.add(new Folder(rootFolder.relativize(path)));
	} else {
	  File file = new File(rootFolder.relativize(path));
	  file.type = File.FileType.NORMAL;
	  file.lastModified = Files.getLastModifiedTime(path).toMillis();
	  file.length = Files.size(path);
	  list.add(file);
	}
      }
    }
    return list;
  }

  public void deleteRealFolder( Path path )
  throws IOException
  {
    Path full = rootFolder.resolve(path);
    if ( !Files.isDirectory(full) ) throw new IOException("Is not a directory : "+full);
    Files.delete(full);
  }

  public void deleteRealFile( Path path )
  throws IOException
  {
    Path full = rootFolder.resolve(path);
    if ( Files.isDirectory(full) ) throw new IOException("Is not a file : "+full);
    Files.delete(full);
  }

  public boolean isSymbolicLink( Path relpath )
  {
    return Files.isSymbolicLink(rootFolder.resolve(relpath));
  }

  public boolean isDirectory( Path relpath )
  {
    return Files.isDirectory(rootFolder.resolve(relpath));
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
