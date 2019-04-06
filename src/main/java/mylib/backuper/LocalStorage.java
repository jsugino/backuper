package mylib.backuper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

  @Override
  public long timeUnit() {
    return 1L;
  }

  @Override
  public String getRoot()
  {
    return rootFolder.toString();
  }

  @Override
  public void makeRealDirectory( Path path )
  throws IOException
  {
    Files.createDirectory(rootFolder.resolve(path));
  }

  @Override
  public InputStream newInputStream( Path path )
  throws IOException
  {
    return Files.newInputStream(rootFolder.resolve(path));
  }

  @Override
  public OutputStream newOutputStream( Path path )
  throws IOException
  {
    return Files.newOutputStream(rootFolder.resolve(path),StandardOpenOption.CREATE_NEW);
  }

  @Override
  public void setRealLastModified( Path path, long time )
  throws IOException
  {
    Files.setLastModifiedTime(rootFolder.resolve(path),FileTime.fromMillis(time));
  }

  @Override
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

  @Override
  public void deleteRealFolder( Path path )
  throws IOException
  {
    Path full = rootFolder.resolve(path);
    if ( !Files.isDirectory(full) ) throw new IOException("Is not a directory : "+full);
    Files.delete(full);
  }

  @Override
  public void deleteRealFile( Path path )
  throws IOException
  {
    Path full = rootFolder.resolve(path);
    if ( Files.isDirectory(full) ) throw new IOException("Is not a file : "+full);
    Files.delete(full);
  }

  @Override
  public void moveRealFile( Path fromPath, Path toPath )
  throws IOException
  {
    Files.move(rootFolder.resolve(fromPath).normalize(),rootFolder.resolve(toPath).normalize());
  }
}
