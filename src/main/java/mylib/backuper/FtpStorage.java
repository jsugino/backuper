package mylib.backuper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import mylib.backuper.DataBase.PathHolder;
import mylib.backuper.DataBase.Storage;

public class FtpStorage extends Storage
{
  public static void connect()
  throws IOException
  {
    FTPClient client = new FTPClient();
    client.connect("comb.sakura.ne.jp");
    client.login("comb","rg@k123");
    if ( !FTPReply.isPositiveCompletion(client.getReplyCode()) ) {
      System.out.println("Login Failed");
      client.disconnect();
      return;
    }
    FTPFile files[] = client.listFiles();
    if ( !FTPReply.isPositiveCompletion(client.getReplyCode()) ) {
      System.out.println("List Failed");
      client.disconnect();
      return;
    }
    for ( FTPFile file : files ) {
      long time = file.getTimestamp().getTimeInMillis();
      System.out
	.format("%tF %tT",time,time)
	.format(" %10d",file.getSize())
	.append(file.isDirectory()?" <dir>":file.isFile()?"      ":" ?????")
	.format(" %s",file.getName())
	.println();
    }
  }

  

  public String hostname;
  public Path rootFolder;

  public String userid;
  public String password;

  public FTPClient client = null;

  @Override
  public List<PathHolder> getPathHolderList(Path path)
  throws IOException
  {
    // TODO Auto-generated method stub
    return null;
  }

  public FtpStorage( DataBase db, String storageName, String userid, String password, String hostname, Path rootFolder )
  {
    db.super(storageName);
    this.hostname = hostname;
    this.userid = userid;
    this.password = password;
    this.rootFolder = rootFolder;
  }

  public String getRoot()
  {
    return "ftp://"+hostname+rootFolder;
  }

  public boolean mkParentDir( Path path )
  throws IOException
  {
    return false;
  }

  public InputStream newInputStream( Path path )
  throws IOException
  {
    return null;
  }

  public OutputStream newOutputStream( Path path )
  throws IOException
  {
    return null;
  }

  public void setLastModified( Path path, long time )
  throws IOException
  {
  }

  public long getLastModified( Path path )
  throws IOException
  {
    return 0L;
  }

  public long getSize( Path relpath )
  throws IOException
  {
    return 0L;
  }

  public List<Path> pathList( Path rel )
  throws IOException
  {
    return null;
  }

  public boolean deleteRealFile( Path path )
  throws IOException
  {
    return false;
  }

  public boolean isSymbolicLink( Path relpath )
  {
    return false;
  }

  public boolean isDirectory( Path relpath )
  {
    return false;
  }
}
