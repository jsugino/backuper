package mylib.backuper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mylib.backuper.DataBase.PathHolder;
import mylib.backuper.DataBase.Storage;

public class FtpStorage extends Storage
{
  private final static Logger log = LoggerFactory.getLogger(FtpStorage.class);

  public static void connect()
  throws IOException
  {
    FTPClient client = new FTPClient();
    client.connect("comb.sakura.ne.jp");
    client.login("comb","rg@k123");
    if ( !FTPReply.isPositiveCompletion(client.getReplyCode()) ) {
      log.error("Login Failed : "+client.getReplyCode());
      client.disconnect();
      return;
    }
    FTPFile files[] = client.listFiles();
    if ( !FTPReply.isPositiveCompletion(client.getReplyCode()) ) {
      log.error("List Failed : "+client.getReplyCode());
      client.disconnect();
      return;
    }
    for ( FTPFile file : files ) {
      long time = file.getTimestamp().getTimeInMillis();
      log.info(String.format(
	  "%tF %tT %10d %5s %s",
	  time,time,
	  file.getSize(),
	  file.isDirectory()?"<dir>":file.isFile()?"      ":" ?????",
	  file.getName()));
    }
  }

  //                 user    pass    host    folder
  // parsing "ftp://<..0..>:<..1..>@<..2..>/<..3..>"
  //     idx        0      1       2       3
  public static String[] parseURL( String url )
  {
    if ( !url.startsWith("ftp://") ) throw new IllegalArgumentException("not start with ftp : "+url);
    int idx0 = "ftp://".length();
    int idx1 = url.indexOf(':',idx0);
    int idx3 = url.indexOf('/',idx1);
    int idx2 = url.lastIndexOf('@',idx3);
    return new String[]{
      url.substring(idx0,idx1),
      url.substring(idx1+1,idx2),
      url.substring(idx2+1,idx3),
      url.substring(idx3+1),
    };
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
