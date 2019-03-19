package mylib.backuper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mylib.backuper.DataBase.File;
import mylib.backuper.DataBase.Folder;
import mylib.backuper.DataBase.PathHolder;
import mylib.backuper.DataBase.Storage;

public class FtpStorage extends Storage
{
  private final static Logger log = LoggerFactory.getLogger(FtpStorage.class);

  // FTPClient variables
  public FTPClient ftpclient;

  public String hostname;
  public String rootFolder;

  public String userid;
  public String password;

  public HashSet<Path> createdFolders = new HashSet<>();

  public boolean connect()
  throws IOException
  {
    if ( ftpclient != null ) return true;
    boolean result;
    try {
      ftpclient = new FTPClient();
      ftpclient.connect(hostname);
      result = ftpclient.login(userid,password);
      if ( !result || !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) ) {
	log.error("login Failed : result="+result+", code="+ftpclient.getReplyCode());
	close();
	return false;
      }
      result = ftpclient.changeWorkingDirectory(rootFolder);
      if ( !result || !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) ) {
	log.error("changeWorkingDirectory Failed : result="+result+", code="+ftpclient.getReplyCode());
	close();
	return false;
      }
    } catch ( IOException ex ) {
      log.error("IOException : "+ex.getMessage(),ex);
      close();
      throw ex;
    }
    return true;
  }

  @Override
  public void close()
  throws IOException
  {
    if ( ftpclient != null ) ftpclient.disconnect();
    ftpclient = null;
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

  public FtpStorage( DataBase db, String storageName, String url )
  {
    db.super(storageName);
    String args[] = FtpStorage.parseURL(url);
    this.userid = args[0];
    this.password = args[1];
    this.hostname = args[2];
    this.rootFolder = args[3];
  }

  public FtpStorage( DataBase db, String storageName, String userid, String password, String hostname, String rootFolder )
  {
    db.super(storageName);
    this.hostname = hostname;
    this.userid = userid;
    this.password = password;
    this.rootFolder = rootFolder;
  }

  // ----------------------------------------------------------------------

  @Override
  public String getRoot()
  {
    return "ftp://"+hostname+rootFolder;
  }

  @Override
  public List<PathHolder> getPathHolderList(Path path)
  throws IOException
  {
    if ( !connect() ) return null;
    return Stream.of(ftpclient.mlistDir(path.toString()))
      .filter(f->!f.getName().equals("..") && !f.getName().equals("."))
      .map(f->(
	  f.isDirectory()
	  ? new Folder(path.resolve(f.getName()).normalize())
	  : new File(path.resolve(f.getName()).normalize(),f.getTimestamp().getTimeInMillis(),f.getSize())))
      .collect(Collectors.toList());
  }

  @Override
  public boolean mkParentDir( Path path )
  throws IOException
  {
    if ( !connect() ) return false;
    Path parent = path.getParent();
    //ftpclient.makeDirectory(
    return false;
  }

  @Override
  public InputStream newInputStream( Path path )
  throws IOException
  {
    if ( !connect() ) return null;
    return new ByteArrayInputStream("abc".getBytes());
  }

  @Override
  public OutputStream newOutputStream( Path path )
  throws IOException
  {
    if ( !connect() ) return null;
    return null;
  }

  @Override
  public void setLastModified( Path path, long time )
  throws IOException
  {
    if ( !connect() ) return;
  }

  @Override
  public void deleteRealFile( Path path )
  throws IOException
  {
    if ( !connect() ) return;
  }

  @Override
  public void deleteRealFolder( Path path )
  throws IOException
  {
    if ( !connect() ) return;
  }
}
