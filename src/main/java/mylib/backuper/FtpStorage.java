package mylib.backuper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
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

  public boolean connect()
  throws IOException
  {
    if ( ftpclient != null ) return true;
    boolean result;
    try {
      ftpclient = new FTPClient();
      log.trace("ftp connect : "+hostname);
      ftpclient.connect(hostname);

      log.trace("ftp login : "+userid);
      result = ftpclient.login(userid,password);
      if ( !result || !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) ) {
	log.error("login Failed : result="+result
	  +", code="+ftpclient.getReplyCode()
	  +", message="+ftpclient.getReplyString()
	);
	close();
	return false;
      }

      if ( rootFolder.length() > 0 && !rootFolder.equals(".") ) {
	log.trace("ftp chdir : "+rootFolder);
	result = ftpclient.changeWorkingDirectory(rootFolder);
	if ( !result || !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) ) {
	  log.error("changeWorkingDirectory Failed : result="+result
	    +", code="+ftpclient.getReplyCode()
	    +", message="+ftpclient.getReplyString()
	  );
	  close();
	  return false;
	}
      }

      log.trace("ftp passive mode");
      ftpclient.enterLocalPassiveMode();
      if ( !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) ) {
	log.error("enterLocalPassiveMode failed "
	  +": code = "+ftpclient.getReplyCode()
	  +", message="+ftpclient.getReplyString()
	);
	close();
	return false;
      }

      log.trace("ftp binary mode");
      result = ftpclient.setFileType(FTP.BINARY_FILE_TYPE);
      if ( !result || !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) ) {
	log.error("setFileType(BINARY) Failed : result="+result
	  +", code="+ftpclient.getReplyCode()
	  +", message="+ftpclient.getReplyString()
	);
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
    if ( ftpclient != null ) {
      log.trace("ftp disconnect");
      ftpclient.disconnect();
    }
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
    try {
      return new String[]{
	url.substring(idx0,idx1),
	url.substring(idx1+1,idx2),
	url.substring(idx2+1,idx3),
	url.substring(idx3+1),
      };
    } catch ( StringIndexOutOfBoundsException ex ) {
      log.error("StringIndexOutOfBoundsException : "+url,ex);
      throw ex;
    }
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
  public long timeUnit() {
    return 1000L;
  }

  @Override
  public String getRoot()
  {
    return "ftp://"+hostname+'/'+rootFolder;
  }

  @Override
  public List<PathHolder> getPathHolderList(Path path)
  throws IOException
  {
    if ( !connect() ) return null;
    FTPFile list[] = ftpclient.mlistDir(path.toString());
    if ( list == null || !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) )
      throw new IOException("mlistDir failed : "+path
	+", list = "+Arrays.asList(list)
	+", code = "+ftpclient.getReplyCode()
	+", message = "+ftpclient.getReplyString());
    return Stream.of(list)
      .filter(f->!f.getName().equals("..") && !f.getName().equals("."))
      .map(f->(
	  f.isDirectory()
	  ? new Folder(path.resolve(f.getName()).normalize())
	  : new File(path.resolve(f.getName()).normalize(),f.getTimestamp().getTimeInMillis(),f.getSize())))
      .collect(Collectors.toList());
  }

  @Override
  public void makeRealDirectory( Path path )
  throws IOException
  {
    if ( !connect() ) return;
    boolean result = ftpclient.makeDirectory(path.toString());
    if ( !result || !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) )
      throw new IOException("makeDirectory failed : "+path+", result = "+result
	+", code = "+ftpclient.getReplyCode()
	+", message = "+ftpclient.getReplyString());
  }

  @Override
  public InputStream newInputStream( Path path )
  throws IOException
  {
    if ( !connect() ) return null;
    log.trace("newInputStream : "+path);
    InputStream source = ftpclient.retrieveFileStream(path.toString());
    if ( source == null )
      throw new IOException("cannot open '"+path+"' for read"
	+", code = "+ftpclient.getReplyCode()
	+", message = "+ftpclient.getReplyString());
    return new InputStream() {
      @Override public int read() throws IOException { return source.read(); }
      @Override public int read(byte[] b) throws IOException { return source.read(b); }
      @Override public int read(byte[] b, int off, int len) throws IOException { return source.read(b, off, len); }
      @Override public long skip(long n) throws IOException { return source.skip(n); }
      @Override public int available() throws IOException { return source.available(); }
      @Override
      public void close()
      throws IOException
      {
	source.close();
	log.trace("CompleteInputStream#completePendingCommand");
	if ( !ftpclient.completePendingCommand() )
	  throw new IOException("completePendingCommand failed");
      }
      @Override public void mark(int readlimit) { source.mark(readlimit); }
      @Override public synchronized void reset() throws IOException { source.reset(); }
      @Override public boolean markSupported() { return source.markSupported(); }
    };
  }

  @Override
  public OutputStream newOutputStream( Path path )
  throws IOException
  {
    if ( !connect() ) return null;
    log.trace("newOutputStream : "+path);
    OutputStream source = ftpclient.storeFileStream(path.toString());
    if ( source == null )
      throw new IOException("cannot open '"+path+"' for write"
	+", code = "+ftpclient.getReplyCode()
	+", message = "+ftpclient.getReplyString());
    return new OutputStream() {
      @Override public void write(int arg0) throws IOException { source.write(arg0); }
      @Override public void write(byte[] b) throws IOException { source.write(b); }
      @Override public void write(byte[] b, int off, int len) throws IOException { source.write(b, off, len); }
      @Override public void flush() throws IOException { source.flush(); }
      @Override
      public void close()
      throws IOException
      {
	source.close();
	log.trace("CompleteOutputStream#completePendingCommand");
	if ( !ftpclient.completePendingCommand() )
	  throw new IOException("completePendingCommand failed");
      }
    };
  }

  public static SimpleDateFormat FTPTIMEFORM;
  static {
    (FTPTIMEFORM = new SimpleDateFormat("yyyyMMddHHmmss"))
      .setTimeZone(TimeZone.getTimeZone("GMT"));
  }

  @Override
  public void setRealLastModified( Path path, long time )
  throws IOException
  {
    if ( !connect() ) return;
    String strtime = FTPTIMEFORM.format(new Date(time));
    log.trace("setModificationTime : "+path+" to "+strtime);
    boolean result = ftpclient.setModificationTime(path.toString(),strtime);
    if ( !result || !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) )
      throw new IOException("setModificationTime failed : "+path+" with "+strtime
	+", result = "+result
	+", code = "+ftpclient.getReplyCode()
	+", message = "+ftpclient.getReplyString());
  }

  @Override
  public void deleteRealFile( Path path )
  throws IOException
  {
    if ( !connect() ) return;
    boolean result = ftpclient.deleteFile(path.toString());
    if ( !result || !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) )
      throw new IOException("deleteFile failed : "+path
	+", result = "+result
	+", code = "+ftpclient.getReplyCode()
	+", message = "+ftpclient.getReplyString());
  }

  @Override
  public void deleteRealFolder( Path path )
  throws IOException
  {
    if ( !connect() ) return;
    boolean result = ftpclient.removeDirectory(path.toString());
    if ( !result || !FTPReply.isPositiveCompletion(ftpclient.getReplyCode()) )
      throw new IOException("removeDirectory failed : "+path
	+", result = "+result
	+", code = "+ftpclient.getReplyCode()
	+", message = "+ftpclient.getReplyString());
  }

  @Override
  public void moveRealFile( Path fromPath, Path toPath )
  throws IOException
  {
    throw new IOException("not implemented : fromPath = "+fromPath+", toPath = "+toPath);
  }

  @Override
  public String toString()
  {
    return storageName+'='+getRoot();
  }
}
