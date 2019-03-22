package mylib.backuper;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.ListIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import mylib.backuper.DataBase.File;
import mylib.backuper.DataBase.Folder;
import mylib.backuper.DataBase.Storage;

public class Backuper
{
  private final static Logger log = LoggerFactory.getLogger(Backuper.class);

  // execute options
  static public boolean debugMode = false;

  static public boolean skipScan = false;

  public static class UsageException extends RuntimeException
  {
    public UsageException() { super(); }
    public UsageException( String message ) { super(message); }
    public UsageException( String message, Throwable cause ) { super(message,cause); }
    public UsageException( Throwable cause ) { super(cause); }
  }

  public static void main( String argv[] )
  {
    LinkedList<String> args = new LinkedList<>();
    args.addAll(Arrays.asList(argv));

    String arg = getArg(args);

    Path dbFolder = Paths.get(arg);

    try ( DataBase db = new DataBase(dbFolder) ) {
      arg = getArg(args);
      Storage srcStorage = db.get(arg);
      if ( srcStorage == null ) throw new UsageException("Illegal Storage Name : "+arg);

      arg = getArg(args,false);
      if ( arg == null ) {
	refresh(srcStorage);
      } else {
	Storage dstStorage = db.get(arg);
	if ( dstStorage == null ) throw new UsageException("Illegal Storage Name : "+arg);
	if ( debugMode ) {
	  simulate(srcStorage,dstStorage);
	} else {
	  backup(srcStorage,dstStorage);
	}
      }
    } catch ( UsageException ex ) {
      log.error(ex.getMessage(),ex);
      System.err.println(ex.getMessage());
      System.err.println("usage : java -jar file.jar [-sd] dic.folder src.name [dst.name]");
      System.err.println("    dic.folder : database folder");
      System.err.println("    src.name   : source id");
      System.err.println("    dst.name   : destination id");
      System.err.println("  Option");
      System.err.println("    -s         : skip scan DB");
      System.err.println("    -d         : debug mode (no scan, no copy, no delete)");
    } catch ( Exception ex ) {
      log.error(ex.getMessage(),ex);
    }
  }

  public static String getArg( LinkedList<String> args )
  {
    return getArg(args,true);
  }

  public static String getArg( LinkedList<String> args, boolean throwflag )
  {
    String arg;
    while ( (arg = args.poll()) != null ) {
      if ( !arg.startsWith("-") ) return arg;
      for ( int i = 1; i < arg.length(); ++i ) {
	switch ( arg.charAt(i) ) {
	case 'd':
	  debugMode = true;
	  break;
	case 's':
	  skipScan = true;
	  break;
	default:
	  throw new UsageException("Illegal Option : "+arg);
	}
      }
    }
    if ( throwflag ) throw new UsageException("Not enough arguments");
    return null;
  }

  public static void backup( Storage srcStorage, Storage dstStorage )
  throws IOException
  {
    log.info("Start Backup "+srcStorage.storageName+" "+dstStorage.storageName);
    srcStorage.readDB();
    if ( skipScan ) {
      srcStorage.complementFolders();
    } else {
      srcStorage.scanFolder();
      srcStorage.writeDB();
    }

    dstStorage.readDB();
    if ( skipScan ) {
      dstStorage.complementFolders();
    } else {
      dstStorage.scanFolder();
      dstStorage.writeDB();
    }

    log.debug("Compare Files "+srcStorage.storageName+" "+dstStorage.storageName);
    long unit = Math.max(srcStorage.timeUnit(),dstStorage.timeUnit());
    LinkedList<File> frlist = toFileList(srcStorage); // ソートされたListに変換
    LinkedList<File> tolist = toFileList(dstStorage); // ソートされたListに変換
    LinkedList<File> difftimelist = new LinkedList<>();
    LinkedList<File> copylist = compare(frlist,tolist,difftimelist,unit); // frlist, tolist にはそれ以外が残る。

    // tolist means "delete"
    log.trace("start delete from "+dstStorage.getRoot());
    for ( File file : tolist ) {
      //file.dump(System.err);
      dstStorage.deleteFile(file.filePath);
    }

    // delete empty folder
    log.trace("clean folder "+dstStorage.getRoot());
    dstStorage.cleanupFolder();

    // check : file in src --> file in dst
    ListIterator<File> itr = frlist.listIterator();
    while ( itr.hasNext() ) {
      File file = itr.next();
      boolean first = true;
      boolean find = false;
      next:
      for ( Path path = file.filePath; path != null; path = path.getParent() ) {
	for ( Folder dfld : dstStorage.folders ) {
	  if ( first && path.equals(dfld.folderPath) ) { find = true; break next; }
	  for ( Path ign : dfld.ignores ) {
	    if ( path.equals(ign) ) { find = true; break next; }
	  }
	}
	first = false;
      }
      if ( find ) {
	log.error("CANNOT COPY "+file.filePath);
	itr.remove();
      }
    }

    // frlist means "copy"
    log.trace("start copy from "+srcStorage.getRoot()+" to "+dstStorage.getRoot());
    for ( File file : frlist ) {
      //file.dump(System.err);
      srcStorage.copyFile(file.filePath,dstStorage,false);
    }

    // copylist means "copy override"
    log.trace("start copy override from "+srcStorage.getRoot()+" to "+dstStorage.getRoot());
    for ( File file : copylist ) {
      //file.dump(System.err);
      srcStorage.copyFile(file.filePath,dstStorage,true);
    }

    // set lastModifed
    for ( File file : difftimelist ) {
      log.info("set lastModified "+file.filePath);
      Folder dstFolder = DataBase.findFromList(dstStorage.folders,file.filePath.getParent());
      File dstFile = DataBase.findFromList(dstFolder.files,file.filePath);
      dstFile.lastModified = file.lastModified/unit*unit;
      dstStorage.setLastModified(file.filePath,file.lastModified);
    }

    // write DB
    dstStorage.writeDB();

    log.info("End Backup "+srcStorage.storageName+" "+dstStorage.storageName);
  }

  public static void refresh( Storage storage )
  throws IOException
  {
    log.info("Start Refresh "+storage.storageName);
    storage.readDB();
    storage.scanFolder();
    storage.writeDB();
    log.info("End Refresh "+storage.storageName);
  }

  public static void simulate( Storage srcStorage, Storage dstStorage )
  throws IOException
  {
    log.info("Start Simulation "+srcStorage.storageName+" "+dstStorage.storageName);
    srcStorage.readDB();
    dstStorage.readDB();

    log.debug("Compare Files "+srcStorage.storageName+" "+dstStorage.storageName);
    long unit = Math.max(srcStorage.timeUnit(),dstStorage.timeUnit());
    LinkedList<File> frlist = toFileList(srcStorage); // ソートされたListに変換
    LinkedList<File> tolist = toFileList(dstStorage); // ソートされたListに変換
    LinkedList<File> difftimelist = new LinkedList<>();
    LinkedList<File> copylist = compare(frlist,tolist,difftimelist,unit); // frlist, tolist にはそれ以外が残る。

    // tolist means "delete"
    log.trace("simulate delete from "+dstStorage.getRoot());
    for ( File file : tolist ) {
      log.info("delete "+file.filePath);
    }

    // frlist means "copy"
    log.trace("simulate copy from "+srcStorage.getRoot()+" to "+dstStorage.getRoot());
    for ( File file : frlist ) {
      log.info("copy "+file.filePath);
    }

    // copylist means "copy override"
    log.trace("simulate copy override from "+srcStorage.getRoot()+" to "+dstStorage.getRoot());
    for ( File file : copylist ) {
      log.info("copy override "+file.filePath);
    }

    // set lastModifed
    for ( File file : difftimelist ) {
      log.info("set lastModified "+file.filePath);
    }

    log.info("End Simulation "+srcStorage.storageName+" "+dstStorage.storageName);
  }

  // ======================================================================
  public static LinkedList<File> toFileList( Storage storage )
  {
    LinkedList<File> list = new LinkedList<>();
    for ( Folder folder : storage.folders ) {
      for ( File file : folder.files ) {
	list.add(file);
      }
    }

    Collections.sort(list,Comparator.comparing(file -> file.filePath));

    File prev = null;
    for ( File file : list ) {
      if ( prev != null && prev.filePath.compareTo(file.filePath) >= 0 ) {
	throw new RuntimeException("Order Error "+prev.filePath+" "+file.filePath);
      }
      prev = file;
    }
    return list;
  }

  public static LinkedList<File> compare( LinkedList<File> frlist, LinkedList<File> tolist, LinkedList<File> difftimelist, long unit )
  {
    LinkedList<File> copylist = new LinkedList<>();
    ListIterator<File> fritr = frlist.listIterator();
    ListIterator<File> toitr = tolist.listIterator();
    while ( fritr.hasNext() && toitr.hasNext() ) {
      File frfile = fritr.next();
      File tofile = toitr.next();
      int cmp = frfile.filePath.compareTo(tofile.filePath);
      log.trace("compare "+frfile.filePath+((cmp < 0) ? " < " : (cmp > 0) ? " > " : " = ")+tofile.filePath);
      if ( cmp == 0 ) {
	if ( !frfile.hashValue.equals(tofile.hashValue) ) {
	  copylist.add(frfile);
	} else if ( frfile.lastModified/unit != tofile.lastModified/unit ) {
	  difftimelist.add(frfile);
	}
	fritr.remove();
	toitr.remove();
      } else if ( cmp < 0 ) {
	toitr.previous();
      } else {
	fritr.previous();
      }
    }
    return copylist;
  }
}
