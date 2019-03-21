package mylib.backuper;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
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

  public static void main( String argv[] )
  {
    LinkedList<String> args = new LinkedList<>();
    args.addAll(Arrays.asList(argv));

    String arg;
    if ( (arg = args.poll()) == null ) { usage(); return; }

    boolean debugMode = false;
    if ( arg.startsWith("-") ) {
      if ( arg.equals("-d") ) {
	debugMode = true;
      } else { usage(); return; }
      if ( (arg = args.poll()) == null ) { usage(); return; }
    }

    Path dbFolder = Paths.get(arg);

    try ( DataBase db = new DataBase(dbFolder) ) {
      if ( (arg = args.poll()) == null ) { usage(); return; }
      Storage srcStorage = db.get(arg);
      if ( srcStorage == null ) {
	log.error("Illegal Storage Name : "+arg);
	usage();
	return;
      }

      Storage dstStorage = null;
      if ( (arg = args.poll()) != null ) {
	dstStorage = db.get(arg);
	if ( dstStorage == null ) {
	  log.error("Illegal Storage Name : "+args.getFirst());
	  return;
	}
      }

      backup(srcStorage,dstStorage);

    } catch ( Exception ex ) {
      log.error(ex.getMessage(),ex);
    }
  }

  public static void backup( Storage srcStorage, Storage dstStorage )
  throws IOException
  {
    if ( dstStorage == null ) {
      refresh(srcStorage);
      return;
    }

    log.info("Start Backup "+srcStorage.storageName+" "+dstStorage.storageName);
    srcStorage.readDB();
    srcStorage.scanFolder();
    srcStorage.writeDB();

    dstStorage.readDB();
    dstStorage.scanFolder();
    dstStorage.writeDB();

    log.debug("Compare Files "+srcStorage.storageName+" "+dstStorage.storageName);
    LinkedList<File> frlist = toFileList(srcStorage);
    LinkedList<File> tolist = toFileList(dstStorage);
    LinkedList<File> copylist = compare(frlist,tolist);

    // frlist means "copy"
    log.trace("start copy from "+srcStorage.getRoot()+" to "+dstStorage.getRoot());
    for ( File file : frlist ) {
      //file.dump(System.err);
      srcStorage.copyFile(file.filePath,dstStorage);
    }

    // copylist means "copy override"
    log.trace("start copy override from "+srcStorage.getRoot()+" to "+dstStorage.getRoot());
    for ( File file : copylist ) {
      //file.dump(System.err);
      srcStorage.copyFile(file.filePath,dstStorage);
    }

    // tolist means "delete"
    log.trace("start delete from "+dstStorage.getRoot());
    for ( File file : tolist ) {
      //file.dump(System.err);
      dstStorage.deleteFile(file.filePath);
    }

    // set lastModifed
    for ( Folder folder : dstStorage.folders ) {
      for ( File file : folder.files ) {
	dstStorage.setLastModified(file.filePath,srcStorage);
      }
    }

    log.trace("clean folder "+dstStorage.getRoot());
    dstStorage.cleanupFolder();
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

  public static void usage()
  {
    System.err.println("usage : java -jar file.jar dic.folder src.name [dst.name]");
    System.err.println("    dic.folder : database folder");
    System.err.println("    src.name   : source id");
    System.err.println("    dst.name   : destination id");
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

    File array[] = new File[list.size()];
    array = list.toArray(array);
    Arrays.sort(array,Comparator.comparing(file -> file.filePath));
    ListIterator<File> itr = list.listIterator();
    for ( int i = 0; itr.hasNext(); ++i ) { itr.next(); itr.set(array[i]); }

    File prev = null;
    for ( File file : list ) {
      if ( prev != null && prev.filePath.compareTo(file.filePath) >= 0 ) {
	throw new RuntimeException("Order Error "+prev.filePath+" "+file.filePath);
      }
      prev = file;
    }
    return list;
  }

  public static LinkedList<File> compare( LinkedList<File> frlist, LinkedList<File> tolist )
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
