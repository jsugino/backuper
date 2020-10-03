package mylib.backuper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mylib.backuper.DataBase.File;
import mylib.backuper.DataBase.Folder;
import mylib.backuper.DataBase.Storage;
import mylib.backuper.Backup.Task;

import static mylib.backuper.DataBase.findFromList;
import static mylib.backuper.DataBase.registerToList;

public class Main
{
  public final static Logger log = LoggerFactory.getLogger(Main.class);

  // execute commands
  static enum Command {
    BACKUP_BY_TASK,
    BACKUP_OR_SCANONLY,
    BACKUP_SKIPSCAN,
    SIMULATE,
    LISTDB
  }

  public static Command exCommand = Command.BACKUP_OR_SCANONLY;

  @SuppressWarnings("serial")
  public static class UsageException extends RuntimeException
  {
    public UsageException() { super(); }
    public UsageException( String message ) { super(message); }
    public UsageException( String message, Throwable cause ) { super(message,cause); }
    public UsageException( Throwable cause ) { super(cause); }
  }

  public final static String CONFIGNAME = "folders.conf";
  public final static String CONFIGXML  = "folders.conf.xml";

  public static void main( String argv[] )
  {
    LinkedList<String> args = new LinkedList<>();
    args.addAll(Arrays.asList(argv));
    try {
      main(args);
    } catch ( UsageException ex ) {
      log.error(ex.getMessage(),ex);
      System.err.println("usage : java -jar file.jar [-sdl] dic.folder src.name [dst.name]");
      System.err.println("    dic.folder : database folder");
      System.err.println("    src.name   : source id");
      System.err.println("    dst.name   : destination id");
      System.err.println("  Option");
      System.err.println("    -s         : skip scan DB");
      System.err.println("    -d         : debug mode (no scan, no copy, no delete)");
      System.err.println("    -l         : print all definition");
    } catch ( Exception ex ) {
      log.error(ex.getMessage(),ex);
    }
  }

  public static void main( List<String> args )
  throws IOException
  {
    String arg = getArg(args);

    Path dbFolder = Paths.get(arg);

    try ( DataBase db = new DataBase(dbFolder) ) {
      Backup backup = null;
      if ( Files.isReadable(dbFolder.resolve(CONFIGXML)) ) {
	backup = db.initializeByXml(dbFolder.resolve(CONFIGXML));
	if ( exCommand == Command.BACKUP_OR_SCANONLY ) {
	  exCommand = Command.BACKUP_BY_TASK;
	}
      } else if ( Files.isReadable(dbFolder.resolve(CONFIGNAME)) ) {
	db.initializeByFile(dbFolder.resolve(CONFIGNAME));
      } else {
	throw new UsageException("no definition file");
      }
      switch ( exCommand ) {
      case BACKUP_BY_TASK:
	{
	  for ( Task task : getTaskList(backup,args) ) {
	    for ( Storage copy : task.copyStorages ) {
	      Storage his = task.historyStorages.get(copy.storageName);
	      backupEx(task.origStorage,copy,his);
	    }
	  }
	}
	break;

      case BACKUP_OR_SCANONLY:
	{
	  Storage srcStorage = getStorage(db,args);
	  Storage dstStorage = getStorage(db,args,false);
	  if ( dstStorage == null ) {
	    refresh(srcStorage);
	  } else {
	    backupEx(srcStorage,dstStorage);
	  }
	}
	break;

      case BACKUP_SKIPSCAN:
	{
	  Storage srcStorage = getStorage(db,args);
	  Storage dstStorage = getStorage(db,args);
	  backupEx(srcStorage,dstStorage);
	}
	break;

      case SIMULATE:
	{
	  Storage srcStorage = getStorage(db,args);
	  Storage dstStorage = getStorage(db,args);
	  simulateEx(srcStorage,dstStorage);
	}
	break;

      case LISTDB:
	{
	  for ( Storage storage : listDB(db) ) {
	    if ( !Files.isReadable(db.dbFolder.resolve(storage.storageName+".db")) ) {
	      System.out.format("%-20s (no *.db file) %s",
		storage.storageName,
		storage.getRoot()).println();
	    } else {
	      storage.readDB();
	      System.out.format("%-20s %6d %7d %s",
		storage.storageName,
		storage.folderSize(),
		storage.fileSize(),
		storage.getRoot()).println();
	    }
	  }
	  backup.printTask(System.out);
	}
	break;
      }
    }
  }

  public static String getArg( List<String> args )
  {
    return getArg(args,true);
  }

  public static String getArg( List<String> args, boolean throwflag )
  {
    while ( args.size() > 0 ) {
      String arg = args.remove(0);
      if ( !checkOpt(arg) ) {
	if ( args.size() > 0 ) checkOpt(args.get(0));
	return arg;
      }
    }
    if ( throwflag ) throw new UsageException("Not enough arguments");
    return null;
  }

  public static boolean checkOpt( String arg )
  {
    if ( !arg.startsWith("-") ) return false;
    for ( int i = 1; i < arg.length(); ++i ) {
      switch ( arg.charAt(i) ) {
      case 'd':
	exCommand = Command.SIMULATE;
	break;
      case 's':
	exCommand = Command.BACKUP_SKIPSCAN;
	break;
      case 'l':
	exCommand = Command.LISTDB;
	break;
      default:
	throw new UsageException("Illegal Option : "+arg);
      }
    }
    return true;
  }

  public static Storage getStorage( DataBase db, List<String> args )
  {
    return getStorage(db,args,true);
  }

  public static Storage getStorage( DataBase db, List<String> args, boolean throwflag )
  {
    String arg = getArg(args,throwflag);
    if ( arg == null ) return null;
    Storage storage = db.get(arg);
    if ( throwflag && storage == null ) throw new UsageException("Illegal Storage Name : "+arg);
    return storage;
  }

  public static List<Task> getTaskList( Backup backup, List<String> args )
  {
    String arg = getArg(args,false);
    if ( arg == null ) {
      StringBuffer buf = new StringBuffer();
      String str = "Need to specify Task Name : ";
      for ( String key : backup.keySet() ) {
	buf.append(str).append(key);
	str = ", ";
      }
      throw new UsageException(buf.toString());
    }
    List<Task> tasklist = backup.get(arg);
    if ( tasklist == null ) throw new UsageException("No such task : "+arg);
    return tasklist;
  }

  public static void backupEx( Storage srcStorage, Storage dstStorage )
  throws IOException
  {
    backupEx(srcStorage,dstStorage,null);
  }

  public static void backupEx( Storage srcStorage, Storage dstStorage, Storage hisStorage )
  throws IOException
  {
    log.info("Start Backup "+srcStorage.storageName+" "+dstStorage.storageName);
    srcStorage.readDB();
    if ( exCommand == Command.BACKUP_SKIPSCAN ) {
      srcStorage.complementFolders();
    } else {
      srcStorage.scanFolder();
    }

    dstStorage.readDB();
    if ( exCommand == Command.BACKUP_SKIPSCAN ) {
      dstStorage.complementFolders();
    } else {
      dstStorage.scanFolder();
    }

    if ( hisStorage != null ) {
      hisStorage.readDB();
      if ( exCommand == Command.BACKUP_SKIPSCAN ) {
	hisStorage.complementFolders();
      } else {
	hisStorage.scanFolder();
      }
    }

    backup(srcStorage,dstStorage,hisStorage,true);

    // write DB
    srcStorage.writeDB();
    dstStorage.writeDB();
    if ( hisStorage != null ) {
      hisStorage.writeDB();
    }

    log.info("End Backup "+srcStorage.storageName+" "+dstStorage.storageName);
  }

  public static void backup( Storage srcStorage, Storage dstStorage, Storage hisStorage, boolean forceCopy )
  throws IOException
  {
    log.debug("Compare Files "+srcStorage.storageName+" "+dstStorage.storageName);
    long unit = Math.max(srcStorage.timeUnit(),dstStorage.timeUnit());
    LinkedList<File> frlist = toFileList(srcStorage); // ソートされたListに変換
    LinkedList<File> tolist = toFileList(dstStorage); // ソートされたListに変換
    LinkedList<Path> difflist = compare(frlist,tolist,unit); // frlist, tolist にはそれ以外が残る。

    // tolist means "delete"
    if ( tolist.size() > 10 && !forceCopy )
      throw new UsageException("too many delete files : "+tolist.size());
    log.trace("start delete from "+dstStorage.getRoot());
    for ( File file : tolist ) {
      //file.dump(System.err);
      if ( hisStorage != null ) {
	dstStorage.moveHistoryFile(file.filePath,hisStorage);
      } else {
	dstStorage.deleteFile(file.filePath);
      }
    }

    // delete empty folder
    log.trace("clean folder "+dstStorage.getRoot());
    dstStorage.cleanupFolder();

    // check : file in src --> file in dst
    /*
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
    */

    // frlist means "copy"
    log.trace("start copy from "+srcStorage.getRoot()+" to "+dstStorage.getRoot());
    for ( File file : frlist ) {
      //file.dump(System.err);

      if ( dstStorage.getFolder(file.filePath) != null ) { log.error("CANNOT COPY "+file.filePath); continue; }
      Path parentPath = file.filePath.getParent();
      if ( parentPath == null ) parentPath = Paths.get(".");
      Folder dstFolder = dstStorage.getFolder(parentPath,dstStorage.storageName);
      if ( dstFolder == null ) { log.error("CANNOT COPY "+file.filePath); continue; }
      if ( dstFolder.ignores.contains(file.filePath) ) { log.error("CANNOT COPY "+file.filePath); continue; }
      log.info("copy "+file.filePath);
      String hashValue = srcStorage.copyRealFile(file.filePath,dstStorage);
      dstStorage.setRealLastModified(file.filePath,file.lastModified);

      File dstFile = new File(file.filePath);
      registerToList(dstFolder.files,dstFile);
      dstFile.length = file.length;
      dstFile.lastModified = file.lastModified/unit*unit;
      dstFile.hashValue = file.hashValue = hashValue;
    }

    // difflist means "set lastModifed" or "copy override" or "do nothing"
    log.trace("start check difflist from "+srcStorage.getRoot()+" to "+dstStorage.getRoot());
    for ( Path filePath : difflist ) {
      copyFile(srcStorage,dstStorage,filePath,hisStorage);
    }
  }

  public static void copyFile( Storage srcStorage, Storage dstStorage, Path filePath, Storage hisStorage )
  throws IOException
  {
    log.trace("copyFile srcStorage = "+srcStorage.storageName
      +", dstStorage = "+dstStorage.storageName
      +", filePath = "+filePath
      +", hisStorage = "+(hisStorage == null ? "(null)" : hisStorage.storageName));
    Path parentPath = filePath.getParent();
    if ( parentPath == null ) parentPath = Paths.get(".");
    Folder srcFolder = srcStorage.getFolder(parentPath);
    File   srcFile   = findFromList(srcFolder.files,filePath);
    Folder dstFolder = dstStorage.getFolder(parentPath,dstStorage.storageName);
    File   dstFile   = findFromList(dstFolder.files,filePath);
    long   unit      = Math.max(srcStorage.timeUnit(),dstStorage.timeUnit());

    if ( srcFile.length == dstFile.length ) {
      log.trace("same length");
      if ( srcFile.hashValue == null ) srcFile.hashValue = srcStorage.getMD5(filePath);
      if ( dstFile.hashValue == null ) dstFile.hashValue = dstStorage.getMD5(filePath);
      if ( srcFile.hashValue.equals(dstFile.hashValue) ) {
	log.trace("same hashValue");
	if ( srcFile.lastModified/unit != dstFile.lastModified/unit ) {
	  log.trace("diff lastModified");
	  log.info("set lastModified "+dstFile.filePath);
	  dstFile.lastModified = srcFile.lastModified/unit*unit;
	  dstStorage.setRealLastModified(dstFile.filePath,dstFile.lastModified);
	}
	return;
      }
    }
    log.trace("copy file");
    if ( hisStorage != null ) {
      dstStorage.moveHistoryFile(filePath,hisStorage);
      dstFile = new File(srcFile.filePath,srcFile.lastModified,srcFile.length);
      registerToList(dstFolder.files,dstFile);
      log.info("copy "+filePath);
      String hashValue = srcStorage.copyRealFile(filePath,dstStorage);
      if ( srcFile.hashValue != null && !srcFile.hashValue.equals(hashValue) )
	log.warn("different hashValue : "+filePath
	  +", orig = "+srcFile.hashValue
	  +", new = "+hashValue
	  +", storage = "+srcStorage.storageName);
      dstFile.hashValue = srcFile.hashValue = hashValue;
      dstStorage.setRealLastModified(dstFile.filePath,dstFile.lastModified);
    } else {
      log.info("copy override "+filePath);
      dstStorage.deleteRealFile(filePath);
      String hashValue = srcStorage.copyRealFile(filePath,dstStorage);
      dstFile.length = srcFile.length;
      if ( srcFile.hashValue != null && !srcFile.hashValue.equals(hashValue) )
	log.warn("different hashValue : "+filePath
	  +", orig = "+srcFile.hashValue
	  +", new = "+hashValue
	  +", storage = "+srcStorage.storageName);
      dstFile.hashValue = srcFile.hashValue = hashValue;
      dstFile.lastModified = srcFile.lastModified/unit*unit;
      dstStorage.setRealLastModified(dstFile.filePath,dstFile.lastModified);
    }
  }

  public static void refresh( Storage storage )
  throws IOException
  {
    log.info("Start Refresh "+storage.storageName);
    storage.readDB();
    storage.scanFolder();
    storage.updateHashvalue();
    storage.writeDB();
    log.info("End Refresh "+storage.storageName);
  }

  public static void simulateEx( Storage srcStorage, Storage dstStorage )
  throws IOException
  {
    log.info("Start Simulation "+srcStorage.storageName+" "+dstStorage.storageName);
    srcStorage.readDB();
    dstStorage.readDB();
    simulate(srcStorage,dstStorage,null);
  }

  public static void simulate( Storage srcStorage, Storage dstStorage, Storage hisStorage )
  throws IOException
  {
    log.debug("Compare Files "+srcStorage.storageName+" "+dstStorage.storageName);
    long unit = Math.max(srcStorage.timeUnit(),dstStorage.timeUnit());
    LinkedList<File> frlist = toFileList(srcStorage); // ソートされたListに変換
    LinkedList<File> tolist = toFileList(dstStorage); // ソートされたListに変換
    LinkedList<Path> difflist = compare(frlist,tolist,unit); // frlist, tolist にはそれ以外が残る。

    // tolist means "delete"
    log.trace("simulate delete from "+dstStorage.getRoot());
    for ( File file : tolist ) {
      if ( hisStorage != null ) {
	log.info("move as history "+file.filePath+" to "+hisStorage.storageName);
      } else {
	log.info("delete "+file.filePath);
      }
    }

    // frlist means "copy"
    log.trace("simulate copy from "+srcStorage.getRoot()+" to "+dstStorage.getRoot());
    for ( File file : frlist ) {
      log.info("copy "+file.filePath);
    }

    // difflist means "set lastModifed" or "copy override" or "do nothing"
    log.trace("simulate copy override from "+srcStorage.getRoot()+" to "+dstStorage.getRoot());
    for ( Path path : difflist ) {
      log.info("copy override "+path);
    }

    log.info("End Simulation "+srcStorage.storageName+" "+dstStorage.storageName);
  }

  public static List<Storage> listDB( DataBase db )
  {
    String keys[] = db.keySet().toArray(new String[0]);
    Arrays.sort(keys);
    LinkedList<Storage> list = new LinkedList<>();
    for ( String key : keys ) {
      list.add(db.get(key));
    }
    return list;
  }

  // ======================================================================
  public static LinkedList<File> toFileList( Storage storage )
  {
    LinkedList<File> list = storage.getAllFiles();

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

  public static LinkedList<Path> compare( LinkedList<File> frlist, LinkedList<File> tolist, long unit )
  {
    LinkedList<Path> difflist = new LinkedList<>();
    ListIterator<File> fritr = frlist.listIterator();
    ListIterator<File> toitr = tolist.listIterator();
    while ( fritr.hasNext() && toitr.hasNext() ) {
      File frfile = fritr.next();
      File tofile = toitr.next();
      int cmp = frfile.filePath.compareTo(tofile.filePath);
      log.trace("compare "+frfile.filePath+((cmp < 0) ? " < " : (cmp > 0) ? " > " : " = ")+tofile.filePath);
      if ( cmp == 0 ) {
	if (
	  frfile.hashValue == null || tofile.hashValue == null || 
	  !frfile.hashValue.equals(tofile.hashValue) ||
	  frfile.lastModified/unit != tofile.lastModified/unit
	) {
	  difflist.add(frfile.filePath);
	}
	fritr.remove();
	toitr.remove();
      } else if ( cmp < 0 ) {
	toitr.previous();
      } else {
	fritr.previous();
      }
    }
    return difflist;
  }
}
