package mylib.backuper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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
    if ( argv.length > 0 && argv[0].equals("--old") ) {
      String newa[] = new String[argv.length-1];
      for ( int i = 1; i < argv.length; ++i ) newa[i-1] = argv[i];
      Main.main(newa);
      return;
    }

    try {
      Main command = new Main();
      command.parseOption(argv,1);

      Path dbFolder = Paths.get(argv[0]);
      try ( DataBase database = new DataBase(dbFolder) ) {
	if ( !Files.isReadable(dbFolder.resolve(CONFIGXML)) )
	  throw new UsageException("No definition file "+CONFIGXML);
	Backup bkTasks = database.initializeByXml(dbFolder.resolve(CONFIGXML));
	command.execute(database,bkTasks);
      }
    } catch ( UsageException ex ) {
      log.error(ex.getMessage(),ex);
      System.err.println("usage : java -jar jarfiile.jar DicFolder [option] [level] [src [dst]]");
      System.err.println("    DicFolder : database folder");
      System.err.println("    level     : task level (ex. daily)");
      System.err.println("    src       : source id (ex. C, common.C, etc.)");
      System.err.println("    dst       : destination id (ex. D, common.D, etc.)");
      System.err.println("  Option");
      System.err.println("    -l         : print definition");
      System.err.println("    -f         : force execute (evan 10 or more delete or override files)");
      System.err.println("    -s         : scan only");
      System.err.println("    -r         : rearrange only");
      System.err.println("    -n         : no preparation");
      System.err.println("    -d         : simulate");
    } catch ( Exception ex ) {
      log.error(ex.getMessage(),ex);
    }
  }

  // ----------------------------------------------------------------------
  public String option = "";
  public boolean forceCopy = false;
  public boolean doPrepare = true;
  public boolean doExecute = true;
  public String arg1 = null;
  public String arg2 = null;
  public String arg3 = null;

  public void parseOption( String argv[], int argcnt )
  {
    if ( argv.length < argcnt+1 ) throw new UsageException("Less arguments");

    if ( argv[argcnt].length() > 0 && argv[argcnt].charAt(0) == '-' ) {
      option = argv[argcnt++];

      String opts[] = new String[]{
	"", "-d", "-l", "-f",
	"-s", "-sn", "-ns",
	"-r", "-rd", "-dr", "-rn", "-nr", "-dnr", "-ndr", "-nrd", "-drn", "-rdn", "-rnd",
	"-n", "-fn", "-nf",
	"-nd", "-dn",
      };
      for ( int i = 0; i < opts.length; ++i ) {
	if ( option.equals(opts[i]) ) {
	  opts = null;
	  break;
	}
      }
      if ( opts != null )
	throw new UsageException("Unknown option : "+option);

      int idx = 1;
      while ( idx < option.length() ) {
	boolean find = false;
	switch ( option.charAt(idx) ) {
	case 'f':
	  forceCopy = true;
	  find = true;
	  break;
	case 'n':
	  doPrepare = false;
	  find = true;
	  break;
	case 'd':
	  doExecute = false;
	  find = true;
	  break;
	}
	if ( find ) {
	  option = option.substring(0,idx)+option.substring(idx+1);
	} else {
	  ++idx;
	}
      }
      if ( option.equals("-") ) option = "";
    }
    if ( argcnt < argv.length ) arg1 = argv[argcnt++];
    if ( argcnt < argv.length ) arg2 = argv[argcnt++];
    if ( argcnt < argv.length ) arg3 = argv[argcnt++];
    if ( argcnt < argv.length ) throw new UsageException("Unused argument "+argv[argcnt]);
  }

  public void execute( DataBase database, Backup bkTasks )
  throws IOException
  {

    if ( option.equals("") ) {
      if ( arg1 == null ) throw new UsageException("No Argument");
      List<Task> tasks = bkTasks.get(arg1);
      if ( tasks != null ) {
	if ( arg2 != null ) throw new UsageException("Unused Arguments "+arg2);
	log.info("Start Backup with level "+arg1);
	backup(tasks);
	log.info("End Backup with level "+arg1);
	return;
      }
      if ( arg2 == null ) throw new UsageException("Less Argument "+arg1);
      if ( arg3 == null ) throw new UsageException("Less Argument "+arg1+" "+arg2);

      Storage srcStorage = database.get(arg1+'.'+arg2);
      if ( srcStorage == null ) throw new UsageException("Unknown Storage Name "+arg1+'.'+arg2);
      Storage dstStorage = database.get(arg1+'.'+arg3);
      if ( dstStorage == null ) throw new UsageException("Unknown Storage Name "+arg1+'.'+arg3);

      log.info("Start Backup "+srcStorage.storageName+" "+dstStorage.storageName);
      backup(srcStorage,dstStorage);
      log.info("End Backup "+srcStorage.storageName+" "+dstStorage.storageName);

    } else if ( option.equals("-l") ) {
      if ( arg1 != null ) throw new UsageException("Unused Arguments for -l "+arg1);
      printConfig(System.out,database,bkTasks);

    } else if ( option.equals("-s" ) ) {
      if ( arg1 == null ) throw new UsageException("No Argument for -s");
      Storage storage = database.get(arg1);
      if ( storage != null ) {
	if ( arg2 != null ) throw new UsageException("Unused Arguments for -s "+arg2);
	log.info("Start Refresh "+storage.storageName);
	storage.readDB();
	storage.scanFolder(false,false);
	storage.updateHashvalue(!doPrepare);
	storage.writeDB();
	log.info("End Refresh "+storage.storageName);
	return;
      }
      throw new UsageException("Unknown ID for -s "+arg1);

    } else if ( option.equals("-r") ) {
      if ( arg1 == null ) throw new UsageException("No Argument for -r");
      Storage storage = database.get(arg1);
      if ( storage != null ) {
	if ( arg2 != null ) throw new UsageException("Unused Arguments for -r "+arg2);
	log.info("Start Rearrange "+storage.storageName);
	rearrange(storage);
	log.info("End Rearrange "+storage.storageName);
	return;
      }
      if ( arg2 == null ) throw new UsageException("Less Argument for -r "+arg1);
      if ( arg3 == null ) throw new UsageException("Less Argument for -r "+arg1+" "+arg2);

      Storage srcStorage = database.get(arg1+'.'+arg2);
      if ( srcStorage == null ) throw new UsageException("Unknown Storage Name for -r "+arg1+'.'+arg2);
      Storage dstStorage = database.get(arg1+'.'+arg3);
      if ( dstStorage == null ) throw new UsageException("Unknown Storage Name for -r "+arg1+'.'+arg3);

      log.info("Start Rearrange "+srcStorage.storageName+" "+dstStorage.storageName);
      rearrange(srcStorage,dstStorage);
      log.info("End Rearrange "+srcStorage.storageName+" "+dstStorage.storageName);
    } else {
      throw new UsageException("Undefined Option "+option);
    }
  }

  // ----------------------------------------------------------------------
  /**
   * レベルに対するバックアップ処理を行う。
   *
   * @param doPrepare true のとき、スキャンする
   * @param doExecute true のとき、コピーを実行する
   */
  public void backup( List<Task> tasks )
  throws IOException
  {
    for ( Task task : tasks ) {
      Storage orig = task.origStorage;
      prepareDB(orig);
      for ( Storage copy : task.copyStorages ) {
	Storage his = task.historyStorages.get(copy.storageName);
	prepareDB(his);
	prepareDB(copy);
	if ( doExecute ) {
	  Main.backupEx(orig,copy,his,forceCopy);
	} else {
	  Main.simulate(orig,copy,his);
	}
	finalizeDB(copy);
	finalizeDB(his);
      }
      finalizeDB(orig);
    }
  }

  public void backup( Storage srcStorage, Storage dstStorage )
  throws IOException
  {
    prepareDB(srcStorage);
    prepareDB(dstStorage);
    if ( doExecute ) {
      Main.backupEx(srcStorage,dstStorage,null,forceCopy);
    } else {
      Main.simulate(srcStorage,dstStorage,null);
    }
    finalizeDB(srcStorage);
    finalizeDB(dstStorage);
  }

  /**
   * readDB() した後に doPrepare により scanFolder() か complementFolders() を呼び出す。
   **/
  public void prepareDB( Storage storage )
  throws IOException
  {
    if ( storage == null ) return;
    storage.readDB();
    if ( doPrepare ) {
      storage.scanFolder(true,false);
    } else {
      storage.complementFolders();
    }
  }

  public void finalizeDB( Storage storage )
  throws IOException
  {
    if ( storage == null ) return;
    if ( doPrepare || doExecute ) {
      storage.writeDB();
    }
  }

  // ----------------------------------------------------------------------
  public void rearrange( Storage storage )
  throws IOException
  {
    storage.readDB();
    if ( doPrepare ) {
      storage.scanFolder(false,true);
    } else {
      storage.complementFolders();
    }
    if ( doExecute ) {
      storage.writeDB();
    }
  }

  public void rearrange( Storage srcStorage, Storage dstStorage )
  throws IOException
  {
    srcStorage.readDB();
    dstStorage.readDB();
    if ( doPrepare ) {
      srcStorage.scanFolder(true,true);
      dstStorage.scanFolder(true,true);
    } else {
      srcStorage.complementFolders();
      dstStorage.complementFolders();
    }
    if ( doExecute ) {
      dstStorage.updateHashvalue(false);
      Main.rearrangeEx(srcStorage,dstStorage,true);
      dstStorage.cleanupFolder();
      srcStorage.writeDB();
      dstStorage.writeDB();
    } else {
      Main.rearrangeEx(srcStorage,dstStorage,false);
    }
  }

  public static void rearrangeEx( Storage srcStorage, Storage dstStorage, boolean doExecute )
  throws IOException
  {
    log.debug("Rearrange Files "+srcStorage.storageName+" "+dstStorage.storageName);
    long unit = Math.max(srcStorage.timeUnit(),dstStorage.timeUnit());
    LinkedList<File> frlist = toFileList(srcStorage); // ソートされたListに変換
    LinkedList<File> tolist = toFileList(dstStorage); // ソートされたListに変換
    LinkedList<Path> difflist = compare(frlist,tolist,unit); // frlist, tolist にはそれ以外が残る。

    HashMap<String,File> frmap = new HashMap<>();
    for ( File file : frlist ) {
      if ( file.hashValue == null ) continue;
      log.trace("rearrange use MD5 : "+file.filePath+" "+file.hashValue);
      frmap.put(file.hashValue,file);
    }
    for ( File file : tolist ) {
      if ( file.hashValue == null ) continue;
      log.trace("rearrange find MD5 : "+file.filePath+" "+file.hashValue);
      File orig = frmap.get(file.hashValue);
      if ( orig == null ) continue;
      if ( doExecute ) {
	dstStorage.moveFile(file.filePath,orig.filePath);
      } else {
	log.info("move "+file.filePath+' '+orig.filePath);
      }
    }
  }

  // ----------------------------------------------------------------------
  public static class ViString implements Comparable<ViString>
  {
    private String value;
    private boolean visible;

    public ViString( String value, boolean visible )
    {
      this.value = value;
      this.visible = visible;
    }

    @Override
    public int hashCode()
    {
      return value.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
      if ( obj == this ) return true;
      if ( !(obj instanceof ViString) ) return false;
      ViString other = (ViString)obj;
      return this.value.equals(other.value);
    }

    @Override
    public String toString()
    {
      return visible ? value : "";
    }

    @Override
    public int compareTo( ViString other )
    {
      return this.value.compareTo(other.value);
    }
  }

  public static void printConfig( java.io.PrintStream out, DataBase db, Backup bk )
  {
    DoubleKeyHashMap<ViString,String,String> map = new DoubleKeyHashMap<>();
    db.toMap().forEach((key,val)->map.put(new ViString(key.key1,true),key.key2,val));
    bk.toMap().forEach((key,val)->map.put(new ViString(key.key1+"0",false),key.key2,val));
    map.pretyPrint(out,"","/.");
  }

  // ======================================================================
  public static void backupEx( Storage srcStorage, Storage dstStorage, Storage hisStorage, boolean forceCopy )
  throws IOException
  {
    log.debug("Compare Files "+srcStorage.storageName+" "+dstStorage.storageName);
    long unit = Math.max(srcStorage.timeUnit(),dstStorage.timeUnit());
    LinkedList<File> frlist = toFileList(srcStorage); // ソートされたListに変換
    LinkedList<File> tolist = toFileList(dstStorage); // ソートされたListに変換
    LinkedList<Path> difflist = compare(frlist,tolist,unit); // frlist, tolist にはそれ以外が残る。

    // tolist means "delete"
    if ( tolist.size() > 10 && !forceCopy ) {
      log.error("too many delete files : "+tolist.size());
      return;
    }
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
      if ( srcFile.hashValue == null ) srcFile.hashValue = srcStorage.getMD5(filePath,false);
      if ( dstFile.hashValue == null ) dstFile.hashValue = dstStorage.getMD5(filePath,false);
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
    storage.scanFolder(true,false);
    storage.updateHashvalue(false);
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
    log.info("End Simulation "+srcStorage.storageName+" "+dstStorage.storageName);
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
