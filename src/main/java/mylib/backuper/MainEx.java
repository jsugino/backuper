package mylib.backuper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mylib.backuper.DataBase.File;
import mylib.backuper.DataBase.Folder;
import mylib.backuper.DataBase.Storage;
import mylib.backuper.Backup.Task;

import static mylib.backuper.DataBase.registerToList;
import static mylib.backuper.Main.toFileList;
import static mylib.backuper.Main.compare;
import static mylib.backuper.Main.copyFile;
import static mylib.backuper.Main.CONFIGXML;
import static mylib.backuper.Main.UsageException;
import static mylib.backuper.Main.log;

public class MainEx
{
  public static void main( String argv[] )
  {
    if ( argv.length > 0 && argv[0].equals("--old") ) {
      String newa[] = new String[argv.length-1];
      for ( int i = 1; i < argv.length; ++i ) newa[i-1] = argv[i];
      Main.main(newa);
      return;
    }
    try {
      Path dbFolder = Paths.get(argv[0]);
      try ( DataBase database = new DataBase(dbFolder) ) {
	if ( !Files.isReadable(dbFolder.resolve(CONFIGXML)) )
	  throw new UsageException("No definition file "+CONFIGXML);
	Backup bkTasks = database.initializeByXml(dbFolder.resolve(CONFIGXML));

	MainEx command = new MainEx();
	command.parseOption(argv,1);
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
      System.err.println("    -f         : force execute (evan 10 or more delete files)");
      System.err.println("    -s         : scan only (not update MD5)");
      System.err.println("    -S         : full scan only (update MD5 for all files)");
      System.err.println("    -n         : no scan (use *.db only)");
      System.err.println("    -d         : simulate file copy (no scan)");
    } catch ( Exception ex ) {
      log.error(ex.getMessage(),ex);
    }
  }

  // ----------------------------------------------------------------------
  public String option = "";
  public boolean forceCopy = false;
  public boolean doScan = true;
  public boolean doExecute = true;
  public String arg1 = null;
  public String arg2 = null;
  public String arg3 = null;

  public void parseOption( String argv[], int argcnt )
  {
    if ( argv.length < argcnt+1 ) throw new UsageException("Less arguments");
    while ( argv[argcnt].length() > 0 && argv[argcnt].charAt(0) == '-' ) {
      option = argv[argcnt++];
      int idx = option.indexOf('f',1);
      if ( idx > 0 ) {
	forceCopy = true;
	if ( option.length() == 2 ) {
	  option = "";
	  continue;
	}
	option = "-"+option.substring(1,idx)+option.substring(idx+1);
      }
      break;
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
      throw new UsageException("'execute task' is not implemented)");

    } else if ( option.equals("-l") ) {
      throw new UsageException("'print definition' is not implemented)");

    } else if ( option.equals("-S" ) ) {
      if ( arg1 == null ) throw new UsageException("no argument for -S");
      Storage storage = database.get(arg1);
      if ( storage != null ) {
	if ( arg2 != null ) throw new UsageException("Unused arguments "+arg2);
	fullScan(storage);
      } else {
	throw new UsageException("Unknown ID "+arg1);
      }

    } else if ( option.equals("-s" ) ) {
      System.out.println("scan (not implemented)");

    } else if ( option.equals("-n" ) ) {
      doScan = false;
      if ( arg1 == null ) throw new UsageException("no argument for -n");
      List<Task> tasks = bkTasks.get(arg1);
      if ( tasks != null ) {
	log.info("backup without scan by level : "+arg1);
	backup(tasks);
      } else {
	throw new UsageException("Unknown ID "+arg1);
      }

    } else if ( option.equals("-d" ) ) {
      doScan = false;
      doExecute = false;
      if ( arg1 == null ) throw new UsageException("no argument for -d");
      List<Task> tasks = bkTasks.get(arg1);
      if ( tasks != null ) {
	log.info("simulate by level : "+arg1);
	backup(tasks);
      } else {
	throw new UsageException("Unknown ID "+arg1);
      }
    } else {
      throw new UsageException("undefined option "+option);
    }
  }

  // ----------------------------------------------------------------------
  public void fullScan( Storage storage )
  {
    throw new UsageException("fullScan() is not implemented)");
  }

  /**
   * レベルに対するバックアップ処理を行う。
   *
   * @param doScan true のとき、スキャンする
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
	  Main.backup(orig,copy,his,forceCopy);
	} else {
	  Main.simulate(orig,copy,his);
	}
	finalizeDB(copy);
	finalizeDB(his);
      }
      finalizeDB(orig);
    }
  }

  /**
   * readDB() した後に doScan により scanFolder() か complementFolders() を呼び出す。
   **/
  public void prepareDB( Storage storage )
  throws IOException
  {
    if ( storage == null ) return;
    storage.readDB();
    if ( doScan ) {
      storage.scanFolder();
    } else {
      storage.complementFolders();
    }
  }

  public void finalizeDB( Storage storage )
  throws IOException
  {
    if ( storage == null ) return;
    if ( doExecute ) {
      storage.writeDB();
    }
  }
}
