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
      MainEx command = new MainEx();
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

  public void backup( Storage srcStorage, Storage dstStorage )
  throws IOException
  {
    prepareDB(srcStorage);
    prepareDB(dstStorage);
    if ( doExecute ) {
      Main.backup(srcStorage,dstStorage,null,forceCopy);
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
      Main.rearrange(srcStorage,dstStorage,true);
      dstStorage.cleanupFolder();
      srcStorage.writeDB();
      dstStorage.writeDB();
    } else {
      Main.rearrange(srcStorage,dstStorage,false);
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
}
