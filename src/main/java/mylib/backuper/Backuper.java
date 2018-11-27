package mylib.backuper;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.APPEND;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.ListIterator;
import mylib.backuper.DataBase.Storage;
import static mylib.backuper.DataBase.File;
import static mylib.backuper.DataBase.Folder;

public class Backuper
{
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

    try ( Logger log = new Logger(dbFolder.resolve("backup.log")) )
    {
      log.debugMode = debugMode;
      Backuper.log = log;
      try {
	DataBase db = new DataBase(dbFolder);

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

	System.err.println("[read src DB]");
	srcStorage.readDB();
	System.err.println("[scan src Folder]");
	srcStorage.scanFolder();
	System.err.println("[write src DB]");
	srcStorage.cleanupFolder(false);
	srcStorage.writeDB();

	if ( dstStorage == null ) return;

	System.err.println("[read dst DB]");
	dstStorage.readDB();
	System.err.println("[scan dst Folder]");
	dstStorage.scanFolder();
	System.err.println("[write dst DB]");
	dstStorage.writeDB();

	System.err.println("[compare]");
	LinkedList<File> frlist = toFileList(srcStorage);
	LinkedList<File> tolist = toFileList(dstStorage);
	LinkedList<File> copylist = compare(frlist,tolist);

	// frlist means "copy"
	System.err.println("[copy]");
	log.info("start copy from "+srcStorage.rootFolder+" to "+dstStorage.rootFolder);
	for ( File file : frlist ) {
	  //file.dump(System.err);
	  srcStorage.copyFile(file.filePath,dstStorage);
	}

	// copylist means "copy override"
	System.err.println("[copy override]");
	log.info("start copy override from "+srcStorage.rootFolder+" to "+dstStorage.rootFolder);
	for ( File file : copylist ) {
	  //file.dump(System.err);
	  srcStorage.copyFile(file.filePath,dstStorage);
	}

	// tolist means "delete"
	System.err.println("[delete]");
	log.info("start delete from "+dstStorage.rootFolder);
	for ( File file : tolist ) {
	  //file.dump(System.err);
	  dstStorage.deleteFile(file.filePath);
	}

	System.err.println("[clean folder]");
	dstStorage.cleanupFolder(true);
	dstStorage.writeDB();
      } catch ( Exception ex ) {
	log.error(ex);
      }
    }
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
      log.debug("compare "+frfile.filePath+((cmp < 0) ? " < " : (cmp > 0) ? " > " : " = ")+tofile.filePath);
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

  // ======================================================================
  public static SimpleDateFormat STDFORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

  public static Logger log;

  public static class Logger implements Closeable
  {
    PrintStream out;
    boolean debugMode = false;

    public Logger( Path logPath )
    {
      try {
	this.out = new PrintStream(Files.newOutputStream(logPath,CREATE,APPEND));
	info("Start Logging");
      } catch ( IOException ex ) {
	ex.printStackTrace();
      }
    }

    public void debug( String str )
    {
      if ( debugMode ) out.println(STDFORMAT.format(new Date())+" DEBUG "+str);
    }

    public void info( String str )
    {
      out.println(STDFORMAT.format(new Date())+" INFO  "+str);
    }

    public void info( Exception ex )
    {
      System.err.println(ex.getClass().getName()+": "+ex.getMessage());
      ex.printStackTrace(out);
    }

    public void error( String str )
    {
      System.err.println(str);
      out.println(STDFORMAT.format(new Date())+" ERROR "+str);
    }

    public void error( Exception ex )
    {
      ex.printStackTrace();
      ex.printStackTrace(out);
    }

    public void close()
    {
      log.info("Stop Logging");
      out.close();
    }
  }
}
