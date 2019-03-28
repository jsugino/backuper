package mylib.backuper;

import static mylib.backuper.DataBase.attrerror;
import static mylib.backuper.DataBase.getAttr;
import static mylib.backuper.DataBase.nodeerror;
import static mylib.backuper.DataBase.selectElement;
import static mylib.backuper.DataBase.serialize;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import javax.xml.transform.TransformerException;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import mylib.backuper.DataBase.Storage;

public class Backup extends HashMap<String,List<Backup.Task>>
{
  public Backup()
  {
  }

  public static class Task
  {
    String name;
    Storage origStorage;
    LinkedList<Storage> copyStorages = new LinkedList<>();

    public Task( String name, Storage origStorage )
    {
      this.name = name;
      this.origStorage = origStorage;
    }
  }

  public void registerElem( DataBase db, Element elem )
  throws IOException, TransformerException
  {
    String nameStr = getAttr(elem,"name");
    if ( nameStr == null ) { attrerror("name",elem); return; }
    String names[] = nameStr.split(",");
    NodeList list = elem.getChildNodes();
    String origStorage = null;
    for ( int i = 0; i < list.getLength(); ++i ) {
      Element copy = selectElement(list.item(i));
      if ( copy == null ) continue;
      if ( copy.getTagName().equals("original") ) {
	if ( origStorage != null ) { nodeerror("too many original tag",copy); continue; }
	origStorage = getAttr(copy,"storage");
	if ( origStorage == null ) { attrerror("storage",copy); continue; }
      } else if ( copy.getTagName().equals("copy") ) {
	String storage = getAttr(copy,"storage");
	String level = getAttr(copy,"level");
	if ( storage == null ) { attrerror("storage",copy); continue; }
	if ( level == null ) { level = "(noname)"; }
	if ( origStorage == null ) { nodeerror("no original tag before copy tag",copy); }
	List<Task> tasklist = get(level);
	if ( tasklist == null ) {
	  tasklist = new LinkedList<Task>();
	  put(level,tasklist);
	}
	for ( String name : names ) {
	  Task task = null;
	  for ( Task tk : tasklist ) {
	    if ( tk.name.equals(name) ) {
	      task = tk;
	      break;
	    }
	  }
	  if ( task == null ) {
	    String strName = name+'.'+origStorage;
	    Storage strg = db.get(strName);
	    if ( strg == null ) { nodeerror("no such storage "+strName,copy); continue; }
	    task = new Task(name,strg);
	    tasklist.add(task);
	  }
	  String strName = name+'.'+storage;
	  Storage strg = db.get(strName);
	  if ( strg == null ) { nodeerror("no such storage "+strName,copy); continue; }
	  task.copyStorages.add(strg);
	}
      } else {
	nodeerror("unknown element",copy);
      }
    }
  }

  public void dump( PrintStream out )
  {
    String keyarray[] = keySet().toArray(new String[0]);
    Arrays.sort(keyarray);
    for ( String key : keyarray ) {
      out.println(key);
      List<Task> tasklist = get(key);
      for ( Task task : tasklist ) {
	int len = task.name.length()+1;
	out.print("    "+task.name+"("+task.origStorage.storageName.substring(len));
	String delim = "->";
	for ( Storage sto : task.copyStorages ) {
	  out.print(delim+sto.storageName.substring(len));
	  delim = ",";
	}
	out.println(")");
      }
    }
  }
}
