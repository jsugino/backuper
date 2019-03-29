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
    HashMap<String,Storage> historyStorages = new HashMap<>();

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
	if ( origStorage != null ) {
	  nodeerror("too many original tag",copy); continue; }
	origStorage = getAttr(copy,"storage");
	if ( origStorage == null ) {
	  attrerror("storage",copy); continue; }

      } else if ( copy.getTagName().equals("copy") ) {
	if ( origStorage == null ) {
	  nodeerror("no original tag before copy tag",copy); continue; }

	String storage = getAttr(copy,"storage");
	if ( storage == null ) {
	  attrerror("storage",copy); continue; }

	String level = getAttr(copy,"level");
	if ( level == null ) { level = "(noname)"; }

	String history = getAttr(copy,"history");

	List<Task> tasklist = get(level);
	if ( tasklist == null ) put(level,tasklist = new LinkedList<Task>());

	String strName;
	Storage strg;
	for ( String name : names ) {
	  Task task = tasklist.stream().filter(tk->tk.name.equals(name)).findFirst().orElse(null);
	  if ( task == null ) {
	    strg = db.get(strName = name+'.'+origStorage);
	    if ( strg == null ) {
	      nodeerror("no such storage "+strName,copy); continue; }
	    tasklist.add(task = new Task(name,strg));
	  }
	  strg = db.get(strName = name+'.'+storage);
	  if ( strg == null ) {
	    nodeerror("no such storage "+strName,copy); continue; }
	  if ( task.copyStorages.contains(strg) ) {
	    nodeerror("already exists same copy "+strName,copy); continue; }
	  task.copyStorages.add(strg);

	  if ( history != null ) {
	    String hisName = strName;
	    strg = db.get(strName = history+'.'+storage);
	    if ( strg == null ) {
	      nodeerror("no such storage "+strName,copy); continue; }
	    if ( task.historyStorages.get(hisName) != null ) {
	      nodeerror("already exists same history "+hisName,copy); continue; }
	    task.historyStorages.put(hisName,strg);
	  }
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
	  Storage his = task.historyStorages.get(sto.storageName);
	  if ( his != null ) out.print("("+his.storageName+")");
	  delim = ",";
	}
	out.println(")");
      }
    }
  }
}
