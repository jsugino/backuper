package mylib.backuper;

import static mylib.backuper.DataBase.attrerror;
import static mylib.backuper.DataBase.getAttr;
import static mylib.backuper.DataBase.nodeerror;
import static mylib.backuper.DataBase.selectElement;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import javax.xml.transform.TransformerException;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import mylib.backuper.DataBase.Storage;

@SuppressWarnings("serial")
public class Backup extends HashMap<String,List<Backup.Task>>
{
  public Backup()
  {
  }

  public static class Task
  {
    public String name;
    public Storage origStorage;
    public LinkedList<Storage> copyStorages = new LinkedList<>();
    public HashMap<String,Storage> historyStorages = new HashMap<>();

    public Task( String name, Storage origStorage )
    {
      this.name = name;
      this.origStorage = origStorage;
    }
  }

  public void registerElem( DataBase db, Element elem, HashMap<String,String[]> folderdefMap )
  throws IOException, TransformerException
  {
    String ref = getAttr(elem,"ref");
    String nameStr = getAttr(elem,"name");
    NodeList list = elem.getChildNodes();
    if ( ref != null ) {
      if ( nameStr != null ) {
	nodeerror("both of attribute 'name' and 'ref' are used",elem);
      } else {
	String refdefs[] = folderdefMap.get(ref);
	String names[] = new String[refdefs.length/2];
	for ( int i = 0; i < names.length; ++i ) {
	  names[i] = refdefs[i*2+1];
	}
	registerElemByNames(names,db,list);
      }
    } else if ( nameStr == null ) {
      nodeerror("neither 'name' nor 'ref' is used",elem);
    } else {
      String names[] = nameStr.split(",");
      registerElemByNames(names,db,list);
    }
  }

  public void registerElemByNames( String names[], DataBase db, NodeList list )
  throws IOException, TransformerException
  {
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
	if ( level == null ) { level = "(non)"; }

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

  public List<String> revList( String path )
  {
    String list[] = path.split("/");
    LinkedList<String> line = new LinkedList<>();
    int end = 1;
    if ( list[0].length() > 0 ) {
      if ( list[1].length() == 0 ) {
	list[2] = list[0]+"//"+list[2];
	end = 2;
      } else {
	throw new IllegalArgumentException("first path is not root : "+path);
      }
    }
    for ( int i = list.length-1; i >= end; --i ) line.add(list[i]);
    return line;
  }

  public DoubleKeyHashMap<String,String,String> toMap()
  {
    DoubleKeyHashMap<String,String,String> map = new DoubleKeyHashMap<>();
    this.forEach((level,list)->{
	list.forEach(task->{
	    String name = task.origStorage.storageName;
	    int idx = name.lastIndexOf('.');
	    String key1 = name.substring(0,idx);
	    String key2 = name.substring(idx+1);
	    map.put(key1,key2,"ORIG");
	    task.copyStorages.forEach(copy->{
		String namea = copy.storageName;
		int idxa = namea.lastIndexOf('.');
		String key1a = namea.substring(0,idxa);
		String key2a = namea.substring(idxa+1);
		map.put(key1a,key2a,level);
	      });
	  });
      });

    return map;
  }
}
