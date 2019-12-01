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

  public void printTask( PrintStream out )
  {
    this.forEach((key,list)->{
	out.println("["+key+"]");
	list.forEach(task->{
	    Storage orig = task.origStorage;
	    out.println("    "+orig.getRoot()+" ("+orig.storageName+")");
	    task.copyStorages.forEach(sto->{
		out.println("        "+sto.getRoot()+" ("+sto.storageName+")");
		Storage his = task.historyStorages.get(sto.storageName);
		if ( his != null )
		  out.println("          history "+his.getRoot());
	      });
	  });
      });
    System.out.println();

    HashMap<List<String>,HashMap<List<String>,String>> table = new HashMap<>();
    this.forEach((key,list)->{
	list.forEach(task->{
	    HashMap<List<String>,String> line = new HashMap<>();
	    line.put(revList(task.origStorage.getRoot()),"ORIG");
	    task.copyStorages.forEach(sto->{
		line.put(revList(sto.getRoot()),key);
		Storage his = task.historyStorages.get(sto.storageName);
		if ( his != null ) {
		  line.put(revList(his.getRoot()+sto.getRoot()),
		    (key.length() > 4 ? key.substring(0,4) : key) + "(h)");
		}
	      });
	    LinkedList<String> common = new LinkedList<>();
	    while ( true ) {
	      if (
		line.keySet().stream()
		.map(List::size)
		.min(Comparator.naturalOrder())
		.get() == 0 ) break;
	      HashSet<String> set = new HashSet<String>();
	      line.keySet().forEach(l->set.add(l.get(0)));
	      if ( set.size() != 1 ) break;
	      set.forEach(common::add);
	      line.keySet().forEach(l->l.remove(0));
	    }
	    line.forEach((l,t)->{
		HashMap<List<String>,String> li = table.get(common);
		if ( li == null ) table.put(common,li = new HashMap<>());
		li.put(l,t);
	      });
	  });
      });

    TreeSet<List<String>> keyset = new TreeSet<>(
      new Comparator<List<String>>(){
	public int compare( List<String> a, List<String> b ) {
	  int as = a.size();
	  int bs = b.size();
	  for ( int i = 0; i < Math.min(as,bs); ++i ) {
	    String av = a.get(as-i-1);
	    String bv = b.get(bs-i-1);
	    if ( av.indexOf(':') > 0 ) return 1;
	    if ( bv.indexOf(':') > 0 ) return -1;
	    int cmp = av.compareTo(bv);
	    if ( cmp != 0 ) return cmp;
	  }
	  return as - bs;
	}
      });
    table.forEach((key,hash)->hash.forEach((k,val)->keyset.add(k)));

    int max = keyset.stream().map(List::size).max(Comparator.naturalOrder()).get();
    for ( int i = 0; i < max; ++i ) {
      final int x = i;
      keyset.forEach(list->{
	  String col = "\t";
	  if ( list.size() == 0 && x == 0 ) {
	    col = "/\t";
	  } else if ( list.size() > x ) {
	    col = list.get(list.size()-x-1);
	    if ( col.indexOf(':') > 0 ) {
	      if ( col.length() > 7 ) col = col.substring(0,7);
	    } else {
	      if ( col.length() > 6 ) col = col.substring(0,6);
	      col = "/"+col;
	    }
	    if ( list != keyset.last() ) {
	      col = col+"\t";
	    }
	  }
	  System.out.print(col);
	});
      System.out.println();
    }
    for ( int i = 0; i < keyset.size(); ++i ) {
      System.out.print("--------");
    }
    System.out.println("--------");

    table.forEach((key,line)->{
	keyset.forEach(sto->{
	    System.out.print(line.getOrDefault(sto,"")+"\t");
	  });
	if ( key.size() == 0 ) {
	  System.out.println("/");
	} else {
	  for ( int i = key.size(); i > 0; --i ) System.out.print("/"+key.get(i-1));
	  System.out.println();
	}
      });
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
}
