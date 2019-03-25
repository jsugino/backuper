package mylib.backuper;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

public class Entry
{
  public int type = 0; // 0 : file, 1 : directory, 2 : symlink
  public Path path;
  public String contents;
  public Date lastModified;

  public Entry()
  {
  }

  public Entry( String path )
  {
    this.path = Paths.get(path);
  }

  public Entry( String path, int type )
  {
    this.path = Paths.get(path);
    this.type = type;
  }

  public Entry( Path path )
  {
    this.path = path;
  }

  public Entry( Path parent, String name )
  {
    if ( name.charAt(0) == '@' ) {
      name = name.substring(1,name.length());
      this.type = 2;
    }
    this.path = parent.resolve(name).normalize();
  }

  public Entry( String path, String contents )
  {
    this.path = Paths.get(path);
    this.contents = contents;
  }

  @Override
  public String toString()
  {
    String tp = type == 0 ? "" : type == 1 ? "[dir]" : type == 2 ? "[link]" : "[error]";
    String con = contents == null ? "" : "="+contents;
    String dt = lastModified == null ? "" : "("+DataBase.STDFORMAT.format(lastModified)+")";
    return tp+path+con+dt;
  }

  @Override
  public boolean equals(Object obj) {
    if ( obj == null ) return false;
    if ( !(obj instanceof Entry) ) return false;
    Entry ent = (Entry)obj;
    if ( type != ent.type ) return false;
    if ( !path.equals(ent.path) ) return false;
    if ( contents == null ) {
      if ( ent.contents != null ) return false;
    } else {
      if ( !contents.equals(ent.contents) )  return false;
    }
    if ( lastModified == null || ent.lastModified == null ) return true;
    return lastModified.equals(ent.lastModified);
  }

  // ----------------------------------------------------------------------

  /**
   * ディレクトリ構造を表すデータを List に変換する。
   *
   * @param data ディレクトリ構造を表すデータ
   * @param dirFirst<br>
   *           未指定 : ディレクトリなし<br>
   *           true   : ディレクトリ→ファイルの順 (ファイル生成時に使用)<br>
   *           false  : ファイル→ディレクトリの順 (ファイル削除時に使用)
   * @return フラット化したファイルとディレクトリ
   */
  public static List<Entry> walkData( Object data[], boolean ... dirFirst )
  {
    LinkedList<Entry> list = new LinkedList<>();
    if ( dirFirst.length == 0 ) {
      walkData(list,Paths.get("."),data,0);
    } else if ( dirFirst[0] ) {
      walkData(list,Paths.get("."),data,1);
    } else {
      walkData(list,Paths.get("."),data,2);
    }
    return list;
  }

  /**
   * ディレクトリ構造を表すデータを List に変換する。
   *
   *
   * @param list フラット化したファイルとディレクトリを入れる場所
   * @param dir 取得するディレクトリ
   * @param data ディレクトリ構造を表すデータ
   * @param option<br>
   *           0 : ディレクトリなし<br>
   *           1 : ディレクトリ→ファイルの順 (ファイル生成時に使用)<br>
   *           2 : ファイル→ディレクトリの順 (ファイル削除時に使用)
   */
  public static void walkData( List<Entry> list, Path dir, Object data[], int option )
  {
    for ( int i = 0; i < data.length; i += 2 ) {
      if ( !(data[i] instanceof String) )
	throw new IllegalArgumentException("data["+i+"] is not a String : "+data[i]);
      Entry ent = new Entry(dir,(String)data[i]);
      String contents = null;
      if ( data[i+1] instanceof String ) {
	contents = (String)data[i+1];
      } else if ( data[i+1] instanceof String[] ) {
	StringWriter buf = new StringWriter();
	PrintWriter out = new PrintWriter(buf);
	Stream.of((String[])data[i+1]).forEach(out::println);
	contents = buf.toString();
      } else if ( data[i+1] instanceof Object[] ) {
	ent.type = 1;
	if ( option == 1 ) list.add(ent);
	walkData(list,ent.path,(Object[])data[i+1],option);
	if ( option == 2 ) list.add(ent);
	continue;
      } else {
	throw new IllegalArgumentException("unknown data type : "
	  +data[i+1].toString()
	  +", class = "+data[i+1].getClass().getName());
      }
      ent.contents = contents;
      if ( i+2 < data.length ) {
	if ( data[i+2] instanceof Date ) {
	  ent.lastModified = (Date)data[i+2];
	  ++i;
	} else if ( data[i+2] instanceof Long ) {
	  ent.lastModified = new Date((Long)data[i+2]);
	  ++i;
	}
      }
      list.add(ent);
    }
  }
}
