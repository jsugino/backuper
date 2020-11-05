package mylib.backuper;

import java.util.LinkedList;
import java.util.UUID;
import java.util.Iterator;
import java.util.stream.Stream;

public class ExtensibleArray<T> extends DoubleKeyHashMap<UUID,UUID,T>
{
  private LinkedList<UUID> rowkeys = new LinkedList<>();
  private LinkedList<UUID> colkeys = new LinkedList<>();

  public void ExtensibleArray()
  {
  }

  public Iterable<UUID> rowIndexes()
  {
    return rowkeys;
  }

  public Iterable<UUID> colIndexes()
  {
    return colkeys;
  }

  public void extendCol( int idx )
  {
    for ( int i = 0; i < idx; ++i ) colkeys.add(UUID.randomUUID());
  }

  public void appendRow( int offset, Stream<T> line )
  {
    insertRow(-1,offset,line);
  }

  public void insertRow( int pos, int offset, Stream<T> line )
  {
    UUID rowkey = UUID.randomUUID();
    if ( pos < 0 ) {
      rowkeys.add(rowkey);
    } else {
      rowkeys.add(pos,rowkey);
    }
    int col = 0;
    Iterator<T> itr = line.iterator();
    for ( UUID colkey : colkeys ) {
      ++col;
      if ( col <= offset ) continue;
      this.put(rowkey,colkey,itr.next());
    }
  }

  public void appendCol( int offset, Stream<T> column )
  {
    insertCol(-1,offset,column);
  }

  public void insertCol( int pos, int offset, Stream<T> column )
  {
    UUID colkey = UUID.randomUUID();
    if ( pos < 0 ) {
      colkeys.add(colkey);
    } else {
      colkeys.add(pos,colkey);
    }
    int row = 0;
    Iterator<T> itr = column.iterator();
    for ( UUID rowkey : rowkeys ) {
      ++row;
      if ( row <= offset ) continue;
      if ( !itr.hasNext() ) break;
      this.put(rowkey,colkey,itr.next());
    }
  }
}
