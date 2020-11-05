package mylib.backuper;

import static org.junit.Assert.assertEquals;

import java.util.stream.Stream;
import java.util.UUID;

import org.junit.Test;

public class ExtensibleArrayTest
{
  @Test
  public void testAppend()
  {
    String exp[];
    int cnt;

    ExtensibleArray<String> array = new ExtensibleArray<>();
    array.extendCol(3);
    array.appendRow(0,Stream.of("a","b","c"));
    array.appendRow(1,Stream.of("a1","b1","c1"));
    cnt = 0;
    exp = new String[]{"a","b","c",null,"a1","b1"};
    for ( UUID row : array.rowIndexes() ) {
      for ( UUID col : array.colIndexes() ) {
	assertEquals("cnt="+cnt,exp[cnt],array.get(row,col));
	++cnt;
      }
    }

    array.appendCol(0,Stream.of("A","B"));
    array.appendCol(1,Stream.of("A1","B1"));
    cnt = 0;
    exp = new String[]{"a","b","c","A",null,null,"a1","b1","B","A1"};
    /*
    for ( UUID row : array.rowkeys ) {
      for ( UUID col : array.colkeys ) {
	System.out.print(array.get(row,col)+',');
      }
      System.out.println();
    }
    */
    for ( UUID row : array.rowIndexes() ) {
      for ( UUID col : array.colIndexes() ) {
	assertEquals("cnt="+cnt,exp[cnt],array.get(row,col));
	++cnt;
      }
    }
  }

  @Test
  public void testInsert()
  {
    String exp[];
    int cnt;

    ExtensibleArray<String> array = new ExtensibleArray<>();
    array.extendCol(3);
    array.insertRow(0,0,Stream.of("a","b","c"));
    array.insertRow(0,1,Stream.of("a1","b1","c1"));
    array.insertRow(1,2,Stream.of("a2","b2","c2"));
    cnt = 0;
    exp = new String[]{null,"a1","b1",null,null,"a2","a","b","c",};
    for ( UUID row : array.rowIndexes() ) {
      for ( UUID col : array.colIndexes() ) {
	assertEquals("cnt="+cnt,exp[cnt],array.get(row,col));
	++cnt;
      }
    }

    array.insertCol(0,0,Stream.of("A","B"));
    array.insertCol(0,1,Stream.of("A1","B1","C1"));
    array.insertCol(1,1,Stream.of("A2","B2","C2"));
    cnt = 0;
    exp = new String[]{null,null,"A",null,"a1","b1", "A1","A2","B",null,null,"a2", "B1","B2",null,"a","b","c",};
    /*
    for ( UUID row : array.rowkeys ) {
      for ( UUID col : array.colkeys ) {
	System.out.print(array.get(row,col)+',');
      }
      System.out.println();
    }
    */
    for ( UUID row : array.rowIndexes() ) {
      for ( UUID col : array.colIndexes() ) {
	assertEquals("cnt="+cnt,exp[cnt],array.get(row,col));
	++cnt;
      }
    }
  }
}
