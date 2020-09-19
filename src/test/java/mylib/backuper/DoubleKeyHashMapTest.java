package mylib.backuper;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;

import org.junit.Test;

public class DoubleKeyHashMapTest
{
  @Test
  public void testSimple()
  throws Exception
  {
    DoubleKeyHashMap<String,Integer,String> map = new DoubleKeyHashMap<>();
    assertEquals("{}",map.toString());
    map.put("A",1,"A1");
    assertEquals("{[A,1]=A1}",map.toString());
    map.put("B",2,"B2");
    map.put("A",2,"A2");
    assertEquals("{[A,1]=A1,[A,2]=A2,[B,2]=B2}",map.toString());

    Iterator<String> itr1 = map.iterator1();
    assertEquals("A",itr1.next());
    assertEquals("B",itr1.next());
    assertEquals(false,itr1.hasNext());

    Iterator<Integer> itr2 = map.iterator2();
    assertEquals(new Integer(1),itr2.next());
    assertEquals(new Integer(2),itr2.next());
    assertEquals(false,itr2.hasNext());
  }
}
