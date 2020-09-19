package mylib.backuper;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;

public class DoubleKeyHashMap<T1, T2, T3>
{
  private HashMap<Pair<T1, T2>, T3> hashMap;
  private TreeSet<T1> key1Set;
  private TreeSet<T2> key2Set;

  public DoubleKeyHashMap()
  {
    hashMap = new HashMap<Pair<T1, T2>, T3>();
    key1Set = new TreeSet<T1>();
    key2Set = new TreeSet<T2>();
  }

  @SuppressWarnings("hiding")
  class Pair<T1, T2>
  {
    T1 key1;
    T2 key2;

    Pair()
    {
      super();
    }

    Pair( T1 key1, T2 key2 )
    {
      this.key1 = key1;
      this.key2 = key2;
    }

    @Override
    public int hashCode()
    {
      return this.key1.hashCode() + this.key2.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
      if (obj == this)
	return true;
      if (!(obj instanceof Pair)) {
	return false;
      }
      @SuppressWarnings("unchecked")
	Pair<T1, T2> pair = (Pair<T1, T2>) obj;

      if (this.key1.equals(pair.key1) && this.key2.equals(pair.key2)) {
	return true;
      }
      return false;
    }
  }

  public T3 put( T1 key1, T2 key2, T3 value )
  {
    T3 previouseValue = null;
    Pair<T1,T2>key = new Pair<T1, T2>(key1,key2);
    previouseValue = this.hashMap.get(key);
    this.hashMap.put(key,value);
    this.key1Set.add(key1);
    this.key2Set.add(key2);
    return previouseValue;
  }

  public T3 get( T1 key1, T2 key2 )
  {
    T3 value = null;
    value = this.hashMap.get(new Pair<T1, T2>(key1, key2));
    return value;
  }

  public T3 remove( T1 key1, T2 key2 )
  {
    T3 previouseValue = null;
    previouseValue = this.hashMap.get(new Pair<T1, T2>(key1, key2));
    this.hashMap.remove(new Pair<T1, T2>(key1, key2));
    return previouseValue;
  }

  public boolean containsKey( T1 key1, T2 key2 )
  {
    return this.hashMap.containsKey(new Pair<T1, T2>(key1, key2));
  }

  public Set<Pair<T1, T2>> keySet()
  {
    return this.hashMap.keySet();
  }

  public Set<T1> key1Set()
  {
    return this.key1Set;
  }

  public Set<T2> key2Set()
  {
    return this.key2Set;
  }

  public int size()
  {
    return this.hashMap.size();
  }

  public Iterator<Pair<T1, T2>> iterator()
  {
    return this.keySet().iterator();
  }

  public Iterator<T1> iterator1()
  {
    return this.key1Set.iterator();
  }

  public Iterator<T2> iterator2()
  {
    return this.key2Set.iterator();
  }

  public T3 computeIfAbsent(T1 key1, T2 key2, BiFunction<T1,T2,T3> value)
  {
    return this.hashMap.computeIfAbsent(new Pair<>(key1,key2), (k) -> value.apply(key1,key2));
  }

  @Override
  public String toString()
  {
    StringBuffer stringBuffer = new StringBuffer("{");
    Set<Pair<T1, T2>> keySet = this.keySet();
    Iterator<Pair<T1, T2>> itr = keySet.iterator();
    while (itr.hasNext()) {
      Pair<T1, T2> keyPair = itr.next();
      stringBuffer.append("[" + keyPair.key1 + "," + keyPair.key2 + "]="
	+ this.hashMap.get(new Pair<T1, T2>(keyPair.key1,keyPair.key2)));
      if(itr.hasNext()) {
	stringBuffer.append(",");
      }
    }
    stringBuffer.append("}");
    return stringBuffer.toString();
  }
}
