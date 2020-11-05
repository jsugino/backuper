package mylib.backuper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;

public class DoubleKeyHashMap<T1, T2, T3> extends HashMap<DoubleKeyHashMap.Pair<T1, T2>, T3>
{
  private TreeSet<T1> key1Set;
  private TreeSet<T2> key2Set;

  public DoubleKeyHashMap()
  {
    key1Set = new TreeSet<T1>();
    key2Set = new TreeSet<T2>();
  }

  public static class Pair<T1, T2>
  {
    public T1 key1;
    public T2 key2;

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
      if ( obj == this ) return true;
      if ( !(obj instanceof Pair) ) return false;
      Pair pair = (Pair)obj;
      return this.key1.equals(pair.key1) && this.key2.equals(pair.key2);
    }

    @Override
    public String toString()
    {
      return "[" + this.key1 + "," + this.key2 + "]";
    }
  }

  @Override
  public T3 put( Pair<T1,T2> key, T3 value )
  {
    T3 previouseValue = super.put(key,value);
    this.key1Set.add(key.key1);
    this.key2Set.add(key.key2);
    return previouseValue;
  }

  public T3 put( T1 key1, T2 key2, T3 value )
  {
    T3 previouseValue = null;
    Pair<T1,T2>key = new Pair<T1, T2>(key1,key2);
    previouseValue = this.get(key);
    this.put(key,value);
    this.key1Set.add(key1);
    this.key2Set.add(key2);
    return previouseValue;
  }

  public T3 get( T1 key1, T2 key2 )
  {
    T3 value = null;
    value = this.get(new Pair<T1, T2>(key1, key2));
    return value;
  }

  public T3 removePair( T1 key1, T2 key2 )
  {
    T3 previouseValue = null;
    previouseValue = this.get(new Pair<T1, T2>(key1, key2));
    this.remove(new Pair<T1, T2>(key1, key2));
    return previouseValue;
  }

  public boolean containsKey( T1 key1, T2 key2 )
  {
    return this.containsKey(new Pair<T1, T2>(key1, key2));
  }

  public Set<T1> key1Set()
  {
    return this.key1Set;
  }

  public Set<T2> key2Set()
  {
    return this.key2Set;
  }

  public Iterator<T1> iterator1()
  {
    return this.key1Set.iterator();
  }

  public Iterator<T2> iterator2()
  {
    return this.key2Set.iterator();
  }

  public T3 computeIfAbsent( T1 key1, T2 key2, BiFunction<T1,T2,T3> value )
  {
    return this.computeIfAbsent(new Pair<>(key1,key2), (k) -> value.apply(key1,key2));
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
	+ this.get(new Pair<T1, T2>(keyPair.key1,keyPair.key2)));
      if(itr.hasNext()) {
	stringBuffer.append(",");
      }
    }
    stringBuffer.append("}");
    return stringBuffer.toString();
  }

  // --------------------------------------------------
  public void pretyPrint( java.io.PrintStream out, String whenNull, String whenEmpty )
  {
    String lines[] = new String[this.key1Set.size()+1];
    lines[0] = "";
    int cnt = 1;
    for ( T1 key1 : this.key1Set ) lines[cnt++] = key1.toString();
    padding(lines);
    for ( T2 key2 : this.key2Set ) {
      lines[0] = lines[0] + ' ' + key2.toString();
      cnt = 0;
      for ( T1 key1 : this.key1Set ) {
	T3 val = this.get(key1,key2);
	String str;
	if ( val == null ) {
	  str = whenNull;
	} else {
	  str = val.toString();
	  if ( str.length() == 0 ) str = whenEmpty;
	}
	lines[++cnt] += " " + str;
      }
      padding(lines);
    }
    for ( String line : lines ) {
      for ( int i = line.length(); i > 0; --i ) {
	if ( line.charAt(i-1) != ' ' ) {
	  line = line.substring(0,i);
	  break;
	}
      }
      out.println(line);
    }
  }

  public static void padding( String lines[] )
  {
    int maxlen = Arrays.stream(lines).mapToInt(l->l.length()).max().orElse(-1);
    if ( maxlen < 0 ) return;
    for ( int i = 0; i < lines.length; ++i ) {
      char col[] = Arrays.copyOf(lines[i].toCharArray(),maxlen);
      for ( int k = lines[i].length(); k < maxlen; ++k ) col[k] = ' ';
      lines[i] = new String(col);
    }
  }
}
