package mylib.backuper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.LinkedList;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DerbyTest
{
  @Rule
  public TemporaryFolder tempdir = new TemporaryFolder(new File("target"));

  public String DBURL;

  @Before
  public void initDBURL()
  {
    DBURL = "jdbc:derby:"+
      tempdir.getRoot().toPath().toAbsolutePath().resolve("testdb").toString();
  }

  @Test
  public void testSimple()
  throws Exception
  {
    try (
      Connection con = DriverManager.getConnection(DBURL+";create=true");
      Statement stmt = con.createStatement()
    ) {
      stmt.executeUpdate("create table testtable (id int, name varchar(32000))");
      stmt.executeUpdate("insert into testtable values (10,'data'),(20,'this'),(30,'that')");
      ResultSet rset = stmt.executeQuery("select * from testtable");
      assertTrue(rset.next());
      assertEquals(10,rset.getInt(1));
      assertEquals("data",rset.getString(2));
      assertTrue(rset.next());
      assertEquals(20,rset.getInt(1));
      assertEquals("this",rset.getString(2));
      assertTrue(rset.next());
      assertEquals(30,rset.getInt(1));
      assertEquals("that",rset.getString(2));
      assertFalse(rset.next());
    }
  }

  // Iterator で取得できるか試す。
  @Test
  public void testIterator()
  throws Exception
  {
    try ( SampleDB db = new SampleDB(DBURL) ) {
      LinkedList<Element> onlya = new LinkedList<>();
      LinkedList<Element> onlyb = new LinkedList<>();
      LinkedList<Integer> same = new LinkedList<>();
      try (
	ElementSequence resa = db.queryAll("a");
	ElementSequence resb = db.queryAll("b");
      ) {
	Element elema = resa.next();
	Element elemb = resb.next();
	while ( elema != null && elemb != null ) {
	  if ( elema.id < elemb.id ) {
	    onlya.add(elema);
	    elema = resa.next();
	  } else if ( elema.id > elemb.id ) {
	    onlyb.add(elemb);
	    elemb = resb.next();
	  } else {
	    same.add(elema.id);
	    elema = resa.next();
	    elemb = resb.next();
	  }
	}
	while ( elema != null ) {
	  onlya.add(elema);
	  elema = resa.next();
	}
	while ( elemb != null ) {
	  onlyb.add(elemb);
	  elemb = resb.next();
	}
      }
      Element elem;
      assertEquals(1,onlya.size());
      elem = onlya.remove();
      assertEquals(20,elem.id);

      assertEquals(1,onlyb.size());
      elem = onlyb.remove();
      assertEquals(22,elem.id);

      assertEquals(2,same.size());
      assertEquals(10,same.remove().intValue());
      assertEquals(30,same.remove().intValue());
    }
  }

  public static class SampleDB implements Closeable
  {
    public Connection con;

    public SampleDB( String dburl )
    throws IOException
    {
      try {
	con = DriverManager.getConnection(dburl+";create=true");
	try ( Statement stmt = con.createStatement() ) {
	  stmt.executeUpdate("create table testtable (type varchar(5), id int, name varchar(32000))");
	  stmt.executeUpdate("insert into testtable values ('a',10,'data'),('a',20,'this'),('a',30,'that')");
	  stmt.executeUpdate("insert into testtable values ('b',10,'data'),('b',22,'this'),('b',30,'that')");
	}
      } catch ( SQLException ex ) {
	throw new IOException(ex.getMessage(),ex);
      }
    }

    @Override
    public void close()
    throws IOException
    {
      try {
	con.close();
      } catch ( SQLException ex ) {
	throw new IOException(ex.getMessage(),ex);
      }
    }

    public ElementSequence queryAll( String type )
    throws IOException
    {
      return new ResultImpl(con,type);
    }
  }

  public static interface ElementSequence extends Closeable
  {
    public Element next() throws IOException;
  }

  public static class ResultImpl implements ElementSequence
  {
    public PreparedStatement stmt;
    public ResultSet rset;

    public ResultImpl( Connection con, String type )
    throws IOException
    {
      try {
	stmt = con.prepareStatement("select id, name from testtable where type = ? order by id");
	stmt.setString(1,type);
	rset = stmt.executeQuery();
      } catch ( SQLException ex ) {
	throw new IOException(ex.getMessage(),ex);
      }
    }

    @Override
    public Element next()
    throws IOException
    {
      try {
	if ( !rset.next() ) return null;
	Element elem = new Element();
	elem.id = rset.getInt(1);
	elem.name = rset.getString(2);
	return elem;
      } catch ( SQLException ex ) {
	throw new IOException(ex.getMessage(),ex);
      }
    }

    @Override
    public void close()
    throws IOException
    {
      try {
	rset.close();
      } catch ( SQLException ex ) {
	throw new IOException(ex.getMessage(),ex);
      }
      try {
	stmt.close();
      } catch ( SQLException ex ) {
	throw new IOException(ex.getMessage(),ex);
      }
    }
  }

  public static class Element
  {
    public int id;
    public String name;
  }

  // "mylib/backuper/CreateTable.sql" で定義されている create table 文を確認する。
  @Test
  public void testCreateTable()
  throws Exception
  {
    try (
      Connection con = DriverManager.getConnection(DBURL+";create=true");
      Statement stmt = con.createStatement()
    ) {
      HashSet<String> orig = new HashSet<>();
      try ( ResultSet result = stmt.executeQuery("select tablename from sys.systables") ) {
	while ( result.next() ) orig.add(result.getString(1));
      }
      String lines[];
      try ( InputStream in = DerbyTest.class.getClassLoader()
	.getResourceAsStream("mylib/backuper/CreateTable.sql")
      ) {
	byte buf[] = new byte[1024];
	int len;
	ByteArrayOutputStream out = new ByteArrayOutputStream();
	while ( (len = in.read(buf)) > 0 ) out.write(buf,0,len);
	lines = new String(out.toByteArray()).split(";");
      }
      for ( int i = 0; i < lines.length; ++i ) {
	String line = lines[i].trim();
	if ( line.length() == 0 ) continue;
	stmt.executeUpdate(line);
      }
      LinkedList<String> ans = new LinkedList<>();
      try ( ResultSet result = stmt.executeQuery("select tablename from sys.systables") ) {
	while ( result.next() ) {
	  String str = result.getString(1);
	  if ( !orig.remove(str) ) ans.add(str);
	}
      }
      assertEquals(1,ans.size());
      assertEquals("FILE",ans.get(0));
      assertEquals(0,orig.size());
    }
  }
}
