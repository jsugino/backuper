package mylib.backuper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
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
