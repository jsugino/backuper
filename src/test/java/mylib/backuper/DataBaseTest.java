package mylib.backuper;

import static mylib.backuper.BackuperTest.checkContents;
import static mylib.backuper.BackuperTest.event;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import mylib.backuper.DataBase.Storage;

public class DataBaseTest
{
  @Rule
  public TemporaryFolder tempdir = new TemporaryFolder(new File("target"));

  @BeforeClass public static void initLogger() { BackuperTest.initLogger(); }
  @Before public void initEvent() { event.list.clear(); }

  @Test
  public void testOldType()
  throws Exception
  {
    try ( DataBase db = new DataBase(tempdir.getRoot().toPath()) ) {
      db.initializeByFile(Paths.get(DataBase.class.getClassLoader()
	  .getResource("mylib/backuper/folders.conf").getPath()));

      DataBase.Storage storage;
      PublicLocalStorage local;
      PublicFtpStorage ftp;

      storage = db.remove("Linux.junsei.SSD");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/home/junsei"),local.rootFolder);
      assertPattern("work/[^/]+/target",local.ignoreFolderPats.remove());
      assertPattern("(.+/)?#[^/]+#",local.ignoreFilePats.remove());
      assertPattern("(.+/)?[^/]+~",local.ignoreFilePats.remove());
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Linux.junsei.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/C/BACKUP/Linux/home/junsei"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Linux.junsei.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/D/Linux/home/junsei"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Linux.junsei.G");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/run/media/junsei/HD-LBU3/Linux/home/junsei"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("blog.comb");
      assertEquals(FtpStorage.class,storage.getClass());
      ftp = new PublicFtpStorage(storage);
      assertEquals("my.host.ne.jp",ftp.hostname);
      assertEquals("www/blog",ftp.rootFolder);
      assertPattern("(.+/)?\\.htaccess",ftp.ignoreFilePats.remove());
      assertPattern("(.+/)?\\.htpasswd",ftp.ignoreFilePats.remove());
      assertEquals(0,ftp.ignoreFilePats.size());
      assertEquals(0,ftp.ignoreFolderPats.size());

      storage = db.remove("blog.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/C/BACKUP/Downloads/5.blog"),local.rootFolder);
      assertPattern("(.+/)?00\\.loader",local.ignoreFolderPats.remove());
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Users.junsei.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/C/Users/junsei"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Users.junsei.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/D/Users/junsei"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Users.junsei.G");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/run/media/junsei/HD-LBU3/Users/junsei"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Common.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/C/BACKUP/Common"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Common.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/D/Common"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Common.G");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/run/media/junsei/HD-LBU3/Common"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("VMs.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/C/BACKUP/Virtual Machines"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("VMs.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/D/Virtual Machines"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("VMs.G");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/run/media/junsei/HD-LBU3/Virtual Machines"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      assertEquals(0,db.size());
    }
  }

  public static class PublicLocalStorage
  {
    public Path rootFolder;
    public LinkedList<Pattern> ignoreFolderPats;
    public LinkedList<Pattern> ignoreFilePats;

    public PublicLocalStorage( Storage orig )
    throws NoSuchFieldException, IllegalAccessException
    {
      setupFields(this,orig);
    }
  }

  public static class PublicFtpStorage
  {
    //public FTPClient ftpclient;
    public String hostname;
    public String rootFolder;
    public String userid;
    public String password;
    public LinkedList<Pattern> ignoreFolderPats;
    public LinkedList<Pattern> ignoreFilePats;

    public PublicFtpStorage( Storage orig )
    throws NoSuchFieldException, IllegalAccessException
    {
      setupFields(this,orig);
    }
  }

  public static void setupFields( Object instance, Object original )
  throws NoSuchFieldException, IllegalAccessException
  {
    Field fields[] = instance.getClass().getDeclaredFields();
    for ( int i = 0; i < fields.length; ++i ) {
      String fieldname = fields[i].getName();
      NoSuchFieldException first = null;
      Field field = null;
      for ( Class<? extends Object> cls = original.getClass(); cls != null; cls = cls.getSuperclass() ) {
	try {
	  field = cls.getDeclaredField(fieldname);
	  break;
	} catch ( NoSuchFieldException ex ) {
	  if ( first == null ) first = ex;
	}
      }
      if ( field == null ) throw first == null ? new NoSuchFieldException(fieldname) : first;
      field.setAccessible(true);
      fields[i].set(instance,field.get(original));
    }
  }

  @Test
  public void testNewType()
  throws Exception
  {
    try ( DataBase db = new DataBase(tempdir.getRoot().toPath()) ) {
      db.initializeByXml(Paths.get(DataBase.class.getClassLoader()
	  .getResource("mylib/backuper/folders.conf.xml").getPath()));

      DataBase.Storage storage;
      PublicLocalStorage local;
      PublicFtpStorage ftp;

      storage = db.remove("Linux.junsei.SSD");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/home/junsei"),local.rootFolder);
      assertPattern("work/[^/]+/target",local.ignoreFolderPats.remove());
      assertPattern("(.+/)?#[^/]+#",local.ignoreFilePats.remove());
      assertPattern("(.+/)?[^/]+~",local.ignoreFilePats.remove());
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Linux.junsei.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/C/BACKUP/Linux/home/junsei"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Linux.junsei.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/D/Linux/home/junsei"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Linux.junsei.G");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/run/media/junsei/HD-LBU3/Linux/home/junsei"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("blog.comb");
      assertEquals(FtpStorage.class,storage.getClass());
      ftp = new PublicFtpStorage(storage);
      assertEquals("my.host.ne.jp",ftp.hostname);
      assertEquals("www/blog",ftp.rootFolder);
      assertPattern("(.+/)?\\.htaccess",ftp.ignoreFilePats.remove());
      assertPattern("(.+/)?\\.htpasswd",ftp.ignoreFilePats.remove());
      assertEquals(0,ftp.ignoreFilePats.size());
      assertEquals(0,ftp.ignoreFolderPats.size());

      storage = db.remove("blog.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/C/BACKUP/Downloads/5.blog"),local.rootFolder);
      assertPattern("(.+/)?00\\.loader",local.ignoreFolderPats.remove());
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Users.junsei.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/C/Users/junsei"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Users.junsei.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/D/Users/junsei"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Users.junsei.G");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/run/media/junsei/HD-LBU3/Users/junsei"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Users.history.junsei.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/D/Users.history/junsei"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Users.history.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/D/Users.history"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Users.history.G");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/run/media/junsei/HD-LBU3/Users.history"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("BACKUP.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/C/BACKUP"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Common.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/C/BACKUP/Common"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Common.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/D/Common"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("Common.G");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/run/media/junsei/HD-LBU3/Common"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("VMs.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/C/BACKUP/Virtual Machines"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("VMs.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/mnt/D/Virtual Machines"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      storage = db.remove("VMs.G");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/run/media/junsei/HD-LBU3/Virtual Machines"),local.rootFolder);
      assertEquals(0,local.ignoreFilePats.size());
      assertEquals(0,local.ignoreFolderPats.size());

      assertEquals(db.toString(),0,db.size());
    }
  }

  public static void assertPattern( String exp, Pattern actual )
  {
    assertEquals(Pattern.compile(exp).toString(),actual.toString());
  }

  @Test
  public void testList()
  throws Exception
  {
    try ( DataBase db = new DataBase(tempdir.getRoot().toPath()) ) {
      String def = DataBase.class
	.getClassLoader()
	.getResource("mylib/backuper/folders.conf.xml")
	.getPath();
      Backup bk = db.initializeByXml(Paths.get(def));
      checkContents(
	Main.listDB(db).stream().map(Storage::toString).collect(Collectors.toList()),
	new String[]{
	  "BACKUP.C=/mnt/C/BACKUP",
	  "Common.C=/mnt/C/BACKUP/Common",
	  "Common.D=/mnt/D/Common",
	  "Common.G=/run/media/junsei/HD-LBU3/Common",
	  "Linux.junsei.C=/mnt/C/BACKUP/Linux/home/junsei",
	  "Linux.junsei.D=/mnt/D/Linux/home/junsei",
	  "Linux.junsei.G=/run/media/junsei/HD-LBU3/Linux/home/junsei",
	  "Linux.junsei.SSD=/home/junsei",
	  "Users.history.D=/mnt/D/Users.history",
	  "Users.history.G=/run/media/junsei/HD-LBU3/Users.history",
	  "Users.history.junsei.D=/mnt/D/Users.history/junsei",
	  "Users.junsei.C=/mnt/C/Users/junsei",
	  "Users.junsei.D=/mnt/D/Users/junsei",
	  "Users.junsei.G=/run/media/junsei/HD-LBU3/Users/junsei",
	  "VMs.C=/mnt/C/BACKUP/Virtual Machines",
	  "VMs.D=/mnt/D/Virtual Machines",
	  "VMs.G=/run/media/junsei/HD-LBU3/Virtual Machines",
	  "blog.C=/mnt/C/BACKUP/Downloads/5.blog",
	  "blog.comb=ftp://my.host.ne.jp/www/blog",
	});
      checkContents(bk::dump,new String[]{
	  "(non)",
	  "    blog(C->comb)",
	  "daily",
	  "    Common(C->D)",
	  "    VMs(C->D)",
	  "    Linux.junsei(SSD->C,D)",
	  "    Users.junsei(C->D(Users.history.junsei.D))",
	  "monthly",
	  "    Common(C->G)",
	  "    VMs(C->G)",
	  "    Linux.junsei(SSD->G)",
	  "    Users.junsei(C->G)",
	  "    Users.history(D->G)",
	});
    }
  }

  @Test
  public void testBackupRef()
  throws Exception
  {
    try ( DataBase db = new DataBase(tempdir.getRoot().toPath()) ) {
      String def = DataBase.class
	.getClassLoader()
	.getResource("mylib/backuper/folders-2.conf.xml")
	.getPath();
      Backup bk = db.initializeByXml(Paths.get(def));
      checkContents(
	Main.listDB(db).stream().map(Storage::toString).collect(Collectors.toList()),
	new String[]{
	  "BACKUP.C=/mnt/C/BACKUP",
	  "Common.C=/mnt/C/BACKUP/Common",
	  "Common.D=/mnt/D/Common",
	  "Common.G=/run/media/junsei/HD-LBU3/Common",
	  "Linux.junsei.C=/mnt/C/BACKUP/Linux/home/junsei",
	  "Linux.junsei.D=/mnt/D/Linux/home/junsei",
	  "Linux.junsei.G=/run/media/junsei/HD-LBU3/Linux/home/junsei",
	  "Linux.junsei.SSD=/home/junsei",
	  "Users.history.D=/mnt/D/Users.history",
	  "Users.history.G=/run/media/junsei/HD-LBU3/Users.history",
	  "Users.history.junsei.D=/mnt/D/Users.history/junsei",
	  "Users.junsei.C=/mnt/C/Users/junsei",
	  "Users.junsei.D=/mnt/D/Users/junsei",
	  "Users.junsei.G=/run/media/junsei/HD-LBU3/Users/junsei",
	  "VMs.C=/mnt/C/BACKUP/Virtual Machines",
	  "VMs.D=/mnt/D/Virtual Machines",
	  "VMs.G=/run/media/junsei/HD-LBU3/Virtual Machines",
	  "blog.C=/mnt/C/BACKUP/Downloads/5.blog",
	  "blog.comb=ftp://my.host.ne.jp/www/blog",
	});
      checkContents(bk::dump,new String[]{
	  "(non)",
	  "    blog(C->comb)",
	  "daily",
	  "    Linux.junsei(C->D)",
	  "    Common(C->D)",
	  "    VMs(C->D)",
	  "    Users.junsei(C->D(Users.history.junsei.D))",
	  "monthly",
	  "    Linux.junsei(C->G)",
	  "    Common(C->G)",
	  "    VMs(C->G)",
	  "    Users.junsei(C->G)",
	  "    Users.history(D->G)",
	});
    }
  }

  @Test
  public void dumpBackup()
  throws Exception
  {
    System.out.println("START!!");
    try ( DataBase db = new DataBase(tempdir.getRoot().toPath()) ) {
      String def = DataBase.class
	.getClassLoader()
	.getResource("mylib/backuper/folders-2.conf.xml")
	.getPath();
      Backup bk = db.initializeByXml(Paths.get(def));
      DoubleKeyHashMap<String,String,Storage> map = new DoubleKeyHashMap<>();
      for ( Map.Entry<String,Storage> ent : db.entrySet() ) {
	String key = ent.getKey();
	Storage val = ent.getValue();
	assertEquals(key,val.storageName);
	System.out.println("key = "+key+", name = "+val.storageName+", root = "+val.getRoot());
	int idx = key.lastIndexOf('.');
	String key1 = key.substring(0,idx);
	String key2 = key.substring(idx+1);
	map.put(key1,key2,val);
      }
      String min[] = new String[map.key2Set().size()];
      int cnt = 0;
      for ( String key2 : map.key2Set() ) {
	min[cnt] = null;
	for ( String key1 : map.key1Set() ) {
	  Storage val = map.get(key1,key2);
	  if ( val == null ) continue;
	  String root = val.getRoot();
	  if ( min[cnt] == null ) {
	    min[cnt] = root;
	    continue;
	  }
	  int len = Math.min(min[cnt].length(),root.length());
	  for ( int i = 0; i < len; ++i ) {
	    if ( min[cnt].charAt(i) != root.charAt(i) ) {
	      min[cnt] = min[cnt].substring(0,i);
	      break;
	    }
	  }
	}
	++cnt;
      }
      for ( String key2 : map.key2Set() ) {
	System.out.print(",\t");
	System.out.print(key2);
      }
      System.out.println();
      for ( String com : min ) {
	System.out.print(",\t");
	System.out.print(com);
      }
      System.out.println();
      for ( String key1 : map.key1Set() ) {
	System.out.print(key1);
	for ( String key2 : map.key2Set() ) {
	  Storage val = map.get(key1,key2);
	  System.out.print(",\t");
	  if ( val != null ) System.out.print(val.getRoot());
	}
	System.out.println();
      }
    }
  }
}
