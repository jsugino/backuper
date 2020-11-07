package mylib.backuper;

import static mylib.backuper.BackuperTest.checkContents;
import static mylib.backuper.BackuperTest.event;

import static mylib.backuper.DataBase.commonHead;
import static mylib.backuper.DataBase.commonTail;

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

      storage = db.remove("Users.history.junsei.G");
      assertEquals(LocalStorage.class,storage.getClass());
      local = new PublicLocalStorage(storage);
      assertEquals(Paths.get("/run/media/junsei/HD-LBU3/Users.history/junsei"),local.rootFolder);
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
	  "Common.C=/mnt/C/BACKUP/Common",
	  "Common.D=/mnt/D/Common",
	  "Common.G=/run/media/junsei/HD-LBU3/Common",
	  "Linux.junsei.C=/mnt/C/BACKUP/Linux/home/junsei",
	  "Linux.junsei.D=/mnt/D/Linux/home/junsei",
	  "Linux.junsei.G=/run/media/junsei/HD-LBU3/Linux/home/junsei",
	  "Linux.junsei.SSD=/home/junsei",
	  "Users.history.junsei.D=/mnt/D/Users.history/junsei",
	  "Users.history.junsei.G=/run/media/junsei/HD-LBU3/Users.history/junsei",
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
	  "    Users.history.junsei(D->G)",
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
	  "Common.C=/mnt/C/BACKUP/Common",
	  "Common.D=/mnt/D/Common",
	  "Common.G=/run/media/junsei/HD-LBU3/Common",
	  "Linux.junsei.C=/mnt/C/BACKUP/Linux/home/junsei",
	  "Linux.junsei.D=/mnt/D/Linux/home/junsei",
	  "Linux.junsei.G=/run/media/junsei/HD-LBU3/Linux/home/junsei",
	  "Linux.junsei.SSD=/home/junsei",
	  "Users.history.junsei.D=/mnt/D/Users.history/junsei",
	  "Users.history.junsei.G=/run/media/junsei/HD-LBU3/Users.history/junsei",
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
	  "bupd",
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
	  "    Users.history.junsei(D->G)",
	});
    }
  }

  @Test
  public void testTrunc()
  {
    assertEquals(null,commonHead(null,null));
    assertEquals("str111",commonHead("str111",null));
    assertEquals("str222",commonHead(null,"str222"));
    assertEquals("",commonHead("str111",""));
    assertEquals("",commonHead("","str222"));
    assertEquals("str",commonHead("str111","str222"));
    assertEquals("str",commonHead("str","str222"));
    assertEquals("str",commonHead("str111","str"));
    assertEquals("",commonHead("str111","STR222"));

    assertEquals(null,commonTail(null,null));
    assertEquals("111str",commonTail("111str",null));
    assertEquals("222str",commonTail(null,"222str"));
    assertEquals("",commonTail("111str",""));
    assertEquals("",commonTail("","222str"));
    assertEquals("str",commonTail("111str","222str"));
    assertEquals("str",commonTail("str","222str"));
    assertEquals("str",commonTail("111str","str"));
    assertEquals("",commonTail("111str","222STR"));
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

      db.checkConsistency();
      checkContents(db::printStorages,new String[]{
	  "Common.C             (no *.db file) /mnt/C/BACKUP/Common",
	  "Common.D             (no *.db file) /mnt/D/Common",
	  "Common.G             (no *.db file) /run/media/junsei/HD-LBU3/Common",
	  "Linux.junsei.C       (no *.db file) /mnt/C/BACKUP/Linux/home/junsei",
	  "Linux.junsei.D       (no *.db file) /mnt/D/Linux/home/junsei",
	  "Linux.junsei.G       (no *.db file) /run/media/junsei/HD-LBU3/Linux/home/junsei",
	  "Linux.junsei.SSD     (no *.db file) /home/junsei",
	  "Users.history.junsei.D (no *.db file) /mnt/D/Users.history/junsei",
	  "Users.history.junsei.G (no *.db file) /run/media/junsei/HD-LBU3/Users.history/junsei",
	  "Users.junsei.C       (no *.db file) /mnt/C/Users/junsei",
	  "Users.junsei.D       (no *.db file) /mnt/D/Users/junsei",
	  "Users.junsei.G       (no *.db file) /run/media/junsei/HD-LBU3/Users/junsei",
	  "VMs.C                (no *.db file) /mnt/C/BACKUP/Virtual Machines",
	  "VMs.D                (no *.db file) /mnt/D/Virtual Machines",
	  "VMs.G                (no *.db file) /run/media/junsei/HD-LBU3/Virtual Machines",
	  "blog.C               (no *.db file) /mnt/C/BACKUP/Downloads/5.blog",
	  "blog.comb            (no *.db file) ftp://my.host.ne.jp/www/blog",
	});
      String exp1[] = new String[]{
	"                                       C             ",
	"                                       /mnt/C        ",
	"                                                     ",
	"Common               /Common           /BACKUP       ",
	"                                       ORIG          ",
	"Linux.junsei         /home/junsei      /BACKUP/Linux ",
	"                                       ORIG          ",
	"Users.history.junsei /Users.history                  ",
	"                     /junsei",
	"                                                     ",
	"Users.junsei         /Users/junsei     /.            ",
	"                                       ORIG          ",
	"VMs                  /Virtual Machines /BACKUP       ",
	"                                       ORIG          ",
	"blog                 /.                /BACKUP       ",
	"                                       /Downloads",
	"                                       /5.blog",
	"                                       ORIG          ",
      };
      String exp2[] = new String[]{
	"D        G               SSD      comb",
	"/mnt/D   /run/media      /.       ftp://my.host.ne.jp",
	"         /junsei/HD-LBU3          /www/blog",
	"/.       /.",
	"daily    monthly",
	"/Linux   /Linux          /.",
	"daily    monthly",
	"/.       /.",
	"",
	"ORIG     monthly",
	"/.       /.",
	"daily    monthly",
	"/.       /.",
	"daily    monthly",
	"                                  /.",
	"",
	"",
	"                                  bupd",
      };
      String exp[] = new String[exp1.length];
      for ( int i = 0; i < exp.length; ++i ) exp[i] = exp1[i] + exp2[i];
      //Main.printConfig(System.out,db,bk);
      checkContents(out->Main.printConfig(out,db,bk,16),exp);
      db.printDataBaseAsCsv(System.out);
    }
    System.out.println("END!!");
  }
}
