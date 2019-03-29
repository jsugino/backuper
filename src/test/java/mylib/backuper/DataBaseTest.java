package mylib.backuper;

import static mylib.backuper.BackuperTest.checkContents;
import static mylib.backuper.BackuperTest.event;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.nio.file.Paths;
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
      LocalStorage local;
      FtpStorage ftp;

      storage = db.remove("Linux.junsei.SSD");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/home/junsei"),local.rootFolder);
      assertPattern("work/[^/]+/target",storage.ignoreFolderPats.remove());
      assertPattern("(.+/)?#[^/]+#",storage.ignoreFilePats.remove());
      assertPattern("(.+/)?[^/]+~",storage.ignoreFilePats.remove());
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Linux.junsei.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/C/BACKUP/Linux/home/junsei"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Linux.junsei.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/D/Linux/home/junsei"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Linux.junsei.G");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/run/media/junsei/HD-LBU3/Linux/home/junsei"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("blog.comb");
      assertEquals(FtpStorage.class,storage.getClass());
      ftp = (FtpStorage)storage;
      assertEquals("my.host.ne.jp",ftp.hostname);
      assertEquals("www/blog",ftp.rootFolder);
      assertPattern("(.+/)?\\.htaccess",storage.ignoreFilePats.remove());
      assertPattern("(.+/)?\\.htpasswd",storage.ignoreFilePats.remove());
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("blog.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/C/BACKUP/Downloads/5.blog"),local.rootFolder);
      assertPattern("(.+/)?00\\.loader",storage.ignoreFolderPats.remove());
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Users.junsei.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/C/Users/junsei"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Users.junsei.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/D/Users/junsei"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Users.junsei.G");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/run/media/junsei/HD-LBU3/Users/junsei"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Common.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/C/BACKUP/Common"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Common.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/D/Common"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Common.G");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/run/media/junsei/HD-LBU3/Common"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("VMs.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/C/BACKUP/Virtual Machines"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("VMs.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/D/Virtual Machines"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("VMs.G");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/run/media/junsei/HD-LBU3/Virtual Machines"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      assertEquals(0,db.size());
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
      LocalStorage local;
      FtpStorage ftp;

      storage = db.remove("Linux.junsei.SSD");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/home/junsei"),local.rootFolder);
      assertPattern("work/[^/]+/target",storage.ignoreFolderPats.remove());
      assertPattern("(.+/)?#[^/]+#",storage.ignoreFilePats.remove());
      assertPattern("(.+/)?[^/]+~",storage.ignoreFilePats.remove());
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Linux.junsei.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/C/BACKUP/Linux/home/junsei"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Linux.junsei.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/D/Linux/home/junsei"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Linux.junsei.G");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/run/media/junsei/HD-LBU3/Linux/home/junsei"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("blog.comb");
      assertEquals(FtpStorage.class,storage.getClass());
      ftp = (FtpStorage)storage;
      assertEquals("my.host.ne.jp",ftp.hostname);
      assertEquals("www/blog",ftp.rootFolder);
      assertPattern("(.+/)?\\.htaccess",storage.ignoreFilePats.remove());
      assertPattern("(.+/)?\\.htpasswd",storage.ignoreFilePats.remove());
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("blog.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/C/BACKUP/Downloads/5.blog"),local.rootFolder);
      assertPattern("(.+/)?00\\.loader",storage.ignoreFolderPats.remove());
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Users.junsei.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/C/Users/junsei"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Users.junsei.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/D/Users/junsei"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Users.junsei.G");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/run/media/junsei/HD-LBU3/Users/junsei"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Users.history.junsei.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/D/Users.history/junsei"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Users.history.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/D/Users.history"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Users.history.G");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/run/media/junsei/HD-LBU3/Users.history"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("BACKUP.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/C/BACKUP"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Common.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/C/BACKUP/Common"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Common.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/D/Common"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("Common.G");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/run/media/junsei/HD-LBU3/Common"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("VMs.C");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/C/BACKUP/Virtual Machines"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("VMs.D");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/mnt/D/Virtual Machines"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

      storage = db.remove("VMs.G");
      assertEquals(LocalStorage.class,storage.getClass());
      local = (LocalStorage)storage;
      assertEquals(Paths.get("/run/media/junsei/HD-LBU3/Virtual Machines"),local.rootFolder);
      assertEquals(0,storage.ignoreFilePats.size());
      assertEquals(0,storage.ignoreFolderPats.size());

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
	  "(noname)",
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
}
