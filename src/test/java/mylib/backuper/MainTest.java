package mylib.backuper;

import org.junit.Test;
import mylib.backuper.Main.UsageException;

import static org.junit.Assert.*;

public class MainTest
{
  @Test
  public void testParseOption()
  {
    checkErr("Less arguments");

    check("",false,true,true,"arg1",null,null,"arg1");
    check("",false,true,true,"arg1","arg2",null,"arg1","arg2");
    check("",false,true,true,"arg1","arg2","arg3","arg1","arg2","arg3");
    checkErr("Unused argument arg4","arg1","arg2","arg3","arg4");

    check("",true,true,true,"arg1",null,null,"-f","arg1");
    check("",true,true,true,"arg1","arg2",null,"-f","arg1","arg2");
    check("",true,true,true,"arg1","arg2","arg3","-f","arg1","arg2","arg3");
    checkErr("Unused argument arg4","-f","arg1","arg2","arg3","arg4");

    check("-l",false,true,true,"arg1",null,null,"-l","arg1");
    check("-l",false,true,true,"arg1","arg2",null,"-l","arg1","arg2");
    check("-l",false,true,true,"arg1","arg2","arg3","-l","arg1","arg2","arg3");
    checkErr("Unused argument arg4","-l","arg1","arg2","arg3","arg4");
    checkErr("Unknown option : -lf","-lf","arg1");

    check("-s",false,true,true,"arg1",null,null,"-s","arg1");
    check("-s",false,true,true,"arg1","arg2",null,"-s","arg1","arg2");
    check("-s",false,true,true,"arg1","arg2","arg3","-s","arg1","arg2","arg3");
    checkErr("Unused argument arg4","-s","arg1","arg2","arg3","arg4");
    checkErr("Unknown option : -sf","-sf","arg1");
    checkErr("Unknown option : -fs","-fs","arg1");
    check("-s",false,false,true,"arg1",null,null,"-sn","arg1");
    check("-s",false,false,true,"arg1",null,null,"-ns","arg1");

    check("",false,false,true,"arg1",null,null,"-n","arg1");
    check("",false,false,true,"arg1","arg2",null,"-n","arg1","arg2");
    check("",false,false,true,"arg1","arg2","arg3","-n","arg1","arg2","arg3");
    checkErr("Unused argument arg4","-n","arg1","arg2","arg3","arg4");
    check("",true,false,true,"arg1",null,null,"-nf","arg1");
    check("",true,false,true,"arg1",null,null,"-fn","arg1");

    check("",false,true,false,"arg1",null,null,"-d","arg1");
    check("",false,true,false,"arg1","arg2",null,"-d","arg1","arg2");
    check("",false,true,false,"arg1","arg2","arg3","-d","arg1","arg2","arg3");
    checkErr("Unused argument arg4","-d","arg1","arg2","arg3","arg4");
    checkErr("Unknown option : -df","-df","arg1");
    checkErr("Unknown option : -fd","-fd","arg1");

    check("",false,false,false,"arg1",null,null,"-nd","arg1");
    check("",false,false,false,"arg1","arg2",null,"-nd","arg1","arg2");
    check("",false,false,false,"arg1","arg2","arg3","-nd","arg1","arg2","arg3");
    checkErr("Unused argument arg4","-nd","arg1","arg2","arg3","arg4");
    checkErr("Unknown option : -fnd","-fnd","arg1");
    checkErr("Unknown option : -nfd","-nfd","arg1");
    checkErr("Unknown option : -ndf","-ndf","arg1");

    check("",false,false,false,"arg1",null,null,"-dn","arg1");
    check("",false,false,false,"arg1","arg2",null,"-dn","arg1","arg2");
    check("",false,false,false,"arg1","arg2","arg3","-dn","arg1","arg2","arg3");
    checkErr("Unused argument arg4","-dn","arg1","arg2","arg3","arg4");
    checkErr("Unknown option : -fdn","-fdn","arg1");
    checkErr("Unknown option : -dfn","-dfn","arg1");
    checkErr("Unknown option : -dnf","-dnf","arg1");
  }

  public static void check( String option, boolean forceCopy, boolean doPrepare, boolean doExecute,
    String arg1, String arg2, String arg3, String ... argv )
  {
    Main command = new Main();
    command.parseOption(argv,0);
    assertEquals(option,command.option);
    assertEquals(forceCopy,command.forceCopy);
    assertEquals(doPrepare,command.doPrepare);
    assertEquals(doExecute,command.doExecute);
    assertEquals(arg1,command.arg1);
    assertEquals(arg2,command.arg2);
    assertEquals(arg3,command.arg3);
  }

  public static void checkErr( String message, String ... argv )
  {
    try {
      Main command = new Main();
      command.parseOption(argv,0);
      fail("not exception occured");
    } catch ( UsageException ex ) {
      assertEquals(message,ex.getMessage());
    }
  }
}
