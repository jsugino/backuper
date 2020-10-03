package mylib.backuper;

import org.junit.Test;
import mylib.backuper.Main.UsageException;

import static org.junit.Assert.*;

public class MainExTest
{
  @Test
  public void testParseOption()
  {
    checkErr("Less arguments");

    check("",false,"arg1",null,null,"arg1");
    check("",false,"arg1","arg2",null,"arg1","arg2");
    check("",false,"arg1","arg2","arg3","arg1","arg2","arg3");
    checkErr("Unused argument arg4","arg1","arg2","arg3","arg4");

    check("",true,"arg1",null,null,"-f","arg1");
    check("",true,"arg1","arg2",null,"-f","arg1","arg2");
    check("",true,"arg1","arg2","arg3","-f","arg1","arg2","arg3");
    checkErr("Unused argument arg4","-f","arg1","arg2","arg3","arg4");

    check("-l",false,"arg1",null,null,"-l","arg1");
    check("-l",false,"arg1","arg2",null,"-l","arg1","arg2");
    check("-l",false,"arg1","arg2","arg3","-l","arg1","arg2","arg3");
    checkErr("Unused argument arg4","-l","arg1","arg2","arg3","arg4");

    check("-s",false,"arg1",null,null,"-s","arg1");
    check("-s",false,"arg1","arg2",null,"-s","arg1","arg2");
    check("-s",false,"arg1","arg2","arg3","-s","arg1","arg2","arg3");
    checkErr("Unused argument arg4","-s","arg1","arg2","arg3","arg4");

    check("-S",false,"arg1",null,null,"-S","arg1");
    check("-S",false,"arg1","arg2",null,"-S","arg1","arg2");
    check("-S",false,"arg1","arg2","arg3","-S","arg1","arg2","arg3");
    checkErr("Unused argument arg4","-S","arg1","arg2","arg3","arg4");

    check("-n",false,"arg1",null,null,"-n","arg1");
    check("-n",false,"arg1","arg2",null,"-n","arg1","arg2");
    check("-n",false,"arg1","arg2","arg3","-n","arg1","arg2","arg3");
    checkErr("Unused argument arg4","-n","arg1","arg2","arg3","arg4");

    check("-d",false,"arg1",null,null,"-d","arg1");
    check("-d",false,"arg1","arg2",null,"-d","arg1","arg2");
    check("-d",false,"arg1","arg2","arg3","-d","arg1","arg2","arg3");
    checkErr("Unused argument arg4","-d","arg1","arg2","arg3","arg4");

    check("-x",true,"arg1",null,null,"-f","-x","arg1");
    check("-x",true,"arg1","arg2",null,"-f","-x","arg1","arg2");
    check("-x",true,"arg1","arg2","arg3","-f","-x","arg1","arg2","arg3");
    checkErr("Unused argument arg4","-f","-x","arg1","arg2","arg3","arg4");

    check("-x",true,"arg1",null,null,"-fx","arg1");
    check("-x",true,"arg1","arg2",null,"-fx","arg1","arg2");
    check("-x",true,"arg1","arg2","arg3","-fx","arg1","arg2","arg3");
    checkErr("Unused argument arg4","-fx","arg1","arg2","arg3","arg4");

    check("-x",true,"arg1",null,null,"-xf","arg1");
    check("-x",true,"arg1","arg2",null,"-xf","arg1","arg2");
    check("-x",true,"arg1","arg2","arg3","-xf","arg1","arg2","arg3");
    checkErr("Unused argument arg4","-fx","arg1","arg2","arg3","arg4");
  }

  public static void check( String option, boolean forceCopy, String arg1, String arg2, String arg3, String ... argv )
  {
    MainEx command = new MainEx();
    command.parseOption(argv,0);
    assertEquals(option,command.option);
    assertEquals(forceCopy,command.forceCopy);
    assertEquals(arg1,command.arg1);
    assertEquals(arg2,command.arg2);
    assertEquals(arg3,command.arg3);
  }

  public static void checkErr( String message, String ... argv )
  {
    try {
      MainEx command = new MainEx();
      command.parseOption(argv,0);
      fail("not exception occured");
    } catch ( UsageException ex ) {
      assertEquals(message,ex.getMessage());
    }
  }
}
