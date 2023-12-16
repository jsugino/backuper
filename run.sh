#!/bin/sh

ARGS="-d /mnt/C/BACKUP/BackupTest linux.src linux.dst"

# java -jar $HOME/.m2/repository/mylib/backuper/0.0.1/backuper-0.0.1-jar-with-dependencies.jar $ARGS
java -classpath target/classes mylib.backuper.Backuper $ARGS
