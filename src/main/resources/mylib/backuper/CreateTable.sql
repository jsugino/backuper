create table FILE (
    STORAGE  varchar(10) NOT NULL,
    FOLDER   varchar(1000) NOT NULL,
    NAME     varchar(100) NOT NULL,
    TYPE     smallint NOT NULL,
    HASH     char(22),
    LENGTH   integer NOT NULL,
    LASTMOD  bigint NOT NULL,
    PRIMARY KEY(STORAGE,FOLDER,NAME)
);
