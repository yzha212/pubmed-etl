CREATE TABLE summary (
    [uid] int PRIMARY KEY NOT NULL,
    pubdate datetime,
    epubdate datetime,
    source varchar(max),
    authors nvarchar(max),
    lastauthor varchar(max),
    title varchar(max),
    sorttitle varchar(max),
    volume varchar(max),
    issue varchar(max),
    pages varchar(max),
    lang varchar(max),
    nlmuniqueid varchar(max),
    issn varchar(max),
    essn varchar(max),
    pubtype varchar(max),
    recordstatus varchar(max),
    pubstatus varchar(max),
    articleids nvarchar(max),
    history varchar(max),
    [references] varchar(max),
    attributes varchar(max),
    pmcrefcount varchar(max),
    fulljournalname varchar(max),
    elocationid varchar(max),
    doctype varchar(max),
    srccontriblist varchar(max),
    booktitle varchar(max),
    medium varchar(max),
    [edition] varchar(max),
    publisherlocation varchar(max),
    publishername varchar(max),
    srcdate datetime,
    reportnumber varchar(max),
    availablefromurl varchar(max),
    locationlabel varchar(max),
    doccontriblist varchar(max),
    docdate datetime,
    bookname varchar(max),
    chapter varchar(max),
    sortpubdate datetime,
    sortfirstauthor varchar(max),
    vernaculartitle varchar(max)
);