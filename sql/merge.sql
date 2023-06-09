MERGE [summary] AS s
USING [staging] AS t
ON (s.[uid] = t.[uid])
WHEN MATCHED THEN
    UPDATE SET 
        [uid] = t.[uid],
        pubdate = t.pubdate,
        epubdate = t.epubdate,
        source = t.source,
        authors = t.authors,
        lastauthor = t.lastauthor,
        title = t.title,
        sorttitle = t.sorttitle,
        volume = t.volume,
        issue = t.issue,
        pages = t.pages,
        lang = t.lang,
        nlmuniqueid = t.nlmuniqueid,
        issn = t.issn,
        essn = t.essn,
        pubtype = t.pubtype,
        recordstatus = t.recordstatus,
        pubstatus = t.pubstatus,
        articleids = t.articleids,
        history = t.history,
        [references] = t.[references],
        attributes = t.attributes,
        pmcrefcount = t.pmcrefcount,
        fulljournalname = t.fulljournalname,
        elocationid = t.elocationid,
        doctype = t.doctype,
        srccontriblist = t.srccontriblist,
        booktitle = t.booktitle,
        medium = t.medium,
        [edition] = t.[edition],
        publisherlocation = t.publisherlocation,
        publishername = t.publishername,
        srcdate = t.srcdate,
        reportnumber = t.reportnumber,
        availablefromurl = t.availablefromurl,
        locationlabel = t.locationlabel,
        doccontriblist = t.doccontriblist,
        docdate = t.docdate,
        bookname = t.bookname,
        chapter = t.chapter,
        sortpubdate = t.sortpubdate,
        sortfirstauthor = t.sortfirstauthor,
        vernaculartitle = t.vernaculartitle
WHEN NOT MATCHED THEN
    INSERT
        (
            [uid],
            pubdate,
            epubdate,
            source,
            authors,
            lastauthor,
            title,
            sorttitle,
            volume,
            issue,
            [pages],
            lang,
            nlmuniqueid,
            issn,
            essn,
            pubtype,
            recordstatus,
            pubstatus,
            articleids,
            history,
            [references],
            attributes,
            pmcrefcount,
            fulljournalname,
            elocationid,
            doctype,
            srccontriblist,
            booktitle,
            medium,
            [edition],
            publisherlocation,
            publishername,
            srcdate,
            reportnumber,
            availablefromurl,
            locationlabel,
            doccontriblist,
            docdate,
            bookname,
            chapter,
            sortpubdate,
            sortfirstauthor,
            vernaculartitle
        )
        VALUES
        (
            t.[uid],
            t.pubdate,
            t.epubdate,
            t.source,
            t.authors,
            t.lastauthor,
            t.title,
            t.sorttitle,
            t.volume,
            t.issue,
            t.[pages],
            t.lang,
            t.nlmuniqueid,
            t.issn,
            t.essn,
            t.pubtype,
            t.recordstatus,
            t.pubstatus,
            t.articleids,
            t.history,
            t.[references],
            t.attributes,
            t.pmcrefcount,
            t.fulljournalname,
            t.elocationid,
            t.doctype,
            t.srccontriblist,
            t.booktitle,
            t.medium,
            t.[edition],
            t.publisherlocation,
            t.publishername,
            t.srcdate,
            t.reportnumber,
            t.availablefromurl,
            t.locationlabel,
            t.doccontriblist,
            t.docdate,
            t.bookname,
            t.chapter,
            t.sortpubdate,
            t.sortfirstauthor,
            t.vernaculartitle
        );