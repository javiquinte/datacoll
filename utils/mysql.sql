DROP TABLE IF EXISTS member;
DROP TABLE IF EXISTS collection;
DROP TABLE IF EXISTS user;
DROP TABLE IF EXISTS datatype;

CREATE TABLE datatype (
        id INT AUTO_INCREMENT,
        name VARCHAR(50) NOT NULL,
        PRIMARY KEY(id),
        UNIQUE KEY datatypename (name)
) ENGINE=INNODB;

CREATE TABLE user (
        id INT AUTO_INCREMENT,
        mail VARCHAR(50) NOT NULL,
        PRIMARY KEY(id),
        UNIQUE KEY usermail (mail)
) ENGINE=INNODB;

CREATE TABLE collection (
        id INT AUTO_INCREMENT NOT NULL,
        pid VARCHAR(42) DEFAULT NULL,
        owner INT NOT NULL,
        restrictedtotype INT NULL,
        ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(id),
        FOREIGN KEY(owner)
          REFERENCES user(id)
          ON DELETE CASCADE,
        FOREIGN KEY(restrictedtotype)
          REFERENCES datatype(id)
          ON DELETE CASCADE,
        UNIQUE KEY collectionpid (pid)
) ENGINE=INNODB;

CREATE TABLE member (
        id INT NOT NULL,
        cid INT NOT NULL,
        pid VARCHAR(42) DEFAULT NULL,
        location VARCHAR(200) DEFAULT NULL,
        checksum VARCHAR(50) DEFAULT NULL,
        datatype INT NULL,
        dateadded TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(cid, id),
        UNIQUE KEY memberpid (pid, cid),
        FOREIGN KEY(datatype)
          REFERENCES datatype(id)
          ON DELETE CASCADE,
        FOREIGN KEY(cid)
          REFERENCES collection(id)
          ON DELETE CASCADE
) ENGINE=INNODB;

INSERT INTO datatype (id, name) VALUES(1, 'miniSEED');
INSERT INTO user (id, mail) VALUES(1, 'javier@gfz-potsdam.de');
INSERT INTO collection (pid, owner) VALUES('uuid-collection', 1);
INSERT INTO collection (pid, owner) VALUES('uuid-collection2', 1);
INSERT INTO member (cid, id, pid, location, checksum, datatype) VALUES (1, 1, NULL, 'https://sec24c79.gfz-potsdam.de/eudat/b2http/api/registered/geofonBak/archive/1993/GE/DSB/BHZ.D/GE.DSB..BHZ.D.1993.351', 'sha2:DAqRlSpLE13O4ECLLAGUN0EZMX0bQqUB8JqprCDfFLE=', 1);
INSERT INTO member (cid, id, pid, location, checksum, datatype) VALUES (1, 2, '11708/BFEF7EDB-48E0-11E6-AC84-82F855C2CCB7', NULL, 'sha2:OIDACYr2jLpcwi2Ba0cszKUJOIQB00yAi7+rEAWRBb0=', 1);
INSERT INTO member (cid, id, pid, location, checksum, datatype) VALUES (2, 1, '11708/331FF9D0-48F6-11E6-AC84-82F855C2CCB7', NULL, 'sha2:OIDACYr2jLpcwi2Ba0cszKUJOIQB00yAi7+rEAWRBb0=', 1);
