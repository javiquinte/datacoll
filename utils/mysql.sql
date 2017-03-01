DROP TABLE IF EXISTS member;
DROP TABLE IF EXISTS collection;
DROP TABLE IF EXISTS user;

CREATE TABLE user (
        id INT AUTO_INCREMENT,
        mail VARCHAR(50) NOT NULL,
        PRIMARY KEY(id)
) ENGINE=INNODB;

CREATE TABLE collection (
        id INT AUTO_INCREMENT,
        pid VARCHAR(42) DEFAULT NULL,
        owner INT NOT NULL,
        ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(id),
        INDEX collpidind (pid)
) ENGINE=INNODB;

CREATE TABLE member (
        id INT AUTO_INCREMENT,
        cid INT NOT NULL,
        pid VARCHAR(42),
        location VARCHAR(200),
        checksum VARCHAR(50),
        PRIMARY KEY(id),
        UNIQUE (cid, id),
        INDEX mempidind (pid),
        FOREIGN KEY(cid)
          REFERENCES collection(id)
          ON DELETE CASCADE
) ENGINE=INNODB;

INSERT INTO user (id, mail) VALUES(1, 'javier@gfz-potsdam.de');
INSERT INTO collection (pid, owner) VALUES('uuid-collection', 1);
INSERT INTO collection (pid, owner) VALUES('uuid-collection2', 1);
INSERT INTO member (cid, pid, location, checksum) VALUES (1, NULL, 'https://sec24c79.gfz-potsdam.de/eudat/b2http/api/registered/geofonBak/archive/1993/GE/DSB/BHZ.D/GE.DSB..BHZ.D.1993.351', 'sha2:DAqRlSpLE13O4ECLLAGUN0EZMX0bQqUB8JqprCDfFLE=');
INSERT INTO member (cid, pid, location, checksum) VALUES (1, '11708/BFEF7EDB-48E0-11E6-AC84-82F855C2CCB7', NULL, 'sha2:OIDACYr2jLpcwi2Ba0cszKUJOIQB00yAi7+rEAWRBb0=');
INSERT INTO member (cid, pid, location, checksum) VALUES (2, '11708/331FF9D0-48F6-11E6-AC84-82F855C2CCB7', NULL, 'sha2:OIDACYr2jLpcwi2Ba0cszKUJOIQB00yAi7+rEAWRBb0=');
