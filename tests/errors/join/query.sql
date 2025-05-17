SELECT *
FROM tests.data.artists AS artist
NATURAL JOIN tests.data.albums AS album

---

SELECT *
FROM tests.data.artists AS artist
GLOBAL INNER JOIN tests.data.albums AS album
ON artist.artist_id = album.artist_id;
---
SELECT *
FROM tests.data.artists
    OUTER APPLY
(SELECT artist_id FROM tests.data.albums) R;
---
