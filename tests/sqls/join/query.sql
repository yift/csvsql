SELECT *
FROM tests.data.artists AS artist
INNER JOIN tests.data.albums AS album
ON artist.artist_id = album.artist_id;

SELECT *
FROM tests.data.artists AS artist
JOIN tests.data.albums AS album
ON artist.artist_id = album.artist_id;


SELECT *
FROM tests.data.artists AS artist
JOIN tests.data.albums AS album
USING (artist_id);


SELECT *
FROM tests.data.artists AS artist
LEFT JOIN tests.data.albums AS album
ON artist.artist_id = album.artist_id;

SELECT *
FROM tests.data.artists AS artist
LEFT OUTER JOIN tests.data.albums AS album
ON artist.artist_id = album.artist_id;


SELECT *
FROM tests.data.artists AS artist
RIGHT JOIN tests.data.albums AS album
ON artist.artist_id = album.artist_id;


SELECT *
FROM tests.data.artists AS artist
RIGHT OUTER JOIN tests.data.albums AS album
ON artist.artist_id = album.artist_id;

SELECT *
FROM tests.data.artists AS artist
FULL OUTER JOIN tests.data.albums AS album
ON artist.artist_id = album.artist_id;

SELECT *
FROM tests.data.artists AS artist
FULL JOIN tests.data.albums AS album
ON artist.artist_id = album.artist_id;

SELECT *
FROM tests.data.artists AS artist
FULL JOIN tests.data.albums AS album
;
