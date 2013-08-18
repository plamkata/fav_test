CREATE TABLE favourites (
    user_id bigint, 
    song_id bigint, 
    rownum bigint,
    PRIMARY KEY (user_id, song_id, rownum) 
);