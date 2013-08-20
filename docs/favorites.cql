CREATE TABLE favorites (
    user_id bigint, 
    song_id bigint, 
    PRIMARY KEY (user_id, song_id) 
);

CREATE TABLE user_favs (
    user_id bigint PRIMARY KEY, 
    songs set<bigint>
);

CREATE TABLE song_favs (
    song_id bigint PRIMARY KEY, 
    users set<bigint>
);