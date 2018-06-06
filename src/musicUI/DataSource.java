package musicUI;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class DataSource {

    public static final String DB_NAME = "music.db";
    public static final String CONNECTION_STRING = "jdbc:sqlite:C:\\Users\\Blazej W\\IdeaProjects\\MusicDB\\" + DB_NAME;
    private Connection connection;

    public static final String TABLE_ALBUMS = "albums";
    public static final String COLUMNS_ID_ALBUMS = "_id";
    public static final String COLUMNS_NAME_ALBUMS = "name";
    public static final String COLUMNS_ARTIST_ALBUMS = "artist";

    public static final String TABLE_ARTISTS = "artists";
    public static final String COLUMNS_ID_ARTIST = "_id";
    public static final String COLUMNS_ARTIST_NAME = "name";
    public static final int INDEX_ARTIST_ID = 1;
    public static final int INDEX_ARTIST_NAME = 2;

    public static final String TABLE_SONGS = "songs";
    public static final String COLUMNS_ID_SONG = "_id";
    public static final String COLUMNS_TRACK_SONG = "track";
    public static final String COLUMNS_TITLE_SONG = "title";
    public static final String COLUMNS_ALBUM_SONG = "album";

    public static final int ORDER_BY_NONE = 1;
    public static final int ORDER_BY_ASC = 2;
    public static final int ORDER_BY_DESC = 3;

    public static final String QUERY_ARTIST_FOR_SONG_START = "SELECT " + TABLE_ARTISTS + "." + COLUMNS_ARTIST_NAME +
            ", " + TABLE_ALBUMS + "." + COLUMNS_NAME_ALBUMS +
            ", " + TABLE_SONGS + "." + COLUMNS_TRACK_SONG +
            " FROM " + TABLE_SONGS +
            " INNER JOIN " + TABLE_ALBUMS + " ON " + TABLE_SONGS + "." + COLUMNS_ALBUM_SONG + " = " + TABLE_ALBUMS + "." + COLUMNS_ID_ALBUMS +
            " INNER JOIN " + TABLE_ARTISTS  + " ON " + TABLE_ALBUMS + "." + COLUMNS_ARTIST_ALBUMS + " = " + TABLE_ARTISTS + "." + COLUMNS_ID_ARTIST +
            " WHERE " + TABLE_SONGS + "." + COLUMNS_TITLE_SONG + " = \"";

    public static final String TABLE_ARTIST_SONG_VIEW = "artist_list";

    public static final String CREATE_ARTIST_FOR_SONG_VIEW = "CREATE VIEW IF NOT EXISTS " + TABLE_ARTIST_SONG_VIEW + " AS SELECT " + TABLE_ARTISTS +
            "." + COLUMNS_ARTIST_NAME + ", " + TABLE_ALBUMS + "." + COLUMNS_NAME_ALBUMS + " AS " + COLUMNS_ALBUM_SONG + ", " +
            TABLE_SONGS + "." + COLUMNS_TRACK_SONG + ", " + TABLE_SONGS + "." + COLUMNS_TITLE_SONG +
            " FROM " + TABLE_SONGS + " INNER JOIN " + TABLE_ALBUMS + " ON " + TABLE_SONGS + "." + COLUMNS_ALBUM_SONG + " = " +TABLE_ALBUMS + "." + COLUMNS_ID_ALBUMS +
            " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMNS_ARTIST_ALBUMS + " = " + TABLE_ARTISTS + "." + COLUMNS_ID_ARTIST;

    public static final String QUERY_VIEW_SONG_INFO = "SELECT " + COLUMNS_ARTIST_NAME + ", " + COLUMNS_ALBUM_SONG + ", " + COLUMNS_TRACK_SONG + " FROM " +
            TABLE_ARTIST_SONG_VIEW + " WHERE " + COLUMNS_TITLE_SONG + " = \"";

    public static final String QUERY_VIEW_SONG_INFO_PREP = "SELECT " + COLUMNS_ARTIST_NAME + ", " + COLUMNS_ALBUM_SONG + ", " +
            COLUMNS_TRACK_SONG + " FROM " + TABLE_ARTIST_SONG_VIEW + " WHERE " + COLUMNS_TITLE_SONG + " = ?";

    private PreparedStatement querySongInfoView;

    //CONSTANTS AND PREPARED STATEMENTS FOR A TRANSACTION
    public static final String INSERT_ARTIST = "INSERT INTO " + TABLE_ARTISTS + " (" + COLUMNS_ARTIST_NAME + ") VALUES (?)";
    public static final String INSERT_ALBUM = "INSERT INTO " + TABLE_ALBUMS + " (" + COLUMNS_NAME_ALBUMS + ", " + COLUMNS_ARTIST_ALBUMS + ") VALUES (?,?)";
    public static final String INSERT_SONG = "INSERT INTO " + TABLE_SONGS + " (" + COLUMNS_TRACK_SONG + ", " + COLUMNS_TITLE_SONG + ", " + COLUMNS_ALBUM_SONG + ") VALUES (?,?,?)";
    private PreparedStatement insertToArtists;
    private PreparedStatement insertToAlbums;
    private PreparedStatement insertToSongs;

    public static final String QUERY_ARTIST = "SELECT " + COLUMNS_ID_ARTIST  + " FROM " + TABLE_ARTISTS + " WHERE " + COLUMNS_ARTIST_NAME + " = ?";
    public static final String QUERY_ALBUM = "SELECT " + COLUMNS_ID_ALBUMS + " FROM " + TABLE_ALBUMS + " WHERE " + COLUMNS_NAME_ALBUMS + " = ?";
    private PreparedStatement queryArtist;
    private PreparedStatement queryAlbum;
    /////////////////////////////////////////////////////

    public static final String QUERY_ALBUMS_BY_ARTIST_ID = "SELECT * FROM " + TABLE_ALBUMS + " WHERE " + COLUMNS_ARTIST_ALBUMS
            + " =? ORDER BY " + COLUMNS_NAME_ALBUMS + " COLLATE NOCASE";

    private PreparedStatement queryAlbumsByArtistID;

    private static DataSource dsInstance = new DataSource();

    private DataSource(){}

    public static DataSource getDsInstance(){
        return dsInstance;
    }


    public boolean open(){
        try{
            connection = DriverManager.getConnection(CONNECTION_STRING);
            querySongInfoView = connection.prepareStatement(QUERY_VIEW_SONG_INFO_PREP);
            //PREPARED STATEMENTS FOR TRANSACTIONS
            insertToArtists = connection.prepareStatement(INSERT_ARTIST, Statement.RETURN_GENERATED_KEYS);
            insertToAlbums = connection.prepareStatement(INSERT_ALBUM, Statement.RETURN_GENERATED_KEYS);
            insertToSongs = connection.prepareStatement(INSERT_SONG);
            queryArtist = connection.prepareStatement(QUERY_ARTIST);
            queryAlbum = connection.prepareStatement(QUERY_ALBUM);
            queryAlbumsByArtistID = connection.prepareStatement(QUERY_ALBUMS_BY_ARTIST_ID);
            //////////////////////////////////////
            return true;
        }catch(SQLException e){
            e.printStackTrace();
            return false;
        }
    }

    public void close(){
        try{
            if(querySongInfoView != null){
                querySongInfoView.close();
            }
            if(insertToArtists != null){
                insertToArtists.close();
            }
            if(insertToAlbums != null){
                insertToAlbums.close();
            }
            if(insertToSongs != null){
                insertToSongs.close();
            }
            if(queryArtist != null){
                queryArtist.close();
            }
            if(queryAlbum != null){
                queryAlbum.close();
            }
            if(queryAlbumsByArtistID != null){
                queryAlbumsByArtistID.close();
            }
            if(connection != null){
                connection.close();
            }
        }catch(SQLException e){
            e.printStackTrace();
        }
    }

    public List<Artist> queryArtists(int sortOrder){
        StringBuilder startQuery = new StringBuilder("SELECT * FROM ");
        startQuery.append(TABLE_ARTISTS);
        if(sortOrder != ORDER_BY_NONE){
            startQuery.append(" ORDER BY ");
            startQuery.append(COLUMNS_ARTIST_NAME);
            startQuery.append(" COLLATE NOCASE ");
            if(sortOrder == ORDER_BY_DESC){
                startQuery.append(" DESC ");
            }else{
                startQuery.append(" ASC ");
            }
        }
        try(Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(startQuery.toString())){
            List<Artist> artists = new ArrayList<>();
            while(results.next()){
                try{
                    Thread.sleep(20);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
                Artist artist = new Artist();
                artist.setId((results.getInt(INDEX_ARTIST_ID)));
                artist.setName(results.getString(INDEX_ARTIST_NAME));
                artists.add(artist);
            }
            return artists;
        }catch(SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<String> queryAlbumsForArtist(String artistName, int sortOrder) {
        StringBuilder startQuery = new StringBuilder("SELECT ");
        startQuery.append(TABLE_ALBUMS);
        startQuery.append(".");
        startQuery.append(COLUMNS_NAME_ALBUMS);
        startQuery.append(" FROM ");
        startQuery.append(TABLE_ALBUMS);
        startQuery.append(" INNER JOIN ");
        startQuery.append(TABLE_ARTISTS);
        startQuery.append(" ON ");
        startQuery.append(TABLE_ALBUMS);
        startQuery.append(".");
        startQuery.append(COLUMNS_ARTIST_ALBUMS);
        startQuery.append(" = ");
        startQuery.append(TABLE_ARTISTS);
        startQuery.append(".");
        startQuery.append(COLUMNS_ID_ARTIST);
        startQuery.append(" WHERE " + TABLE_ARTISTS + "." + COLUMNS_ARTIST_NAME + " = \"" + artistName + "\"");
        if (sortOrder != ORDER_BY_NONE) {
            startQuery.append(" ORDER BY " + TABLE_ALBUMS + "." + COLUMNS_NAME_ALBUMS + " COLLATE NOCASE ");
            if (sortOrder == ORDER_BY_DESC) {
                startQuery.append("DESC");
            } else {
                startQuery.append("ASC");
            }
        }
        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery(startQuery.toString())) {
            List<String> artistToalbum = new LinkedList<>();
            while (results.next()) {
                artistToalbum.add(results.getString(1));
            }
            return artistToalbum;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Album> queryAlbumsForArtistID(int artistID){
        try{
            queryAlbumsByArtistID.setInt(1, artistID);
            ResultSet results = queryAlbumsByArtistID.executeQuery();
            List<Album> albums = new ArrayList<>();
            while(results.next()){
                Album album = new Album();
                album.setId(results.getInt(1));
                album.setName(results.getString(2));
                album.setArtist_id(artistID);
                albums.add(album);
            }
            return albums;
        }catch(SQLException e){
            e.printStackTrace();
            return null;
        }
    }

    public void querySongsMetadata(){
        String sql = "SELECT * FROM " + TABLE_SONGS;
        try(Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(sql)){

            ResultSetMetaData meta = results.getMetaData();
            int columnns = meta.getColumnCount();
            for(int i=1; i<=columnns; i++){
                System.out.println("Columns %d in the songs table is name %s\n" + i + " => " + meta.getColumnName(i));
            }
        }catch(SQLException e){
            e.printStackTrace();
        }
    }

    public int getCount(String table){
        String sql = "SELECT COUNT(*), MIN(_id), MAX(_id) AS maximum FROM " + table;
        try(Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(sql)){
            int count = results.getInt(1);
            int min = results.getInt(2);
            int max = results.getInt("maximum");
            System.out.format("MINIMAL NUM => %d MAXIMUM NUM => %d\n", min, max);
            return count;
        }catch(SQLException e){
            e.printStackTrace();
            return -1;
        }
    }

    public boolean createViewSongArtists(){
        try(Statement statement = connection.createStatement()){
            statement.execute(CREATE_ARTIST_FOR_SONG_VIEW);
            return true;
        }catch(SQLException e){
            e.printStackTrace();
            return false;
        }
    }

    private int insertArtist(String name) throws SQLException{
       queryArtist.setString(1, name);
       ResultSet results = queryArtist.executeQuery();
       if(results.next()){
           return results.getInt(1);
       }else{
           insertToArtists.setString(1, name);
           int affectedRows = insertToArtists.executeUpdate();
           if(affectedRows != 1){
               throw new SQLException("Couldn't insert the artists");
           }
               ResultSet genKeys = insertToArtists.getGeneratedKeys();
               if(genKeys.next()){
                   return genKeys.getInt(1);
               }else{
                   throw new SQLException("Couldn't get id for the artists");
               }
       }
    }

    private int insertAlbum(String name, int artistId) throws SQLException{
        queryAlbum.setString(1, name);
        ResultSet results = queryAlbum.executeQuery();
        if(results.next()){
            return results.getInt(1);
        }else{
            insertToAlbums.setString(1, name);
            insertToAlbums.setInt(2, artistId);
            int affectedRows = insertToAlbums.executeUpdate();
            if(affectedRows != 1){
                throw new SQLException("Couldn't insert the albums");
            }
                ResultSet genKeys = insertToAlbums.getGeneratedKeys();
                if(genKeys.next()){
                    return genKeys.getInt(1);
                }else{
                    throw new SQLException("Couldn't get id for the albums");
                }
            }
    }

    public void insertSong(String name, String artist, String album, int track){
        try{
            connection.setAutoCommit(false);
            int artistId = insertArtist(artist);
            int albumId = insertAlbum(album, artistId);
            insertToSongs.setInt(1, track);
            insertToSongs.setString(2, name);
            insertToSongs.setInt(3, albumId);
            int affectedRows = insertToSongs.executeUpdate();
            if(affectedRows == 1){
                connection.commit();
                System.out.println("Song inserted");
            }else{
                throw new SQLException("Song not inserted");
            }
        }catch(SQLException e){
            e.printStackTrace();
            try{
                //ROLLBACK
                connection.rollback();
            }catch(SQLException e2){
                e2.printStackTrace();
            }
        }finally{
            try{
                connection.setAutoCommit(true);
            }catch(SQLException e3){
                e3.printStackTrace();
            }
        }
    }
}
