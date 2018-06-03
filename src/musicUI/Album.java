package musicUI;

public class Album {

    private int id;
    private String name;
    private int artist_id;

    public void setName(String name) {
        this.name = name;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setArtist_id(int artist_id) {
        this.artist_id = artist_id;
    }

    public String getName() {
        return name;
    }

    public int getId() {
        return id;
    }

    public int getArtist_id() {
        return artist_id;
    }
}
