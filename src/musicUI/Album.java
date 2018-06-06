package musicUI;

import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleStringProperty;

public class Album {

    private SimpleIntegerProperty id;
    private SimpleStringProperty name;
    private SimpleIntegerProperty artist_id;

    public Album(){
        this.id = new SimpleIntegerProperty();
        this.name = new SimpleStringProperty();
        this.artist_id = new SimpleIntegerProperty();
    }

    public void setName(String name) {
        this.name.set(name);
    }

    public void setId(int id) {
        this.id.set(id);
    }

    public void setArtist_id(int artist_id) {
        this.artist_id.set(artist_id);
    }

    public String getName() {
        return name.get();
    }

    public int getId() {
        return id.get();
    }

    public int getArtist_id() {
        return artist_id.get();
    }
}
