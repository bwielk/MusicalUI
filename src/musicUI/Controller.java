package musicUI;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.TableView;

public class Controller {

    @FXML
    private TableView artistTable;

    public void listArtists(){
        Task<ObservableList<Artist>> task = new GetAllArtistTask();
        artistTable.itemsProperty().bind(task.valueProperty());
        new Thread(task).start();
    }

    public void listAlbumsForArtist(){
        final Artist artist = (Artist) artistTable.getSelectionModel().getSelectedItem();
        if(artist == null){
            System.out.println("NO ARTIST SELECTED");
            return;
        }
        Task<ObservableList<Album>> task = new Task<ObservableList<Album>>() {
            @Override
            protected ObservableList<Album> call() throws Exception {
                return FXCollections.observableArrayList(DataSource.getDsInstance().queryAlbumsForArtistID(artist.getId()));
            }
        };

        artistTable.itemsProperty().bind(task.valueProperty());
        new Thread(task).start();
    }
}

class GetAllArtistTask extends Task {

    @Override
    public ObservableList<Artist> call(){
        return FXCollections.observableArrayList(DataSource.getDsInstance().queryArtists(DataSource.ORDER_BY_ASC));
    }
}
