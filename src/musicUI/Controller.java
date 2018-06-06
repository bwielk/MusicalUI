package musicUI;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.TableView;

public class Controller {

    @FXML
    private TableView<Artist> artistTable;

    public void listArtists(){
        Task<ObservableList<Artist>> task = new GetAllArtistTask();
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
