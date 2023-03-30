import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class SenderThread implements Runnable {

    NodeMetaData neighbor;
    private Socket socket = null;
    private DataOutputStream outputStream = null;

    public SenderThread(NodeMetaData neighbor) {
        this.neighbor = neighbor;
    }

    @Override
    public void run() {
        try {
            int counter = 1;
            //Wait for the connections
            while (socket == null || counter < 11) {
                try {
                    socket = new Socket(neighbor.hostUrl, neighbor.port);
                    neighbor.isConnected.set(true);
                } catch (IOException e) {
                    System.out.println("Waiting for connection...");
                }
                counter++;
                Thread.sleep(500);
            }
            System.out.println("Connected to " + neighbor.uid);

        } catch (IOException | InterruptedException e) {

        }
    }
}
