import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;

public class ListenerThreadHandler implements Runnable {

    private Socket clientSocket;

    public ListenerThreadHandler(Socket socket) {
        this.clientSocket = socket;
    }

    @Override
    public void run() {
        try {
            DataInputStream inputStream = new DataInputStream(clientSocket.getInputStream());

            try {
                int length = 0;
                length = inputStream.readInt();
                if (length > 0) {
                    byte[] line = new byte[length];
                    inputStream.readFully(line);
                    String msg = new String(line);
                    System.out.println("Msg received is : " + msg);
                }
            } catch (Exception e) {

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
