import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Queue;

public class ListenerThreadHandler implements Runnable {

    private Socket clientSocket;
    Queue<String> inputMessages;

    public ListenerThreadHandler(Socket socket, Queue<String> inputMessages) {
        this.clientSocket = socket;
        this.inputMessages = inputMessages;
    }

    @Override
    public void run() {
        try {
            DataInputStream inputStream = new DataInputStream(clientSocket.getInputStream());
            while (true) {
                try {
                    int length = 0;
                    length = inputStream.readInt();
                    if (length > 0) {
                        byte[] line = new byte[length];
                        inputStream.readFully(line);
                        String msg = new String(line);
                        inputMessages.add(msg);
                    }
                } catch (Exception e) {

                }
                Thread.sleep(500);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

