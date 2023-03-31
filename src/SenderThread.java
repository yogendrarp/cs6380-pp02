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
            while (socket == null && counter < 30) {
                try {
                    socket = new Socket(neighbor.hostUrl, neighbor.port);
                    neighbor.isConnected.set(true);
                } catch (IOException e) {
                    System.out.println("Waiting for connection...failed to connect to" + neighbor.uid);
                }
                counter++;
                Thread.sleep(3000);
            }
            System.out.println("Connected to " + neighbor.uid);

            outputStream = new DataOutputStream(socket.getOutputStream());

            while (true) {
                System.out.print("");
                try {
                    if (!(neighbor.msgQueue.isEmpty())) {
                        String msg = neighbor.msgQueue.poll();
                        outputStream.writeInt(msg.length());
                        outputStream.writeBytes(msg);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                Thread.sleep(1000);
            }

        } catch (IOException | InterruptedException e) {

        }
    }
}
