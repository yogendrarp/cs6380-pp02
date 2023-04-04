import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;

class ListenerThread implements Runnable {
    NetworkInformation networkInformation;
    ServerSocket serverSocket;
    Socket serverAcceptSocket;
    List<Thread> createdThreads = new ArrayList<>();
    Queue<String> inputMessages;
    ArrayList<ListenerThreadHandler> listenerThreads = new ArrayList<>();

    public ListenerThread(NetworkInformation networkInformation, Queue<String> inputMessages) {
        this.networkInformation = networkInformation;
        this.inputMessages = inputMessages;
    }

    @Override
    public void run() {
        try {
            NodeMetaData nodeMetaData = networkInformation.nodeMetaData;
            serverSocket = new ServerSocket(nodeMetaData.port);
            serverSocket.setReuseAddress(true);
            while (true) {
                serverAcceptSocket = serverSocket.accept();//accept unlimited requests, reuses the address
                ListenerThreadHandler listenerThreadHandler = new ListenerThreadHandler(serverAcceptSocket, inputMessages);
                listenerThreads.add(listenerThreadHandler);
                Thread thread = new Thread(listenerThreadHandler);
                createdThreads.add(thread);
                thread.start();
            }
            //serverSocket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}