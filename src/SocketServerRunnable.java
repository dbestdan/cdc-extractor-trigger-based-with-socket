import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketServerRunnable implements Runnable {

	private ServerSocket SocketServer;
	private Socket socket;
	private DataOutputStream dataOutputStream;
	private DataInputStream dataInputStream;
	private int socketPortNumber = 0;
	private long sessionEndTime = 0L;
	private long freshness = 0L;

	public SocketServerRunnable(long sessionEndTime) {
		this.sessionEndTime = sessionEndTime;
		socketPortNumber = Integer.parseInt(System.getProperty("socketPortNumber"));
		try {
			SocketServer = new ServerSocket(socketPortNumber);
			socket = SocketServer.accept();
			dataInputStream = new DataInputStream(socket.getInputStream());
			dataOutputStream = new DataOutputStream(socket.getOutputStream());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void run() {
		while (System.currentTimeMillis() > sessionEndTime) {
			try {

				synchronized (CoordinatorRunnable.freshness) {
					while (freshness >= CoordinatorRunnable.freshness.getTime()) {
						CoordinatorRunnable.freshness.wait();
					}
					freshness = CoordinatorRunnable.freshness.getTime();
					dataOutputStream.writeLong(freshness);
				}
				

			} catch (InterruptedException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

}
