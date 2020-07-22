package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.HashMap;
import java.util.HashSet;
import android.content.ContentProvider;

import android.database.Cursor;
import android.net.Uri;

import android.content.Context;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Formatter;

import java.net.Socket;
import android.telephony.TelephonyManager;


import java.io.BufferedReader;
import android.database.MatrixCursor;
import java.security.NoSuchAlgorithmException;

import java.util.concurrent.Semaphore;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.io.PrintWriter;
import java.security.MessageDigest;

import android.os.AsyncTask;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import android.content.ContentValues;
import android.util.Log;


public class SimpleDynamoProvider extends ContentProvider {

	HashMap<String, String> portToPred1Mapping;
	HashMap<String, String> portToPred2Mapping;
	static final String contentProviderURI = "content://edu.buffalo.cse.cse486586.simpledynamo.provider";
	private Semaphore lock1 = new Semaphore(1, true);
	static HashMap<String, Integer> msgTypeHandler;
	HashMap<String, String> portToSucc1Mapping;
	HashMap<String, String> portToSucc2Mapping;
	boolean allRecRetrieved = false;
	MatrixCursor allRecords;
	String masterPort = "11108";
	String runningPort;
	static final int SERVER_PORT = 10000;
	DBHelper helper;
	HashSet<String> deletedRecords = new HashSet<String>();
	HashMap<String, String> portToHashIdMapping;
	private Semaphore lock2 = new Semaphore(1, true);
	static Uri CONTENT_URI;
	String predNode1, predNode2, succNode1, succNode2;
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	MatrixCursor singleRec;
	boolean inDataRecoveryProcess = false;

	enum responseMsgType  {RETRIEVEMSGRESPONSE, RECOVEREDDATA, RELEASELOCK, ACK}

	enum msgReqType {INSERT, RETRIEVEALLRECORDS, RETRIEVEMSG,
		DELETEALLRECORDS, DELETEMSG, RETRIEVEALLRESPONSE, GETRIGHTNODES  }



	@Override
	public boolean onCreate() {
		acquireLock();
		loadPortToPred1Mapping();
		loadPortToPred2Mapping();
		loadPortToSucc1Mapping();
		loadPortToSucc2Mapping();
		loadPortToHashIdMapping();
		msgTypeHandler = new HashMap<String, Integer>();
		loadMsgTypeIdentifier();


		//fetch the port in which the node is running.
		runningPort = fetchRunningPort();
		//creating server socket
		createServerTask();

		// Initially set predecessor and successor nodes.
		predNode1 = portToPred1Mapping.get(runningPort);
		predNode2 = portToPred2Mapping.get(runningPort);
		succNode1 = portToSucc1Mapping.get(runningPort);
		succNode2 = portToSucc2Mapping.get(runningPort);

		//Load DB Helper
		helper = new DBHelper(getContext());
		Cursor tester = helper.retrieveAllMessages();
		if(tester.getCount() > 0)
		{
			inDataRecoveryProcess = true;
			acquireLock2();
			FetchRightNodesFromPred(portToPred1Mapping.get(runningPort));
			acquireLock2();
			FetchRightNodesFromPred(portToPred2Mapping.get(runningPort));
			acquireLock2();
			FetchRightNodesFromSucc(portToSucc1Mapping.get(runningPort));
			acquireLock2();
			inDataRecoveryProcess = false;
			releaseLock2();

		}

		releaseLock();

		return false;
	}

	public void FetchRightNodesFromPred(String port)
	{
		StringBuilder msgForPred = new StringBuilder(msgReqType.GETRIGHTNODES.toString());
		msgForPred.append("@pred");
		invokeClientTask(msgForPred.toString(), port);
	}


	public void FetchRightNodesFromSucc(String port)
	{
		StringBuilder msgForPred = new StringBuilder(msgReqType.GETRIGHTNODES.toString());
		msgForPred.append("@succ");
		msgForPred.append("@" + runningPort);
		invokeClientTask(msgForPred.toString(), port);
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {

		acquireLock();

		if (selection.equals("*") || selection.equals("@")) {
			helper.deleteAllMessages();

			if(selection.equals("*")) {

				// Passing the deleteAll request to Successor.
				StringBuilder msg = new StringBuilder(msgReqType.DELETEALLRECORDS.toString());
				msg.append(runningPort);
				for (String port : portToPred1Mapping.keySet()) {
					acquireLock2();
					invokeClientTask(msg.toString(), port);
				}
			}
		}
		else {

			String msgPort = getAddressOftheKey(selection);

			StringBuilder msg = new StringBuilder(msgReqType.DELETEMSG.toString() + "@");
			msg.append(selection);
			msg.append("@" +runningPort);


			deleteInNodeOrReplica(selection, msg.toString(), msgPort);

			deleteInNodeOrReplica(selection, msg.toString(), portToSucc1Mapping.get(msgPort));

			deleteInNodeOrReplica(selection, msg.toString(), portToSucc2Mapping.get(msgPort));

		}

		acquireLock2();
		releaseLock2();
		releaseLock();

		return 0;
	}

	public void deleteInNodeOrReplica(String selection, String msg, String port)
	{
		if(port.equals(runningPort))
			helper.deleteMessage(selection);

		else {

			acquireLock2();
			invokeClientTask(msg, port);
		}

	}

	@Override
	public String getType(Uri uri) {

		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

		acquireLock();

		String key = values.get("key").toString();
		String value = values.get("value").toString();

		//Log.i("Insert key", key);

		StringBuilder msg = new StringBuilder(msgReqType.INSERT.toString());
		msg.append("@" + key);
		msg.append("@" + value);
		msg.append("@" + runningPort);

		String keyPort = getAddressOftheKey(key);
		//Log.i(key, keyPort);


		insertInNodeOrReplica(msg.toString(), keyPort, key, value);

		insertInNodeOrReplica(msg.toString(), portToSucc1Mapping.get(keyPort), key, value);

		insertInNodeOrReplica(msg.toString(), portToSucc2Mapping.get(keyPort), key, value);

		acquireLock2();
		releaseLock2();

		// Log.i(TAG, "releasing lock at insert");
		releaseLock();

		return uri;
	}

	public void insertInNodeOrReplica(String msg, String port, String key, String value)
	{
		if(port.equals(runningPort))
			helper.insertMessage(key, value);

		else {

			acquireLock2();
			// Log.i(TAG, "Lock2 acquired at insert");
			invokeClientTask(msg, port);

		}
	}


	public void acquireLock()
	{
		try {
			lock1.acquire();
		}
		catch (InterruptedException ex)
		{
			Log.i(TAG, "Acquiring Lock failed");
		}
	}

	public void releaseLock()
	{
		lock1.release();
	}

	public void acquireLock2()
	{
		try {
			lock2.acquire();
		}
		catch (InterruptedException ex)
		{
			Log.i(TAG, "Acquiring Lock failed");
		}
	}

	public void releaseLock2()
	{
		lock2.release();
	}





	public void createServerTask() {
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			serverSocket.setReuseAddress(true);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e("serverCreateException", "Exception occured while creating server socket");
		}
	}

	// Referred PA1 code
	public String fetchRunningPort() {

		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String lineNumber = tel.getLine1Number();
		String portId = lineNumber.substring(tel.getLine1Number().length() - 4);
		return "" + Integer.valueOf(portId) * 2;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// Log.i("retrieveAll query", selection);
		Cursor cursor;
		acquireLock();
		if (selection.equals("*") || selection.equals("@")) {

			// Retrieving all the messages stored on this node.
			cursor = helper.retrieveAllMessages();
			if (selection.equals("@")) {
				cursor.moveToNext();
				releaseLock();
				return cursor;
			}

            /* Passing the request to successor if the node is not
             alone or it is request to fetch all dht records. */

			StringBuilder msg = new StringBuilder(msgReqType.RETRIEVEALLRECORDS.toString());
			msg.append("@" + runningPort);
			allRecords = new MatrixCursor(new String[]{"key", "value"});
			while (cursor.moveToNext()) {
				allRecords.addRow(new String[]{cursor.getString(0), cursor.getString(1)});
			}
			invokeClientTask(msg.toString(), succNode1);

			while (!allRecRetrieved) {
			}

			allRecRetrieved = false;
			releaseLock();
			//Log.i("retrieveAll", "Lock Released at RetrieveAll");
			return allRecords;
		} else {

			// Log.i(TAG, "Lock2 acquired at query1");
			StringBuilder msg = new StringBuilder(msgReqType.RETRIEVEMSG.toString());
			singleRec = new MatrixCursor(new String[]{"key", "value"});
			String port = getAddressOftheKey(selection);
			msg.append("@" + selection);
			msg.append("@" + port);
			msg.append("@" + runningPort);
			// Log.i(selection, port);
			//Log.i("msgToFind", msg.toString());

			if(port.equals(runningPort)) {
				Cursor temp = helper.retrieveMessage(selection);
				while(temp.moveToNext()) {
					singleRec.newRow()
							.add("key", temp.getString(0))
							.add("value", temp.getString(1));
				}
				releaseLock();
				return singleRec;
			}

			else {
				acquireLock2();
				invokeClientTask(msg.toString(), port);
			}


			acquireLock2();
			singleRec.moveToFirst();
			//Log.i("retriveFound  ", selection + " " + singleRec.getString(1));

			releaseLock2();
			if(singleRec.getCount() != 1) {
				acquireLock2();
				invokeClientTask(msg.toString(), portToSucc1Mapping.get(port));
			}


			acquireLock2();
			releaseLock2();
			releaseLock();
			return singleRec;

		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	public void loadMsgTypeIdentifier() {

		msgTypeHandler.put("insert", 0);
		msgTypeHandler.put("retrieveAllRecords", 1);
		msgTypeHandler.put("retrieveMsg", 2);
		msgTypeHandler.put("deleteAllRecords", 3);
		msgTypeHandler.put("deleteMsg", 4);
		msgTypeHandler.put("retrieveAllResponse", 5);
		msgTypeHandler.put("getRightNodes", 6);
	}

	// Given
	private String genHash(String input) {
		try {
			MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
			byte[] sha1Hash = sha1.digest(input.getBytes());
			Formatter formatter = new Formatter();
			for (byte b : sha1Hash) {
				formatter.format("%02x", b);
			}
			return formatter.toString();
		} catch (NoSuchAlgorithmException ex) {
			Log.i("genHash", "NoSuchAlgorithm Exception occured while hashing");
		}
		return null;
	}

	private void loadPortToPred1Mapping()
	{
		portToPred1Mapping = new HashMap<String, String>();
		portToPred1Mapping.put("11124", "11120");
		portToPred1Mapping.put("11112", "11124");
		portToPred1Mapping.put("11108", "11112");
		portToPred1Mapping.put("11116", "11108");
		portToPred1Mapping.put("11120", "11116");
	}



	private void loadPortToPred2Mapping()
	{
		portToPred2Mapping = new HashMap<String, String>();
		portToPred2Mapping.put("11124", "11116");
		portToPred2Mapping.put("11112", "11120");
		portToPred2Mapping.put("11108", "11124");
		portToPred2Mapping.put("11116", "11112");
		portToPred2Mapping.put("11120", "11108");
	}

	private void loadPortToSucc1Mapping()
	{
		portToSucc1Mapping = new HashMap<String, String>();
		portToSucc1Mapping.put("11124", "11112");
		portToSucc1Mapping.put("11112", "11108");
		portToSucc1Mapping.put("11108", "11116");
		portToSucc1Mapping.put("11116", "11120");
		portToSucc1Mapping.put("11120", "11124");
	}

	private void loadPortToSucc2Mapping()
	{
		portToSucc2Mapping = new HashMap<String, String>();
		portToSucc2Mapping.put("11124", "11108");
		portToSucc2Mapping.put("11112", "11116");
		portToSucc2Mapping.put("11108", "11120");
		portToSucc2Mapping.put("11116", "11124");
		portToSucc2Mapping.put("11120", "11112");
	}

	public String getAddressOftheKey(String key)
	{
		for (String port : portToPred1Mapping.keySet())
		{
			boolean inRange = checkRange(key, port);
			if(inRange) {
				return port;
			}

		}
		return masterPort;
	}

	public boolean checkRange(String key, String port) {

		// Check if the message lies in the range of the current node.
		String predNode = portToPred1Mapping.get(port);
		String predHash = portToHashIdMapping.get(predNode);
		String currNodeHash = portToHashIdMapping.get(port);
		String hashedKey = genHash(key);
		if (hashedKey.compareTo(predHash) > 0 && hashedKey.compareTo(currNodeHash) <= 0)
			return true;
		else if (predHash.compareTo(currNodeHash) > 0 && (hashedKey.compareTo(predHash) > 0 || hashedKey.compareTo(currNodeHash) <= 0))
			return true;

		return false;
	}

	private void loadPortToHashIdMapping() {
		portToHashIdMapping = new HashMap<String, String>();
		portToHashIdMapping.put("11108", genHash("5554"));
		portToHashIdMapping.put("11112", genHash("5556"));
		portToHashIdMapping.put("11116", genHash("5558"));
		portToHashIdMapping.put("11120", genHash("5560"));
		portToHashIdMapping.put("11124", genHash("5562"));
	}

	// Referred PA1 Code
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

			BufferedReader br = null;

            /*
             Referred https://developer.android.com/reference/java/net/ServerSocket
             Referred https://developer.android.com/reference/java/io/BufferedReader
             Referred https://developer.android.com/reference/java/io/PrintWriter
             https://developer.android.com/guide/topics/providers/content-provider-creating
             */
			ContentValues newValues = new ContentValues();
			Uri.Builder uriBuilder = new Uri.Builder();
			uriBuilder.scheme("content");
			uriBuilder.authority(contentProviderURI);
			CONTENT_URI = uriBuilder.build();

			try {
				while (true) {
					Socket soc = serverSocket.accept();
					soc.setReuseAddress(true);
					//Log.i("accept", "Server accepted connection");
					br = new BufferedReader(new InputStreamReader(soc.getInputStream()));
					String msgReceived = br.readLine();
					String[] msgInfo = msgReceived.split("@");
					//int msgType = msgTypeHandler.get(msgInfo[0]);
					String responseMsg =responseMsgType.ACK.toString();

					switch (msgReqType.valueOf(msgInfo[0])) {

						case INSERT: {

							// insert

							if(!inDataRecoveryProcess) {
								String keyId = msgInfo[1];
								String data = msgInfo[2];


								// inserting in the current node if the msg lies in the range of this node.
								helper.insertMessage(keyId, data);
							}

							responseMsg = responseMsgType.RELEASELOCK.toString();

							break;
						}
						case RETRIEVEALLRECORDS: {

							String receiverPort = msgInfo[1];
							if (runningPort.equals(receiverPort)) {
								// Request has been passed between all the nodes.
								allRecRetrieved = true;
								Log.i(TAG, "All Records Retrieved");
							} else {


								Cursor resp = helper.retrieveAllMessages();

								StringBuilder sb = new StringBuilder(msgReqType.RETRIEVEALLRESPONSE.toString());
								while (resp.moveToNext()) {
									sb.append("@" + resp.getString(0).trim() + "#" + resp.getString(1).trim());
								}

								// Send the current node data to the requested node.
								invokeClientTask(sb.toString(), receiverPort);

								invokeClientTask(msgReceived, succNode1);

							}


							break;
						}

						case RETRIEVEMSG: {

							while(inDataRecoveryProcess)
							{}
							// retrieveMsg
							String msgId = msgInfo[1];
							// String senderPort = msgInfo[2];
							StringBuilder msgToSend = new StringBuilder();


							Cursor temp = helper.retrieveMessage(msgId);
							//Log.i(TAG, "Msg Found in this node and sent back to sender");
							temp.moveToNext();
							String key = temp.getString(0);
							String value = temp.getString(1);
							msgToSend.append(responseMsgType.RETRIEVEMSGRESPONSE.toString() + "@" + key + "@" + value);
							//temp.close();

							responseMsg = msgToSend.toString();


							break;
						}
						case DELETEALLRECORDS: {
							// deleteAllRecords

							//Log.i(TAG, "delete all records executed");
							helper.deleteAllMessages();

							responseMsg = responseMsgType.RELEASELOCK.toString();


							break;
						}

						case DELETEMSG: {
							//delete single record

							String msg = msgInfo[1];

							if (!deletedRecords.contains(msg)) {
								//Log.i(TAG, "deleting record");
								helper.deleteMessage(msg);
								deletedRecords.add(msg);
							}

							responseMsg = responseMsgType.RELEASELOCK.toString();


							break;

						}

						case RETRIEVEALLRESPONSE: {

							String[] result = msgReceived.split("@");
							//Log.v(TAG, "Inside retrieveAll Response");
							//Log.i("recvAllResp", msgReceived);
							for (int i = 1; i < result.length; i++) {
								String temp = result[i];
								//Log.i(TAG, temp);
								String[] keyValue = temp.split("#");
								String key = keyValue[0];
								String value = keyValue[1];
								allRecords.newRow()
										.add("key", key)
										.add("value", value);
							}

							break;
						}
						case GETRIGHTNODES:{

							StringBuilder sb = new StringBuilder(responseMsgType.RECOVEREDDATA.toString());
							Cursor resp = helper.retrieveAllMessages();

							if(msgInfo[1].equals("pred"))
							{

								while(resp.moveToNext())
								{
									String key = resp.getString(0);
									String value = resp.getString(1);
									boolean inRange = checkRange(key, runningPort);
									if(inRange)
									{
										sb.append("@" + key.trim() + "#" + value.trim());
									}
								}

							}
							else
							{
								while(resp.moveToNext())
								{
									String failedNode = msgInfo[2];
									String key = resp.getString(0);
									String value = resp.getString(1);
									boolean inRange = checkRange(key, failedNode);
									if(inRange)
									{
										sb.append("@" + key.trim() + "#" + value.trim());
									}

								}
							}

							responseMsg = sb.toString();

							break;
						}


					}

					OutputStream op = soc.getOutputStream();
					PrintWriter pw = new PrintWriter(op, true);
					pw.println(responseMsg);

				}
			} catch (Exception ex) {
				Log.e("SERVER-NA", ex.getMessage());
			}

			return null;
		}

		//Referred PA1 code
		protected void onProgressUpdate(String... strings) {
			String msg = strings[0];
			String node = strings[1];
			new ClientTaskOnCreate().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node);
			return;
		}

	}

	public  void invokeClientTask(String msg, String port) {
		//Log.i("requestMsgtt", msg + " " + port);
		new ClientTaskOnCreate().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);
	}

	// Referred PA1 Code
	private class ClientTaskOnCreate extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {

			try {
				//Log.i("requestMsg", msgToSend);
				String remotePort = msgs[1];
				final Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(remotePort));
				String msgToSend = msgs[0];

				socket.setReuseAddress(true);
				socket.setSoTimeout(1500);

				// Referred https://developer.android.com/reference/java/net/Socket
				// Referred https://developer.android.com/reference/java/io/BufferedReader
				// Referred https://developer.android.com/reference/java/io/PrintWriter

				OutputStream op = socket.getOutputStream();
				PrintWriter pw = new PrintWriter(op, true);
				//Log.i("requestMsg", msgToSend);
				pw.println(msgToSend);
				// Log.i("clientmsg","Client sent message to the server");
				BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String response = br.readLine();
				if (response != null) {
					// Log.i(TAG, response);

					switch(responseMsgType.valueOf(response.split("@")[0]))
					{
						case RETRIEVEMSGRESPONSE:{
							//Log.i("retrieveResp", response);
							singleRec.newRow()
									.add("key", response.split("@")[1])
									.add("value", response.split("@")[2]);
							releaseLock2();
							// Log.i(TAG, "Lock2 released for query at client");
							break;

						}

						case RECOVEREDDATA: {

							String[] result = response.split("@");

							for (int i = 1; i < result.length; i++) {
								String temp = result[i];
								String[] keyValue = temp.split("#");
								String key = keyValue[0];
								String value = keyValue[1];
								helper.insertMessage(key, value);
							}

							releaseLock2();
							break;

						}

						case RELEASELOCK:{

							releaseLock2();
							break;

						}

					}

					socket.close();

				} else {

					if (msgToSend.contains("RETRIEVEALLRECORDS"))
						invokeClientTask(msgToSend, portToSucc2Mapping.get(runningPort));
					else
						releaseLock2();
				}

			}


			catch (UnknownHostException ex) {
				releaseLock2();
				Log.e(TAG, "UnknownHostException occured when client tried to connect to the server task");
			}


			catch (IOException ex) {
				releaseLock2();
				Log.e(TAG, "socket IOException occured when client tried to connect to the server task");
			}

			return null;
		}
	}

}