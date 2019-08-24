package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TabHost;
import android.widget.TextView;

import java.io.EOFException;
import java.lang.Math;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
//import java.io.Serializable;
//import java.io.NotSerializableException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Boolean.*;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 */
//References:
// https://www.baeldung.com/java-serialization
// https://www.tutorialspoint.com/java/util/arrays_fill_boolean.htm

public class GroupMessengerActivity extends Activity {

    public boolean [] deadPorts= new boolean[]{FALSE,FALSE,FALSE,FALSE,FALSE};
    float highestDeliveredSequenceNumber = 0;
    private AtomicInteger sequenceNumberForFile=new AtomicInteger(0);
    PriorityBlockingQueue<ClientToServerMessageCommunication> priorityQueueMessages = new PriorityBlockingQueue<ClientToServerMessageCommunication>(100,new SequenceNumberComparator());
    final Uri providerUri = Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger2.provider");
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

//        PriorityQueue<ClientToServerMessageCommunication> priorityQueueMessages = new                              PriorityQueue<ClientToServerMessageCommunication>(100, new SequenceNumberComparator());

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            // This code has been taken from PA1
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.e(TAG, "ServerSocket created" + myPort);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        final Button button = (Button) findViewById(R.id.button4);

        button.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
//                The implementation has been taken from the PA1 code.
                final EditText editText = (EditText) findViewById(R.id.editText1);
                String msg = editText.getText().toString() + "\n";
                editText.setText("");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    public class SequenceNumberComparator implements Comparator<ClientToServerMessageCommunication>{
        @Override
        public int compare(ClientToServerMessageCommunication lhs, ClientToServerMessageCommunication rhs) {
            if(lhs.getSequenceNumber() < rhs.getSequenceNumber()) return -1;
            else if(lhs.getSequenceNumber() > rhs.getSequenceNumber())return 1;
            return 0;
        }
    }
    public class ClientToServerMessageCommunication {
        String msg;
        float sequenceNumber;
        String portNumber;
        Boolean isFinalSequenceNumber;

        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }

        public float getSequenceNumber() {
            return sequenceNumber;
        }

        public void setSequenceNumber(float sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
        }

        public String getPortNumber() {
            return portNumber;
        }

        public void setPortNumber(String portNumber) {
            this.portNumber = portNumber;
        }

        public Boolean getIsFinalSequenceNumber() {
            return isFinalSequenceNumber;
        }

        public void setIsFinalSequenceNumber(Boolean finalSequenceNumber) {
            isFinalSequenceNumber = finalSequenceNumber;
        }

        ClientToServerMessageCommunication(String msg, Boolean isFinalSequenceNumber){
            this.msg = msg;
            this.isFinalSequenceNumber=isFinalSequenceNumber;
        }
        ClientToServerMessageCommunication(String msg, float sequenceNumber, Boolean finalSequenceNumber){
            this.msg=msg;
            this.sequenceNumber=sequenceNumber;
            this.isFinalSequenceNumber = isFinalSequenceNumber;
        }
        ClientToServerMessageCommunication(String msg, String portNumber, float sequenceNumber, Boolean finalSequenceNumber){
            this.msg=msg;
            this.portNumber=portNumber;
            this.sequenceNumber = sequenceNumber;
            this.isFinalSequenceNumber = isFinalSequenceNumber;
        }
    }

    public class Proposal{
        float proposedSeqNumber;
        String msgAck;
        int portNumber;

        public float getProposedSeqNumber() {
            return proposedSeqNumber;
        }

        public void setProposedSeqNumber(float proposedSeqNumber) {
            this.proposedSeqNumber = proposedSeqNumber;
        }

        public String getMsgAck() {
            return msgAck;
        }

        public void setMsgAck(String msgAck) {
            this.msgAck = msgAck;
        }


        public int getPortNumber() {
            return portNumber;
        }

        public void setPortNumber(int portNumber) {
            this.portNumber = portNumber;
        }

//        @Override
//        public int compareTo(Proposal another) {
//            return Math.min(this.getProposedSeqNumber(),another.getProposedSeqNumber());
//        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        final String TAG = ServerTask.class.getSimpleName();

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            int count = 0;
            float finalSequenceNumber=-1;

            try {
                while (true) {
                    ContentResolver cr = getContentResolver();
                    Socket socket = serverSocket.accept();
                    Log.e(TAG,"Connection accepted");
                    socket.setSoTimeout(3000);

                    ObjectInputStream serverObjectInputStream = new ObjectInputStream(socket.getInputStream());
                    ObjectOutputStream serverObjectOutputStream = new ObjectOutputStream(socket.getOutputStream());

                    String msg = (String) serverObjectInputStream.readObject();
                    String senderPort = (String) serverObjectInputStream.readObject();
                    String senderPortOriginal = (String) serverObjectInputStream.readObject();
                    Boolean isFinalSequenceNumber = (Boolean) serverObjectInputStream.readObject();
                    finalSequenceNumber = count;
                    ClientToServerMessageCommunication messageReceived = new ClientToServerMessageCommunication(msg,senderPortOriginal,                                                 finalSequenceNumber,isFinalSequenceNumber);
                    messageReceived.setIsFinalSequenceNumber(isFinalSequenceNumber);
                    Log.e(TAG,"Message Received.");

//                    Overriding the finally agreed sequence number
                    if(isFinalSequenceNumber!=null && isFinalSequenceNumber){
                        finalSequenceNumber = (Float) serverObjectInputStream.readObject();
//                        String originalMyPort = (String) serverObjectInputStream.readObject();
                        ClientToServerMessageCommunication messageFinalized = new ClientToServerMessageCommunication(msg,senderPortOriginal,                                                    finalSequenceNumber,true);

//                        Adding the final sequence number and messages to Queue
// and Sending final acknowledgment
                        if(!priorityQueueMessages.isEmpty()){
                            Iterator i = priorityQueueMessages.iterator();
                            while (i.hasNext()){
                                ClientToServerMessageCommunication obj = (ClientToServerMessageCommunication)i.next();
//                                Log.e(TAG,"Final Sequence number:"+sequenceNumberForFile.get());

                                if(!obj.getIsFinalSequenceNumber() && obj.getMsg().equalsIgnoreCase(messageFinalized.getMsg())
                                        && obj.getPortNumber().equalsIgnoreCase(messageFinalized.getPortNumber())){

                                    Log.e(TAG,"Message in Queue:"+obj.getMsg());
                                    Log.e(TAG,"Sequence number in Queue:"+obj.getSequenceNumber());
                                    Log.e(TAG,"Agreed Sequence number:"+messageFinalized.getSequenceNumber());

                                    Log.e(TAG,"Old Queue Size:"+priorityQueueMessages.size());
                                    Log.e(TAG,"the head of Queue(BEFORE):"+priorityQueueMessages.peek().getSequenceNumber()
                                            +"--"+priorityQueueMessages.peek().getMsg());
                                    priorityQueueMessages.remove(obj);
                                    Log.e(TAG,"Old Sequence number removed from Queue:"+obj.getSequenceNumber()+"--"+obj.getMsg());
                                    Log.e(TAG,"new Queue Size:"+priorityQueueMessages.size());
                                    obj.setSequenceNumber(messageFinalized.getSequenceNumber());
                                    obj.setIsFinalSequenceNumber(true);
//                                    obj.setPortNumber(senderPortOriginal);
                                    priorityQueueMessages.add(obj);
                                    Log.e(TAG,"New Sequence number added to Queue:"+obj.getSequenceNumber()+"--"+obj.getMsg());
                                    Log.e(TAG,"the head of Queue(AFTER):"+priorityQueueMessages.peek().getSequenceNumber()
                                            +"--"+priorityQueueMessages.peek().getMsg());
                                    Log.e(TAG,"newnew Queue Size:"+priorityQueueMessages.size());
                                    Log.e(TAG,"Agreed Sequence number added to Queue:"+obj.getSequenceNumber());
                                }
//
//                                ClientToServerMessageCommunication dummyPeek;
//                                if(!priorityQueueMessages.isEmpty())
//                                    dummyPeek = priorityQueueMessages.peek();

                                if(priorityQueueMessages.peek()!=null && priorityQueueMessages.peek().getIsFinalSequenceNumber()!=null &&
                                        priorityQueueMessages.peek().getIsFinalSequenceNumber()){
                                    ClientToServerMessageCommunication deliveringMessage = priorityQueueMessages.poll();
                                    Log.e(TAG,"Delivering from queue:"+deliveringMessage.getSequenceNumber()+"--"+
                                            deliveringMessage.getMsg());
                                    Log.e(TAG,"Final queue size:"+priorityQueueMessages.size());

                                    highestDeliveredSequenceNumber = deliveringMessage.getSequenceNumber();
                                    Log.e(TAG,"Highest Delivered message:"+highestDeliveredSequenceNumber);
                                    deliverMessage(cr, senderPort, deliveringMessage);
                                }
                                else if(priorityQueueMessages.peek() !=null && priorityQueueMessages.peek().getIsFinalSequenceNumber()!=null &&
                                        !priorityQueueMessages.peek().getIsFinalSequenceNumber()){
                                        Log.e(TAG,"Extracting dead port's message");
                                        ClientToServerMessageCommunication dummyPeek = priorityQueueMessages.peek();
                                        Log.e(TAG,"Potential Faulty process message = "+dummyPeek.getMsg());
                                        Log.e(TAG,"Potential Faulty process port = "+dummyPeek.getPortNumber());
                                        Log.e(TAG,"Potential Faulty process - Deliverable"+dummyPeek.getIsFinalSequenceNumber());

                                        if((dummyPeek.getPortNumber().equalsIgnoreCase(REMOTE_PORT0) && deadPorts[0]) ||
                                                (dummyPeek.getPortNumber().equalsIgnoreCase(REMOTE_PORT1) && deadPorts[1]) ||
                                                (dummyPeek.getPortNumber().equalsIgnoreCase(REMOTE_PORT2) && deadPorts[2]) ||
                                                (dummyPeek.getPortNumber().equalsIgnoreCase(REMOTE_PORT3) && deadPorts[3]) ||
                                                (dummyPeek.getPortNumber().equalsIgnoreCase(REMOTE_PORT4) && deadPorts[4]) )
                                        {
                                            Log.e(TAG,"TRIAL"+dummyPeek.getIsFinalSequenceNumber());
                                              priorityQueueMessages.poll();
                                        }
                                }
                            }
                            serverObjectOutputStream.writeObject("Message with sequence number received");
                        }
                    }

//                    Proposing Sequence number
                    if(isFinalSequenceNumber!=null && !isFinalSequenceNumber){
                        Proposal proposal = new Proposal();

                        int senderPortIntValue = Integer.parseInt(senderPort);
//                        Proposing only if the client is not dead
                        if((senderPortOriginal.equalsIgnoreCase(REMOTE_PORT0) && !deadPorts[0]) ||
                                (senderPortOriginal.equalsIgnoreCase(REMOTE_PORT1) && !deadPorts[1]) ||
                                (senderPortOriginal.equalsIgnoreCase(REMOTE_PORT2) && !deadPorts[2]) ||
                                (senderPortOriginal.equalsIgnoreCase(REMOTE_PORT3) && !deadPorts[3]) ||
                                (senderPortOriginal.equalsIgnoreCase(REMOTE_PORT4) && !deadPorts[4]))
                        {float y = (float) senderPortIntValue;
                        y=y/100000;
//                    Proposing the highest known number+1
                        if(!priorityQueueMessages.isEmpty()){
                            Iterator queueIterator = priorityQueueMessages.iterator();
                            while (queueIterator.hasNext()){
                                ClientToServerMessageCommunication dummy = (ClientToServerMessageCommunication) queueIterator.next();
                                int tempSequence = (int)dummy.getSequenceNumber();
                                if(tempSequence>count) count=tempSequence+1;
                                Log.e(TAG,"Highest number from Queue:"+tempSequence);
                            }
                        }

//                    Proposing the highest known number+1
                        int tempHightest = (int) highestDeliveredSequenceNumber;
                        if(tempHightest>count) count=tempHightest+1;
                        Log.e(TAG,"Highest Delivered message:"+tempHightest);
                        Log.e(TAG,"Count:"+count);

                        float x = count+y;
                        proposal.setProposedSeqNumber(x);
                        proposal.setMsgAck("Ack");
                        serverObjectOutputStream.writeObject(proposal.getProposedSeqNumber());
                        serverObjectOutputStream.writeObject(proposal.getMsgAck());
                        messageReceived.setSequenceNumber(x);
                        Log.e(TAG,"Proposal adding to Queue"+x+messageReceived.getMsg());
                        priorityQueueMessages.add(messageReceived);
                        Log.e(TAG,"Proposal added to priority Queue");
//                    socket.close();
                    }}
                    count++;
                }
            }catch (SocketTimeoutException e) {
                Log.e(TAG, "ServerTask - readObject() SocketTimeoutException");
            }catch (ClassNotFoundException e) {
                Log.e(TAG, "ServerTask - readObject() failed");
            }catch (IOException e) {
                Log.e(TAG, "ServerTask - serverSocket.accept() failed");
            }
            return null;
        }

        private void deliverMessage(ContentResolver cr, String senderPort, ClientToServerMessageCommunication messageFinalized) {
            String key = Integer.toString(sequenceNumberForFile.get());
//                                    String []keys = key.toString().split("\\.");
            ContentValues cv = new ContentValues();
            cv.put("key", key);
            cv.put("value", messageFinalized.getMsg());
            Log.e(TAG,"Calling Insert of Content Provider");
            Uri newUri = cr.insert(
                    providerUri,
                    cv
            );
            Log.e(TAG,"senderPort:"+senderPort);
            Log.e(TAG,"Sequence number:"+key+"Message:"+messageFinalized.getMsg());
            publishProgress(messageFinalized.getSequenceNumber()+"-"+messageFinalized.getMsg());
            sequenceNumberForFile.set(sequenceNumberForFile.incrementAndGet());
            Log.e(TAG, "Removing from Queue:"+messageFinalized.getSequenceNumber()+"--"+messageFinalized.getMsg());
//            priorityQueueMessages.remove();
        }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = "";
            strReceived = strings[0].trim();
            TextView localTextView = (TextView) findViewById(R.id.textView1);
            /*
             * TODO: Use the TextView to display your messages. Though there is no grading component
             * on how you display the messages, if you implement it, it'll make your debugging easier.
             */
//            The received messages are printed on screen.
            localTextView.append("\n" + strReceived);
            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
//            try {
            Boolean isFinalSequenceNumber=false;
            String msg = msgs[0];
            String myport = msgs[1];
            ClientToServerMessageCommunication messagesTOSend = new ClientToServerMessageCommunication(msg,isFinalSequenceNumber);
            float proposedSeqNumber0=0;
            float proposedSeqNumber1=0;
            float proposedSeqNumber2=0;
            float proposedSeqNumber3=0;
            float proposedSeqNumber4=0;

            try{
                Socket socket0 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0));
                ObjectOutputStream objectOutputStream0 = new ObjectOutputStream(socket0.getOutputStream());
                objectOutputStream0.writeObject(messagesTOSend.getMsg());
                objectOutputStream0.writeObject(REMOTE_PORT0);
                Log.e(TAG, "myport"+myport);
                objectOutputStream0.writeObject(myport);
                objectOutputStream0.writeObject(messagesTOSend.getIsFinalSequenceNumber());
                objectOutputStream0.flush();
                ObjectInputStream clientObjectInputStream0 = new ObjectInputStream(socket0.getInputStream());
                proposedSeqNumber0 = (Float) clientObjectInputStream0.readObject();
                socket0.setSoTimeout(3000);
                String ack0 = (String) clientObjectInputStream0.readObject();
                socket0.setSoTimeout(3000);
                Proposal proposalFromServer0 = new Proposal();
                proposalFromServer0.setPortNumber(socket0.getPort());
                proposalFromServer0.setMsgAck(ack0);
                if (ack0 != null && !ack0.isEmpty() && ack0.equals("Ack")) {
                    objectOutputStream0.close();
                    socket0.close();
                }
            }catch (SocketTimeoutException e) {
                Log.e(TAG, "sendingFinalSequenceNumbersToAll - SocketTimeoutException");
                setFaultyPort(REMOTE_PORT0);
                Log.e(TAG, "The dead port is"+myport+"-SocketTimeoutException");
            }catch(IOException e){
                Log.e(TAG, "sendingFinalSequenceNumbersToAll - IOException");
                setFaultyPort(REMOTE_PORT0);
                Log.e(TAG, "The dead port is"+myport+"-IOException");
            } catch (ClassNotFoundException e){
                Log.e(TAG, "sendingFinalSequenceNumbersToAll - ClassNotFoundException");
                setFaultyPort(REMOTE_PORT0);
                Log.e(TAG, "The dead port is"+myport+"-ClassNotFoundException");
            }

            try {
                Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT1));
                ObjectOutputStream objectOutputStream1 = new ObjectOutputStream(socket1.getOutputStream());
                objectOutputStream1.writeObject(messagesTOSend.getMsg());
                objectOutputStream1.writeObject(REMOTE_PORT1);
                objectOutputStream1.writeObject(myport);
                objectOutputStream1.writeObject(messagesTOSend.getIsFinalSequenceNumber());
                objectOutputStream1.flush();
                ObjectInputStream clientObjectInputStream1 = new ObjectInputStream(socket1.getInputStream());
                proposedSeqNumber1 = (Float) clientObjectInputStream1.readObject();
                socket1.setSoTimeout(3000);
                String ack1 = (String) clientObjectInputStream1.readObject();
                socket1.setSoTimeout(3000);
                Proposal proposalFromServer1 = new Proposal();
                proposalFromServer1.setPortNumber(socket1.getPort());
                proposalFromServer1.setMsgAck(ack1);
//                    proposalFromServer1.setProposedSeqNumber(proposedSeqNumber1);
                if (ack1 != null && !ack1.isEmpty() && ack1.equals("Ack")) {
                    objectOutputStream1.close();
                    socket1.close();
                }
            }catch (SocketTimeoutException e) {
                Log.e(TAG, "sendingFinalSequenceNumbersToAll - SocketTimeoutException");
                setFaultyPort(REMOTE_PORT1);
                Log.e(TAG, "The dead port is"+myport+"-SocketTimeoutException");
            }catch(IOException e){
                Log.e(TAG, "sendingFinalSequenceNumbersToAll - IOException");
                setFaultyPort(REMOTE_PORT1);
                Log.e(TAG, "The dead port is"+myport+"-IOException");
            } catch (ClassNotFoundException e){
                Log.e(TAG, "sendingFinalSequenceNumbersToAll - ClassNotFoundException");
                setFaultyPort(REMOTE_PORT1);
                Log.e(TAG, "The dead port is"+myport+"-ClassNotFoundException");
            }

            try {
                Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT2));
                ObjectOutputStream objectOutputStream2 = new ObjectOutputStream(socket2.getOutputStream());
                objectOutputStream2.writeObject(messagesTOSend.getMsg());
                objectOutputStream2.writeObject(REMOTE_PORT2);
                objectOutputStream2.writeObject(myport);
                objectOutputStream2.writeObject(messagesTOSend.getIsFinalSequenceNumber());
                objectOutputStream2.flush();
                ObjectInputStream clientObjectInputStream2 = new ObjectInputStream(socket2.getInputStream());
                proposedSeqNumber2 = (Float) clientObjectInputStream2.readObject();
                socket2.setSoTimeout(3000);
                String ack2 = (String) clientObjectInputStream2.readObject();
                socket2.setSoTimeout(3000);
                Proposal proposalFromServer2 = new Proposal();
                proposalFromServer2.setPortNumber(socket2.getPort());
                proposalFromServer2.setMsgAck(ack2);
//                    proposalFromServer2.setProposedSeqNumber(proposedSeqNumber2);
                if (ack2 != null && !ack2.isEmpty() && ack2.equals("Ack")) {
                    objectOutputStream2.close();
                    socket2.close();
                }
            }catch (SocketTimeoutException e) {
                Log.e(TAG, "sendingFinalSequenceNumbersToAll - SocketTimeoutException");
                setFaultyPort(REMOTE_PORT2);
                Log.e(TAG, "The dead port is"+REMOTE_PORT2+"-SocketTimeoutException");
            }catch(IOException e){
                Log.e(TAG, "sendingFinalSequenceNumbersToAll - IOException");
                setFaultyPort(REMOTE_PORT2);
                Log.e(TAG, "The dead port is"+REMOTE_PORT2+"-IOException");
            } catch (ClassNotFoundException e){
                Log.e(TAG, "sendingFinalSequenceNumbersToAll - ClassNotFoundException");
                setFaultyPort(REMOTE_PORT2);
                Log.e(TAG, "The dead port is"+REMOTE_PORT2+"-ClassNotFoundException");
            }

            try{
                Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT3));
                ObjectOutputStream objectOutputStream3 = new ObjectOutputStream(socket3.getOutputStream());
                objectOutputStream3.writeObject(messagesTOSend.getMsg());
                objectOutputStream3.writeObject(REMOTE_PORT3);
                objectOutputStream3.writeObject(myport);
                objectOutputStream3.writeObject(messagesTOSend.getIsFinalSequenceNumber());
                objectOutputStream3.flush();
                ObjectInputStream clientObjectInputStream3 = new ObjectInputStream(socket3.getInputStream());
                proposedSeqNumber3 = (Float) clientObjectInputStream3.readObject();
                socket3.setSoTimeout(3000);
                String ack3 = (String)clientObjectInputStream3.readObject();
                socket3.setSoTimeout(3000);
                Proposal proposalFromServer3 = new Proposal();
                proposalFromServer3.setPortNumber(socket3.getPort());
                proposalFromServer3.setMsgAck(ack3);
//                    proposalFromServer3.setProposedSeqNumber(proposedSeqNumber3);
                if (ack3 != null && !ack3.isEmpty() && ack3.equals("Ack")) {
                    objectOutputStream3.close();
                    socket3.close();
                }
            }catch (SocketTimeoutException e) {
                Log.e(TAG, "sendingFinalSequenceNumbersToAll - SocketTimeoutException");
                setFaultyPort(REMOTE_PORT3);
                Log.e(TAG, "The dead port is"+REMOTE_PORT3+"-SocketTimeoutException");
            }catch(IOException e){
                Log.e(TAG, "sendingFinalSequenceNumbersToAll - IOException");
                setFaultyPort(REMOTE_PORT3);
                Log.e(TAG, "The dead port is"+REMOTE_PORT3+"-IOException");
            } catch (ClassNotFoundException e){
                Log.e(TAG, "sendingFinalSequenceNumbersToAll - ClassNotFoundException");
                setFaultyPort(REMOTE_PORT3);
                Log.e(TAG, "The dead port is"+REMOTE_PORT3+"-ClassNotFoundException");
            }


            try {
                Socket socket4 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT4));
                ObjectOutputStream objectOutputStream4 = new ObjectOutputStream(socket4.getOutputStream());
                objectOutputStream4.writeObject(messagesTOSend.getMsg());
                objectOutputStream4.writeObject(REMOTE_PORT4);
                objectOutputStream4.writeObject(myport);
                objectOutputStream4.writeObject(messagesTOSend.getIsFinalSequenceNumber());
                objectOutputStream4.flush();
                ObjectInputStream clientObjectInputStream4 = new ObjectInputStream(socket4.getInputStream());
                proposedSeqNumber4 = (Float) clientObjectInputStream4.readObject();
                socket4.setSoTimeout(3000);
                String ack4 = (String) clientObjectInputStream4.readObject();
                socket4.setSoTimeout(3000);
                Proposal proposalFromServer4 = new Proposal();
                proposalFromServer4.setPortNumber(socket4.getPort());
                proposalFromServer4.setMsgAck(ack4);
//                    proposalFromServer4.setProposedSeqNumber(proposedSeqNumber4);
                if (ack4 != null && !ack4.isEmpty() && ack4.equals("Ack")) {
                    objectOutputStream4.close();
                    socket4.close();
                }
            }catch (SocketTimeoutException e) {
                Log.e(TAG, "sendingFinalSequenceNumbersToAll - SocketTimeoutException");
                setFaultyPort(REMOTE_PORT4);
                Log.e(TAG, "The dead port is"+REMOTE_PORT4+"-SocketTimeoutException");
            }catch(IOException e){
                Log.e(TAG, "sendingFinalSequenceNumbersToAll - IOException");
                setFaultyPort(REMOTE_PORT4);
                Log.e(TAG, "The dead port is"+REMOTE_PORT4+"-IOException");
            } catch (ClassNotFoundException e){
                Log.e(TAG, "sendingFinalSequenceNumbersToAll - ClassNotFoundException");
                setFaultyPort(REMOTE_PORT4);
                Log.e(TAG, "The dead port is"+REMOTE_PORT4+"-ClassNotFoundException");
            }
//
//            if(proposedSeqNumber0!=0.0 && proposedSeqNumber1!=0.0 && proposedSeqNumber2!=0.0 &&
//                    proposedSeqNumber3!=0.0 && proposedSeqNumber4!=0.0) isFinalSequenceNumber=true;
            //                Handle Failure
            if((!deadPorts[0] && !deadPorts[1] && !deadPorts[2] && !deadPorts[3] && !deadPorts[4] &&
                    proposedSeqNumber0!=0.0 && proposedSeqNumber1!=0.0 && proposedSeqNumber2!=0.0 &&
                    proposedSeqNumber3!=0.0 && proposedSeqNumber4!=0.0)||
                    (deadPorts[0] && !deadPorts[1] && !deadPorts[2] && !deadPorts[3] && !deadPorts[4] && proposedSeqNumber1!=0.0 &&
                            proposedSeqNumber2!=0.0 && proposedSeqNumber3!=0.0 && proposedSeqNumber4!=0.0) ||
                    (!deadPorts[0] && deadPorts[1] && !deadPorts[2] && !deadPorts[3] && !deadPorts[4] && proposedSeqNumber0!=0.0 &&
                            proposedSeqNumber2!=0.0 && proposedSeqNumber3!=0.0 && proposedSeqNumber4!=0.0) ||
                    (!deadPorts[0] && !deadPorts[1] && deadPorts[2] && !deadPorts[3] && !deadPorts[4] && proposedSeqNumber1!=0.0 &&
                            proposedSeqNumber0!=0.0 && proposedSeqNumber3!=0.0 && proposedSeqNumber4!=0.0)||
                    (!deadPorts[0] && !deadPorts[1] && !deadPorts[2] && deadPorts[3] && !deadPorts[4] && proposedSeqNumber1!=0.0 &&
                            proposedSeqNumber2!=0.0 && proposedSeqNumber0!=0.0 && proposedSeqNumber4!=0.0) ||
                    (!deadPorts[0] && !deadPorts[1] && !deadPorts[2] && !deadPorts[3] && deadPorts[4] && proposedSeqNumber1!=0.0 &&
                            proposedSeqNumber2!=0.0 && proposedSeqNumber3!=0.0 && proposedSeqNumber0!=0.0)
                ) isFinalSequenceNumber=true;

//                }
            if (isFinalSequenceNumber!=null && isFinalSequenceNumber){
                float finalSeqNumber = Math.max(Math.max(Math.max(proposedSeqNumber0,proposedSeqNumber1),Math.max                                                               (proposedSeqNumber2, proposedSeqNumber3)),proposedSeqNumber4);

                ClientToServerMessageCommunication clientToServerMessageCommunication = new ClientToServerMessageCommunication(msg, myport, finalSeqNumber,true);

                if(!deadPorts[0])sendingFinalSequenceNumbersToAll(finalSeqNumber, clientToServerMessageCommunication, REMOTE_PORT0,myport);
                if(!deadPorts[1])sendingFinalSequenceNumbersToAll(finalSeqNumber, clientToServerMessageCommunication, REMOTE_PORT1,myport);
                if(!deadPorts[2])sendingFinalSequenceNumbersToAll(finalSeqNumber, clientToServerMessageCommunication, REMOTE_PORT2,myport);
                if(!deadPorts[3])sendingFinalSequenceNumbersToAll(finalSeqNumber, clientToServerMessageCommunication, REMOTE_PORT3,myport);
                if(!deadPorts[4])sendingFinalSequenceNumbersToAll(finalSeqNumber, clientToServerMessageCommunication, REMOTE_PORT4,myport);
            }
            return null;
        }
    }

    private void sendingFinalSequenceNumbersToAll(float finalSeqNumber, ClientToServerMessageCommunication clientToServerMessageCommunication, String remotePort0,String myPort){
        try{
            Socket socket00 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort0));
            ObjectOutputStream objectOutputStream0 = new ObjectOutputStream(socket00.getOutputStream());
            objectOutputStream0.writeObject(clientToServerMessageCommunication.getMsg());
            objectOutputStream0.writeObject(remotePort0);
            objectOutputStream0.writeObject(myPort);
            objectOutputStream0.writeObject(true);
            objectOutputStream0.writeObject(finalSeqNumber);
            objectOutputStream0.flush();
            ObjectInputStream clientObjectInputStream0 = new ObjectInputStream(socket00.getInputStream());
            String finalAck0 = (String) clientObjectInputStream0.readObject();
            socket00.setSoTimeout(3000);
            if (finalAck0 != null && !finalAck0.isEmpty() && finalAck0.matches("Message with sequence number received")) {
                objectOutputStream0.close();
                clientObjectInputStream0.close();
                socket00.close();
            }
        } catch (SocketTimeoutException e) {
            Log.e(TAG, "sendingFinalSequenceNumbersToAll - SocketTimeoutException");
            setFaultyPort(remotePort0);
            Log.e(TAG, "The dead port is"+remotePort0+"-SocketTimeoutException");
        }catch(IOException e){
            Log.e(TAG, "sendingFinalSequenceNumbersToAll - IOException");
            setFaultyPort(remotePort0);
            Log.e(TAG, "The dead port is"+remotePort0+"-IOException");
        } catch (ClassNotFoundException e){
            Log.e(TAG, "sendingFinalSequenceNumbersToAll - ClassNotFoundException");
            setFaultyPort(remotePort0);
            Log.e(TAG, "The dead port is"+remotePort0+"-ClassNotFoundException");
        }
    }

    private void setFaultyPort(String remotePort0) {
        if (remotePort0 == REMOTE_PORT0) deadPorts[0] = TRUE;
        else if (remotePort0 == REMOTE_PORT1) deadPorts[1] = TRUE;
        else if (remotePort0 == REMOTE_PORT2) deadPorts[2] = TRUE;
        else if (remotePort0 == REMOTE_PORT3) deadPorts[3] = TRUE;
        else if (remotePort0 == REMOTE_PORT4) deadPorts[4] = TRUE;
    }
}