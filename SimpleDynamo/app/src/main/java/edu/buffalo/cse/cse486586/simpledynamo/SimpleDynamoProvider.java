package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.acl.LastOwnerException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Formatter;
import java.util.Iterator;
import java.util.Set;

import android.annotation.SuppressLint;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    Boolean isLocalStorageOnly = false;
    Boolean recoveryCursorSet = true;
    Boolean recoveryGoingOn = false;
    Boolean initialGlobalQueryCompleted = true;
    Boolean calledClientTaskForGlobalQuery = false;
    MyNode node = new MyNode();
    static final int SERVER_PORT = 10000;
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    ArrayList<String> nodesInRing = new ArrayList<String>();
    Boolean calledClientTaskForGlobalQueryStar=false;
    Boolean initialGlobalQueryCompletedStar=true;
    Boolean isPreviousQueryCompleted=true;
    Boolean valueFromGlobalNodeForAVDStar = false;
    MatrixCursor globalStarQueryResult = new MatrixCursor(new String[]{"key", "value"});
    Boolean calledClientTaskForGlobalDelete = false;
    Boolean initialGlobalDeleteCompleted=true;
    Boolean previousInsertComplete = true;
    String globalMyAvdIDHash = "";
    String globalQueryOutput1 = "";
    MatrixCursor recoveryCursor = new MatrixCursor(new String[]{"key", "value"});
    String TAG = SimpleDynamoProvider.class.getSimpleName();

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private void performInsert(String[] msgs){
        try {
            Log.i(TAG, "Perform global insert in Client");

        String port1 = msgs[0];
        String key = msgs[1];
        String value = msgs[2];
        if (port1!=null){
            String portToConnect = "";
            portToConnect = getHashToPort(port1, portToConnect);
            Socket socket = new Socket
                    (InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portToConnect));
            Log.i(TAG, "Connected to port = "+portToConnect);
            ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
            outputStream.writeObject("Perform insert");
            outputStream.writeObject(key);
            outputStream.writeObject(value);
            outputStream.flush();

            ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
            String ack = (String) objectInputStream.readObject();
            Log.i(TAG, "Received ack = "+ack);
            if (ack!=null && !ack.isEmpty() && "ack".equalsIgnoreCase(ack)){
                outputStream.close();
                socket.close();
            }
        }
        Log.i(TAG, "Completed global insert in Client");
    }catch (IOException e){
            Log.e(TAG,"IOException");
        }catch (ClassNotFoundException e){
            Log.e(TAG,"ClassNotFoundException");
        }
    }

    private void queryGlobalNode(String originalKeyFromQuery, String valueToSetWhere, String correctPortNumber) throws ClassNotFoundException {
        String queryGlobalNode = "queryGlobalNode";
        try{
            Socket socketForInsertToGlobalNode = new Socket();
            socketForInsertToGlobalNode.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(correctPortNumber)),300);
//                    Socket socketForInsertToGlobalNode = new Socket
//                            (InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(correctPortNumber));
            ObjectOutputStream objectOutputStreamForInsertToGlobalNode = new ObjectOutputStream(socketForInsertToGlobalNode.getOutputStream());
            objectOutputStreamForInsertToGlobalNode.writeObject("Perform Global Query into Your Node");
            Log.e(TAG,"originalKeyFromQuery1 = "+originalKeyFromQuery);
            Log.e(TAG,"Perform Global Query into Your Node");
            objectOutputStreamForInsertToGlobalNode.writeObject(originalKeyFromQuery);
//                    objectOutputStreamForInsertToGlobalNode.writeObject(valueToSetWhere);
            objectOutputStreamForInsertToGlobalNode.flush();
            Log.e(TAG,"Write complete to server for Global query");

            ObjectInputStream inputStreamForInsertToGlobalNode = new ObjectInputStream(socketForInsertToGlobalNode.getInputStream());
            Log.e(TAG,"Write complete to server for Global query1");
//            socketForInsertToGlobalNode.setSoTimeout(200);
            String valueFromGlobalNode = (String) inputStreamForInsertToGlobalNode.readObject();
            String ackForInsertToGlobalNode = (String) inputStreamForInsertToGlobalNode.readObject();
            Log.e(TAG,"Read complete from server for Global query");
            if (ackForInsertToGlobalNode!=null && !ackForInsertToGlobalNode.isEmpty()
                    && "Query Success in correctNode".equalsIgnoreCase(ackForInsertToGlobalNode)){
                Log.e(TAG,"Query Success in correctNode");
                calledClientTaskForGlobalQuery=false;
                Log.e(TAG, "calledClientTaskForGlobalQuery=false");
                Log.e(TAG, "initialGlobalQueryCompleted=true");
                initialGlobalQueryCompleted = true;
                Log.e(TAG, "valueFromGlobalNode:"+valueFromGlobalNode);

                if ("original".equalsIgnoreCase(valueToSetWhere)){
                    globalQueryOutput1 = valueFromGlobalNode;
                    Log.e(TAG,"globalQueryOutput1 = "+globalQueryOutput1);
                }
                objectOutputStreamForInsertToGlobalNode.close();
                socketForInsertToGlobalNode.close();
            }
        }catch(SocketTimeoutException e){
            Log.e(TAG,"SocketTimeoutException - CorrectNode is dead");
            queryDeadNodesSuccessor(originalKeyFromQuery, correctPortNumber);
        }catch(StreamCorruptedException e){
            Log.e(TAG,"StreamCorruptedException - CorrectNode is dead");
            queryDeadNodesSuccessor(originalKeyFromQuery, correctPortNumber);
        }catch(IOException e){
            Log.e(TAG,"IOException - CorrectNode is dead");
            queryDeadNodesSuccessor(originalKeyFromQuery, correctPortNumber);
        }
    }
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        String TAG = "DELETE";
        Boolean dontDeleteInOtherNodes = false;
//        try{
//            if (calledClientTaskForGlobalDelete)
//                Thread.sleep(1000);
//        }catch (InterruptedException e) {
//            Log.e(TAG, "InterruptedException");
//        }

//        if (node.prev==null && node.next==null) isLocalStorageOnly=true;
        Context context = getContext();
        try{
            String [] selectionSplit;
            if (selection!=null && !selection.isEmpty() && selection.contains("true")){
                dontDeleteInOtherNodes = true;
                Log.e(TAG,"dontDeleteInOtherNodes = true");
                selectionSplit=selection.split("--");
                selection = selectionSplit[0];
                Log.e(TAG,"Final selection"+selection);
            }else {
                Log.e(TAG,"dontDeleteInOtherNodes=false");
                dontDeleteInOtherNodes=false;
            }
            String currentKeyHash = genHash(selection);
            Log.e(TAG,"currentKeyHash = "+currentKeyHash);
            String correctNode  = checkForCorrectPartition(currentKeyHash);
            Log.e(TAG,"correctNode = "+correctNode);
            ArrayList<String> twoReplicas = getFollowingTwoNodes(correctNode);
            Log.e(TAG, "twoReplicas0 = "+twoReplicas.get(0));
            Log.e(TAG, "twoReplicas1 = "+twoReplicas.get(1));

            if (dontDeleteInOtherNodes){
                Log.e(TAG,"Only deleting -called from server");
                Boolean x = deleteMyFile(context,currentKeyHash);
                dontDeleteInOtherNodes = false;
                Log.e(TAG,"dontDeleteInOtherNodes=false");
            } else if(globalMyAvdIDHash.equalsIgnoreCase(correctNode)){
                Log.e(TAG,"I am correct node");
                Boolean x = deleteMyFile(context,currentKeyHash);
                if(x)
                    Log.e(TAG,"Deleted file = "+selection);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, currentKeyHash,selection,
                        twoReplicas.get(0),"Delete Request");
                Log.e(TAG,"ClientTask for replica 0 called");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, currentKeyHash,selection,
                        twoReplicas.get(1),"Delete Request");
                Log.e(TAG,"ClientTask for replica 1 called");
                Log.e(TAG,"END");
            }else if (globalMyAvdIDHash.equalsIgnoreCase(twoReplicas.get(0))){
                Log.e(TAG,"I am replica 0");
                Boolean x = deleteMyFile(context,currentKeyHash);
                if(x)
                    Log.e(TAG,"Deleted file = "+selection);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, currentKeyHash,selection,
                        correctNode,"Delete Request");
                Log.e(TAG,"ClientTask for correctNode called");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, currentKeyHash,selection,
                        twoReplicas.get(1),"Delete Request");
                Log.e(TAG,"ClientTask for replica 1 called");
                Log.e(TAG,"END");
            }else if (globalMyAvdIDHash.equalsIgnoreCase(twoReplicas.get(1))){
                Log.e(TAG,"I am replica 1");
                Boolean x = deleteMyFile(context,currentKeyHash);
                if(x)
                    Log.e(TAG,"Deleted file = "+selection);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, currentKeyHash,selection,
                        correctNode,"Delete Request");
                Log.e(TAG,"ClientTask for correctNode called");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, currentKeyHash,selection,
                        twoReplicas.get(0),"Delete Request");
                Log.e(TAG,"ClientTask for replica 0 called");
                Log.e(TAG,"END");
            }else {
                Log.e(TAG,"Global");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, currentKeyHash,selection,
                        correctNode,"Delete Request");
                Log.e(TAG,"ClientTask for correctNode called");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, currentKeyHash,selection,
                        twoReplicas.get(0),"Delete Request");
                Log.e(TAG,"ClientTask for replica -0 called");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, currentKeyHash,selection,
                        twoReplicas.get(1),"Delete Request");
                Log.e(TAG,"ClientTask for replica 1 called");
                Log.e(TAG,"END");
            }
//            if(node.getNext()==null && node.getPrev()==null && isLocalStorageOnly){
//                Log.e(TAG,"isLocalStorageOnly"+"=true");
//                InputStream is = context.openFileInput(currentKeyHash);
//                if (is!=null) {
//                    File x = context.getFilesDir();
//                    File actualFile = new File(x, currentKeyHash);
//                    actualFile.delete();
//                }
//            } else if((node.getHashValue().compareTo(currentKeyHash)>0 && node.getPrev()!=null && !node.getPrev().isEmpty() &&
//                    node.getPrev().compareTo(currentKeyHash)<0)){
//                Log.e(TAG,"Normal range");
//
//                if (deleteMyFile(context, currentKeyHash)) return 1;
//            }else if(node.getHashValue().compareTo(currentKeyHash)<0 && node.getPrev()!=null && !node.getPrev().isEmpty() &&
//                    node.getPrev().compareTo(currentKeyHash)<0 && node.getPrev().compareTo(node.getHashValue())>0){
//                Log.e(TAG,"Cond2");
//                if (deleteMyFile(context, currentKeyHash)) return 1;
//            } else if(node.getHashValue().compareTo(currentKeyHash)>0 && node.getPrev()!=null && !node.getPrev().isEmpty() &&
//                    node.getPrev().compareTo(currentKeyHash)>0 && node.getPrev().compareTo(node.getHashValue())>0) {
//                Log.e(TAG,"Cond3");
//                if (deleteMyFile(context, currentKeyHash)) return 1;
//            } else {
//                Log.e("Global node to delete","keyhash:"+currentKeyHash+"key"+selection);
//                if(initialGlobalDeleteCompleted){
//                    Log.e(TAG,initialGlobalDeleteCompleted.toString());
//                    Log.e(TAG,"Calling clientTask for delete Request");
//                    calledClientTaskForGlobalDelete = true;
//                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, currentKeyHash,selection,"dummy","Delete Request");
//                }
//            }
        }catch (NoSuchAlgorithmException e){
            Log.e(TAG,"NoSuchAlgorithmException in delete");
        }catch (FileNotFoundException e){
            Log.e(TAG,"FileNotFoundException in delete");
        }
        return 0;
    }

    private boolean deleteMyFile(Context context, String currentKeyHash) throws FileNotFoundException {
        InputStream is = context.openFileInput(currentKeyHash);
        if (is!=null){
            File x = context.getFilesDir();
            File actualFile = new File(x,currentKeyHash);
            actualFile.delete();
            return true;
        }
        return false;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @SuppressLint("LongLogTag")
    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Boolean noReplicationToDo = false;
        String TAG = "INSERT";
        Log.v(TAG,"Inside insert()");
        Context context = getContext();
        Set<String> keySet = values.keySet();
        Iterator<String> iteratorKeys = keySet.iterator();
        String x = "";

        while(recoveryGoingOn) {
            Log.e(TAG,"recoveryGoingOn in while (true)= "+previousInsertComplete);
            try {
                Thread.sleep(100);
            }catch (InterruptedException e){
                Log.e(TAG,"InterruptedException in recoveryGoingOn");
            }
        }
//        previousInsertComplete=false;
//        Log.e(TAG,"previousInsertComplete = "+previousInsertComplete);

        while(iteratorKeys.hasNext()) {
            x = iteratorKeys.next();
            FileOutputStream outputStream;
            try {
                String fileValue = values.get(x).toString();
                Log.v(TAG,"TEST value= "+fileValue);
                String key = values.get(iteratorKeys.next()).toString();
                Log.v(TAG,"TEST key= "+key);
                String []keySplit;
                if (key!=null && !key.isEmpty() && key.contains("true")){
                    noReplicationToDo = true;
                    Log.e(TAG,"server has set noReplicationToDo = true for INSERT");
                    Log.e(TAG,"Setting noReplicationToDo = true in INSERT");
                    keySplit = key.split("--");
                    key = keySplit[0];
                } else {
                    Log.e(TAG,"key.contains(\"true\") == false");
                    noReplicationToDo=false;
                }
                String keyHash = genHash(key);
                Log.v(TAG,"keyhash = "+keyHash);
                Log.v(TAG,"final key = "+key);
                Log.v(TAG,"DANGER = "+noReplicationToDo);
                if(!noReplicationToDo){
                    Log.v(TAG,"noReplicationToDo = false - inside insert");

                    String correctNode = checkForCorrectPartition(keyHash);
                    Log.e(TAG,"Correct node = "+correctNode);
                    ArrayList<String> twoReplicas = getFollowingTwoNodes(correctNode);
                    Log.e(TAG, "twoReplicas0 = "+twoReplicas.get(0));
                    Log.e(TAG, "twoReplicas1 = "+twoReplicas.get(1));
                    if (correctNode.equalsIgnoreCase(globalMyAvdIDHash)){
                        Log.v("either local storage or my hash space", "Inserting");
                        outputStream = context.openFileOutput(keyHash, Context.MODE_PRIVATE);
                        String xyz = key+"-";

                        outputStream.write(xyz.getBytes());
                        outputStream.write(fileValue.getBytes());
                        //                    outputStream.write("-"+);  //Add object versioning
                        Log.v("inserted in local storage", values.toString());
                        outputStream.close();
                        Log.v(TAG,"called client task for replica insertion");
                        String [] vals = new String[3];
                        vals[0]=twoReplicas.get(0);
                        vals[1]=key;
                        vals[2]=fileValue;
                        performInsert(vals);
//                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, twoReplicas.get(0),
//                                key,fileValue, "Perform global insert");
//                        try{
//                            Thread.sleep(200);
//                        } catch (InterruptedException e){
//                            Log.e(TAG,"InterruptedException in sleep");
//                        }
                        Log.v(TAG,"called client task 2 for replica insertion");
                        String [] vals2 = new String[3];
                        vals2[0]=twoReplicas.get(1);
                        vals2[1]=key;
                        vals2[2]=fileValue;
                        performInsert(vals2);
//                        try {performInsert(vals2);}catch (ClassNotFoundException e){
//                            Log.e(TAG,"ClassNotFoundException");
//                        }
//                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, twoReplicas.get(1),
//                                key,fileValue, "Perform global insert");
//                        try{
//                            Thread.sleep(200);
//                        } catch (InterruptedException e){
//                            Log.e(TAG,"InterruptedException in sleep");
//                        }
                        Log.v(TAG,"replica insertion - completed");
                        previousInsertComplete = true;
                        Log.e(TAG,"previousInsertComplete1 =  "+previousInsertComplete);
                    }
                    else if(twoReplicas.size()>0 && twoReplicas.get(0).equalsIgnoreCase(globalMyAvdIDHash)){
                        Log.v("I am replica 0", "Inserting");
                        outputStream = context.openFileOutput(keyHash, Context.MODE_PRIVATE);
                        String xyz = key+"-";

                        outputStream.write(xyz.getBytes());
                        outputStream.write(fileValue.getBytes());
                        //                    outputStream.write("-"+);  //Add object versioning
                        Log.v("inserted in local storage", values.toString());
                        outputStream.close();
                        Log.v(TAG,"called client task for replica insertion");
                        String [] vals = new String[3];
                        vals[0]=correctNode;
                        vals[1]=key;
                        vals[2]=fileValue;
                        performInsert(vals);
//                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, correctNode,
//                                key,fileValue, "Perform global insert");
//                        try{
//                            Thread.sleep(200);
//                        } catch (InterruptedException e){
//                            Log.e(TAG,"InterruptedException in sleep");
//                        }
                        String [] vals2 = new String[3];
                        vals2[0]=twoReplicas.get(1);
                        vals2[1]=key;
                        vals2[2]=fileValue;
                        performInsert(vals2);
//                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, twoReplicas.get(1),
//                                key,fileValue, "Perform global insert");
//                        try{
//                            Thread.sleep(200);
//                        } catch (InterruptedException e){
//                            Log.e(TAG,"InterruptedException in sleep");
//                        }
                        Log.v(TAG,"replica insertion - completed");
                        previousInsertComplete = true;
                        Log.e(TAG,"previousInsertComplete12 =  "+previousInsertComplete);
                    }else if(twoReplicas.size()>1 && twoReplicas.get(1).equalsIgnoreCase(globalMyAvdIDHash)){
                        Log.v("I am replica 1", "Inserting");
                        outputStream = context.openFileOutput(keyHash, Context.MODE_PRIVATE);
                        String xyz = key+"-";

                        outputStream.write(xyz.getBytes());
                        outputStream.write(fileValue.getBytes());
                        //                    outputStream.write("-"+);  //Add object versioning
                        Log.v("inserted in local storage", values.toString());
                        outputStream.close();
                        Log.v(TAG,"called client task for replica insertion");
                        String [] vals = new String[3];
                        vals[0]=correctNode;
                        vals[1]=key;
                        vals[2]=fileValue;
                        performInsert(vals);
//                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, correctNode,
//                                key,fileValue, "Perform global insert");
//                        try{
//                            Thread.sleep(200);
//                        } catch (InterruptedException e){
//                            Log.e(TAG,"InterruptedException in sleep");
//                        }
                        String [] vals2 = new String[3];
                        vals2[0]=twoReplicas.get(0);
                        vals2[1]=key;
                        vals2[2]=fileValue;
                        performInsert(vals2);
//                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, twoReplicas.get(0),
//                                key,fileValue, "Perform global insert");
//                        try{
//                            Thread.sleep(200);
//                        } catch (InterruptedException e){
//                            Log.e(TAG,"InterruptedException in sleep");
//                        }
                        Log.v(TAG,"replica insertion76 - completed");
                        previousInsertComplete = true;
                        Log.e(TAG,"previousInsertComplete123 =  "+previousInsertComplete);
                    }
                    else{
                        Log.v("Global insert", "calling client task");
                        String [] vals = new String[3];
                        vals[0]=correctNode;
                        vals[1]=key;
                        vals[2]=fileValue;
                        performInsert(vals);
//                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, correctNode,
//                                key,fileValue, "Perform global insert");
//                        try{Thread.sleep(200);}
//                        catch (InterruptedException e){
//                            Log.e(TAG,"InterruptedException in sleep");
//                        }
                        String [] vals2 = new String[3];
                        vals2[0]=twoReplicas.get(0);
                        vals2[1]=key;
                        vals2[2]=fileValue;
                        performInsert(vals2);
//                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, twoReplicas.get(0),
//                                key,fileValue, "Perform global insert");
//                        try{Thread.sleep(200);}
//                        catch (InterruptedException e){
//                            Log.e(TAG,"InterruptedException in sleep");
//                        }
                        String [] vals3 = new String[3];
                        vals3[0]=twoReplicas.get(1);
                        vals3[1]=key;
                        vals3[2]=fileValue;
                        performInsert(vals3);
//                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, twoReplicas.get(1),
//                                key,fileValue, "Perform global insert");
//                        try{Thread.sleep(200);}
//                        catch (InterruptedException e){
//                            Log.e(TAG,"InterruptedException in sleep");
//                        }
                        Log.v(TAG,"Entire global insertion - completed");
                        previousInsertComplete = true;
                        Log.e(TAG,"previousInsertComplete2 =  "+previousInsertComplete);
                    }
                }else {
                    Log.v(TAG,"noReplicationToDo=true");
                    outputStream = context.openFileOutput(keyHash, Context.MODE_PRIVATE);
                    String xyz = key+"-";
                    outputStream.write(xyz.getBytes());
                    outputStream.write(fileValue.getBytes());
                    Log.v(TAG,"key = "+key+"#"+"value = "+fileValue);
                    //                    outputStream.write("-"+);  //Add object versioning
                    Log.v("inserted in local storage -noReplicationToDo ", values.toString());
                    outputStream.close();
                    noReplicationToDo = false;
                    Log.v(TAG,"noReplicationToDo=false");
                    previousInsertComplete = true;
                    Log.e(TAG,"previousInsertComplete3 =  "+previousInsertComplete);
                }
            }catch (NoSuchAlgorithmException e){
                Log.e(TAG,"NoSuchAlgorithmException in Insert");
            }catch (IOException e){
                Log.e(TAG,"IOException in Insert");
            }
        }
        return uri;
    }

    private ArrayList<String> getFollowingTwoNodes(String correctNode) {
        ArrayList<String> resultNodes = new ArrayList<String>();
        int correctNodeIndex = nodesInRing.indexOf(correctNode);
        for (int i =0; i<nodesInRing.size();i++){
            if (nodesInRing.size() != 1
                    && correctNodeIndex==nodesInRing.size()-1) {
                resultNodes.add(nodesInRing.get(0));
                resultNodes.add(nodesInRing.get(1));
                return resultNodes;
            } else if(correctNodeIndex==i){
                if (i+1<nodesInRing.size())
                    resultNodes.add(nodesInRing.get(i+1));
                else
                {
                    resultNodes.add(nodesInRing.get(0));
                    if (nodesInRing.size()>=2)
                        resultNodes.add(nodesInRing.get(1));
                    return resultNodes;
                }
                if (i+2<nodesInRing.size())
                    resultNodes.add(nodesInRing.get(i+2));
                else
                    resultNodes.add(nodesInRing.get(0));
                return resultNodes;
            }
        }
        return null;
    }

    private String checkForCorrectPartition(String keyHash) {
        if(nodesInRing.size()==0){
            Log.e(TAG,"Returned null");
            return null;
        }else {
            for (int i=0; i<nodesInRing.size(); i++){
                Log.e(TAG,"Current key = "+keyHash);
                Log.e(TAG,"node - i ="+nodesInRing.get(i));
                if (i==0 && keyHash.compareTo(nodesInRing.get(i))<0
                        && keyHash.compareTo(nodesInRing.get(nodesInRing.size()-1))>0){
                    Log.e(TAG,"nodesInRing.get(0)");
                    return nodesInRing.get(0);
                }else if (i==0 && keyHash.compareTo(nodesInRing.get(i))>0
                        && keyHash.compareTo(nodesInRing.get(nodesInRing.size()-1))>0){
                    Log.e(TAG,"nodesInRing.get(0)");
                    return nodesInRing.get(0);
                }else if (i==0 && keyHash.compareTo(nodesInRing.get(i))<0
                        && keyHash.compareTo(nodesInRing.get(nodesInRing.size()-1))<0){
                    Log.e(TAG,"nodesInRing.get(0)");
                    return nodesInRing.get(0);
                }else if(i!=0 && keyHash.compareTo(nodesInRing.get(i-1))>0 &&
                        keyHash.compareTo(nodesInRing.get(i))<0){
                    Log.e(TAG,"nodesInRing.get(i) = "+i);
                    return nodesInRing.get(i);
                }
            }
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try{
            nodesInRing = new ArrayList<String>();
            nodesInRing.add(genHash("5554"));
            nodesInRing.add(genHash("5556"));
            nodesInRing.add(genHash("5558"));
            nodesInRing.add(genHash("5560"));
            nodesInRing.add(genHash("5562"));
            Collections.sort(nodesInRing);

            globalMyAvdIDHash = genHash(portStr);
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            recoverNode(globalMyAvdIDHash);
        }catch (IOException e){
            Log.e(TAG, "Can't create a ServerSocket");
        }catch (NoSuchAlgorithmException e){
            Log.e(TAG,"NoSuchAlgorithmException");
            return false;
        }
        return false;
    }

    private void recoverNode(String globalMyAvdIDHash) {
        String TAG = "RecoverNode";
        Log.e(TAG,"Inside recoverNode()");
        recoveryCursorSet = false;
        Log.e(TAG,"recoveryCursorSet = false");
        recoveryGoingOn = true;
        Log.e(TAG,"recoveryGoingOn = true");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "dummy", globalMyAvdIDHash, "dummy", "Node Recovery");
        Log.e(TAG,"clientTask() for Node Recovery completed");

        try{
            while (!recoveryCursorSet){
                Thread.sleep(200);
            }
        }catch (InterruptedException e){
            Log.e(TAG,"Sleep interrupted in recoverNode");
        }

        Log.e(TAG,"Got cursor for recovery");

        if(recoveryCursor!=null && recoveryCursor.getCount()>0)
            getContentValuesFromCursor(recoveryCursor);
        Log.e(TAG,"Inserted CV for recovery");

        recoveryGoingOn = false;
        Log.e(TAG,"recoveryGoingOn = false");
    }

    private void getContentValuesFromCursor(MatrixCursor recoveryCursor) {
        String TAG = "ContentValuesFromCursor";
//        recoveryCursor.moveToFirst();
        int keyIndex = recoveryCursor.getColumnIndex("key");
        int valueIndex = recoveryCursor.getColumnIndex("value");

        Log.e(TAG,"putting cv-insert");
        while (recoveryCursor.moveToNext()){
            String key = recoveryCursor.getString(keyIndex);
            Log.e(TAG,"key = "+key);

            String value = recoveryCursor.getString(valueIndex);
            Log.e(TAG,"value = "+value);

            try{
                String correctNode = checkForCorrectPartition(genHash(key));
                Log.e(TAG,"correctNode for recovery key ="+correctNode);
                Log.e(TAG,"globalMyAvdIDHash ="+globalMyAvdIDHash);
                if ("33d6357cfaaf0f72991b0ecd8c56da066613c089".equalsIgnoreCase(globalMyAvdIDHash) &&
                        (
                                globalMyAvdIDHash.equalsIgnoreCase(correctNode) ||
                                "177ccecaec32c54b82d5aaafc18a2dadb753e3b1".equalsIgnoreCase(correctNode) ||
                                "208f7f72b198dadd244e61801abe1ec3a4857bc9".equalsIgnoreCase(correctNode)
                        )
                ){
                    Log.e(TAG,"Recovering node="+globalMyAvdIDHash);
//                    key = key+"--"+"true";
                    ContentValues cv = new ContentValues();

                    cv.put("key", key);
                    cv.put("value", value);

                    performInternalInsert(TAG, key, value);
//                    insert(mUri,cv);
                    Log.e(TAG,"Inserted for recovery-avd0"+key);
                }else if ("208f7f72b198dadd244e61801abe1ec3a4857bc9".equalsIgnoreCase(globalMyAvdIDHash) &&
                        (
                                globalMyAvdIDHash.equalsIgnoreCase(correctNode) ||
                                        "177ccecaec32c54b82d5aaafc18a2dadb753e3b1".equalsIgnoreCase(correctNode) ||
                                        "c25ddd596aa7c81fa12378fa725f706d54325d12".equalsIgnoreCase(correctNode)
                        )
                ){
                    Log.e(TAG,"Recovering node="+globalMyAvdIDHash);
//                    key = key+"--"+"true";
                    ContentValues cv = new ContentValues();
                    cv.put("key", key);
                    cv.put("value", value);

                    performInternalInsert(TAG, key, value);
//                    insert(mUri,cv);
                    Log.e(TAG,"Inserted for recovery-avd1"+key);
                }else if ("abf0fd8db03e5ecb199a9b82929e9db79b909643".equalsIgnoreCase(globalMyAvdIDHash) &&
                        (
                                globalMyAvdIDHash.equalsIgnoreCase(correctNode) ||
                                        "33d6357cfaaf0f72991b0ecd8c56da066613c089".equalsIgnoreCase(correctNode) ||
                                        "208f7f72b198dadd244e61801abe1ec3a4857bc9".equalsIgnoreCase(correctNode)
                        )
                ){
                    Log.e(TAG,"Recovering node="+globalMyAvdIDHash);
//                    key = key+"--"+"true";
                    ContentValues cv = new ContentValues();
                    cv.put("key", key);
                    cv.put("value", value);
                    performInternalInsert(TAG, key, value);

//                    insert(mUri,cv);
                    Log.e(TAG,"Inserted for recovery-avd2"+key);
                }else if ("c25ddd596aa7c81fa12378fa725f706d54325d12".equalsIgnoreCase(globalMyAvdIDHash) &&
                        (
                                globalMyAvdIDHash.equalsIgnoreCase(correctNode) ||
                                        "abf0fd8db03e5ecb199a9b82929e9db79b909643".equalsIgnoreCase(correctNode) ||
                                        "33d6357cfaaf0f72991b0ecd8c56da066613c089".equalsIgnoreCase(correctNode)
                        )
                ){
                    Log.e(TAG,"Recovering node="+globalMyAvdIDHash);
//                    key = key+"--"+"true";
                    ContentValues cv = new ContentValues();
                    cv.put("key", key);
                    cv.put("value", value);
                    performInternalInsert(TAG, key, value);

//                    insert(mUri,cv);
                    Log.e(TAG,"Inserted for recovery-avd3="+key);
                }else if ("177ccecaec32c54b82d5aaafc18a2dadb753e3b1".equalsIgnoreCase(globalMyAvdIDHash) &&
                        (
                                globalMyAvdIDHash.equalsIgnoreCase(correctNode) ||
                                        "abf0fd8db03e5ecb199a9b82929e9db79b909643".equalsIgnoreCase(correctNode) ||
                                        "c25ddd596aa7c81fa12378fa725f706d54325d12".equalsIgnoreCase(correctNode)
                        )
                ){
                    Log.e(TAG,"Recovering node="+globalMyAvdIDHash);
//                    key = key+"--"+"true";
                    ContentValues cv = new ContentValues();
                    cv.put("key", key);
                    cv.put("value", value);
                    performInternalInsert(TAG, key, value);

//                    insert(mUri,cv);
                    Log.e(TAG,"Inserted for recovery-avd4"+key);
                }
            }catch (NoSuchAlgorithmException e){
                Log.e(TAG,"NoSuchAlgorithmException in getContentValuesFromCursor");
            }

        }
    }

    private void performInternalInsert(String TAG, String key, String value) throws NoSuchAlgorithmException {
        try {
            FileOutputStream outputStream;
            Context context = getContext();
            String keyHash = genHash(key);
            Log.v(TAG, "noReplicationToDo=true");
            outputStream = context.openFileOutput(keyHash, Context.MODE_PRIVATE);
            String xyz = key + "-";
            outputStream.write(xyz.getBytes());
            outputStream.write(value.getBytes());
            Log.v(TAG, "key = " + key + "#" + "value = " + value);
            //                    outputStream.write("-"+);  //Add object versioning
//                        Log.v(TAG, values.toString());
            outputStream.close();
//                        noReplicationToDo = false;
//                        Log.v(TAG, "noReplicationToDo=false");
//                        previousInsertComplete = true;
//                        Log.e(TAG, "previousInsertComplete3 =  " + previousInsertComplete);
        }catch (IOException e){

        }
    }

    @Override
    public MatrixCursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        String TAG = "QUERY";
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
        String key = "";
        String value = "";
        FileInputStream inputStream;
        Context context = getContext();

        Log.e(TAG,"selection = "+selection);
        try {
            Log.e(TAG,"recoveryGoingOn = "+recoveryGoingOn);
            while (recoveryGoingOn){
                Thread.sleep(200);
            }
            Log.e(TAG,"isPreviousQueryCompleted1 = "+isPreviousQueryCompleted);
            while (!isPreviousQueryCompleted && !"@".equalsIgnoreCase(selection) && !"*".equalsIgnoreCase(selection)){
                Thread.sleep(50);
            }
            String currentKeyHash = genHash(selection);
            String correctNode = checkForCorrectPartition(currentKeyHash);
            ArrayList<String> twoReplicas = getFollowingTwoNodes(correctNode);
            if (!"*".equalsIgnoreCase(selection) && !"@".equalsIgnoreCase(selection)) {
                isPreviousQueryCompleted = false;
                Log.e(TAG,"isPreviousQueryCompleted2 = "+isPreviousQueryCompleted);

                Log.e(TAG, "Inside 1");
                Log.e("currentKeyHash",currentKeyHash);
                Log.e("key",selection);
                Log.e("correctNode = ",correctNode);
                Log.e(TAG,"twoReplicas0 = "+twoReplicas.get(0));
                Log.e(TAG,"twoReplicas1 = "+twoReplicas.get(1));
                Log.e(TAG,"globalMyAvdIDHash = "+globalMyAvdIDHash);

                if(correctNode!=null && !correctNode.isEmpty()
                        && correctNode.equalsIgnoreCase(globalMyAvdIDHash)){
                    Log.e("key in Local storage","Normal range");
                    InputStream is = context.openFileInput(currentKeyHash);
                    InputStreamReader ir = new InputStreamReader(is);
                    BufferedReader br = new BufferedReader(ir);
                    String contentsOfFile = br.readLine();
                    String [] keyValues = contentsOfFile.split("-");
                    String x = keyValues[0];
                    String y = keyValues[1];
                    Log.e(TAG,"x = "+x);
                    Log.e(TAG,"y = "+y);
                    is.close();
                    matrixCursor.addRow(new String[]{x, y});
                    isPreviousQueryCompleted=true;
                    Log.e(TAG,"Found in my node itself");
                    Log.e(TAG,"isPreviousQueryCompleted=true");
                    return matrixCursor;
                }else if(twoReplicas.size()>0 && twoReplicas.get(0)!=null
                        && !twoReplicas.get(0).isEmpty()
                        && twoReplicas.get(0).equalsIgnoreCase(globalMyAvdIDHash)){
                    Log.e(TAG,"I am replica 0");
                    InputStream is = context.openFileInput(currentKeyHash);
                    InputStreamReader ir = new InputStreamReader(is);
                    BufferedReader br = new BufferedReader(ir);
                    String contentsOfFile = br.readLine();
                    String [] keyValues = contentsOfFile.split("-");
                    String x = keyValues[0];
                    String y = keyValues[1];
                    Log.e(TAG,"x = "+x);
                    Log.e(TAG,"y = "+y);
                    is.close();
                    matrixCursor.addRow(new String[]{x, y});
                    isPreviousQueryCompleted=true;
                    Log.e(TAG,"replica 0 returned result");
                    Log.e(TAG,"isPreviousQueryCompleted=true");
                    return matrixCursor;
                }else if(twoReplicas.size()>=1 && twoReplicas.get(1)!=null
                        && !twoReplicas.get(1).isEmpty()
                        && twoReplicas.get(1).equalsIgnoreCase(globalMyAvdIDHash)){
                    Log.e(TAG,"I am replica 1");
                    InputStream is = context.openFileInput(currentKeyHash);
                    InputStreamReader ir = new InputStreamReader(is);
                    BufferedReader br = new BufferedReader(ir);
                    String contentsOfFile = br.readLine();
                    String [] keyValues = contentsOfFile.split("-");
                    String x = keyValues[0];
                    String y = keyValues[1];
                    Log.e(TAG,"x = "+x);
                    Log.e(TAG,"y = "+y);
                    is.close();
                    matrixCursor.addRow(new String[]{x, y});
                    isPreviousQueryCompleted=true;
                    Log.e(TAG,"second replica returned");
                    Log.e(TAG,"isPreviousQueryCompleted=true");
                    return matrixCursor;
                }else {
                    String myKey = selection;
                    Log.e(TAG,"Query - else");
                    String correctPortNumber = "";
                    correctPortNumber = getHashToPort(correctNode,correctPortNumber);

                    queryGlobalNode(myKey, "original", correctPortNumber);
//                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, correctNode,
//                            myKey,"original","Query Global node");
                    Log.e(TAG,"called original - now sleep 200");
                    while (globalQueryOutput1==null || globalQueryOutput1.isEmpty()){
//                        Log.e(TAG,"Waiting for globalQueryOutput1 to be filled");
                        Thread.sleep(50);}
                    if (globalQueryOutput1!=null && !globalQueryOutput1.isEmpty()){
                        Log.e(TAG,"globalQueryOutput1 = "+globalQueryOutput1);
                        matrixCursor.addRow(new String[]{myKey, globalQueryOutput1});
                        isPreviousQueryCompleted=true;
                        Log.e(TAG,"Queried global node");
                        Log.e(TAG,"isPreviousQueryCompleted=true");
                        globalQueryOutput1="";
                        return matrixCursor;
                    }
                }
                Log.v("Query", "Success in individual key query");
            }else if ("*".equalsIgnoreCase(selection) && !isLocalStorageOnly){
                Log.e(TAG,"* for global query");
                calledClientTaskForGlobalQueryStar = true;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "dummy","dummy","dummy","* for global query");

                initialGlobalQueryCompletedStar=false;
                Log.e(TAG,"initialGlobalQueryCompletedStar=false");

                try{
                    while (!initialGlobalQueryCompletedStar) Thread.sleep(100);
                }catch (InterruptedException e) {
                    Log.e("* Global Query", "InterruptedException");
                }
                if(initialGlobalQueryCompletedStar){
                    Log.e(TAG, "Inside cond11 - initialGlobalQueryCompletedStar");
                    if (valueFromGlobalNodeForAVDStar){
                        Log.e(TAG,"valueFromGlobalNodeForAVDStar:"+valueFromGlobalNodeForAVDStar);
//                        isPreviousQueryCompleted=true;
//                        Log.e(TAG,"* setting isPreviousQueryCompleted2 = true");
                        return globalStarQueryResult;
                    }
                }
            } else if((("*".equalsIgnoreCase(selection) || "@".equalsIgnoreCase(selection)) && isLocalStorageOnly)
                    || ("@".equalsIgnoreCase(selection) && !isLocalStorageOnly)){
                Log.e(TAG, "Inside 2");
                MatrixCursor matrixCursorLocal = new MatrixCursor(new String[]{"key", "value"});
                String[] fileList = context.fileList();
                Log.v(TAG,"Filelist"+fileList);
                if(fileList!=null)
                {
                    for (int i = 0; i<fileList.length; i++){
                        InputStream is = context.openFileInput(fileList[i]);
                        InputStreamReader ir = new InputStreamReader(is);
                        BufferedReader br = new BufferedReader(ir);
                        String contentsOfFile = br.readLine();
                        String [] keyValues = contentsOfFile.split("-");
                        key = keyValues[0];
                        value = keyValues[1];
                        is.close();
                        Log.v("Returing key-value",key+"-"+value);
                        matrixCursorLocal.addRow(new String[] {key, value});
                    }
//                    isPreviousQueryCompleted=true;
//                    Log.e(TAG,"isPreviousQueryCompleted=true");
//                    Log.e(TAG,"@ setting isPreviousQueryCompleted2 = true");
                    return matrixCursorLocal;
                }
            }
        } catch (NoSuchAlgorithmException e){
            Log.e("Query", "NoSuchAlgorithmException");
        }catch (FileNotFoundException e) {
            Log.v("query", "File Not Found");
        }catch (Exception e) {
            Log.v("query", "File read failed:"+e.toString());
        }
        Log.v("query", selection);
        isPreviousQueryCompleted = true;
        Log.e(TAG,"isPreviousQueryCompleted=true");
        return matrixCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        final String TAG = ServerTask.class.getSimpleName();

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try{
                while (true){
                    Socket socket = serverSocket.accept();
                    Log.i(TAG,"Connection accepted");

                    ObjectInputStream serverObjectInputStream = new ObjectInputStream(socket.getInputStream());
                    ObjectOutputStream serverObjectOutputStream = new ObjectOutputStream(socket.getOutputStream());

                    String msgType = (String) serverObjectInputStream.readObject();
                    if("Are you alive?".equalsIgnoreCase(msgType)){
                        serverObjectOutputStream.writeObject("I am alive");
                        Log.i(TAG,"Sent- I am alive from Server");
                    } else
                        if("Perform insert".equalsIgnoreCase(msgType)){
                        String key = (String) serverObjectInputStream.readObject();
                        String value = (String) serverObjectInputStream.readObject();
                        ContentValues cv = new ContentValues();
                        key = key+"--"+"true";
                        cv.put("key", key);
                        cv.put("value", value);
//                        noReplicationToDo = true;
                        insert(mUri,cv);
                        Log.i(TAG,"Performed insert into myNode");

                        serverObjectOutputStream.writeObject("ack");
                    } else if("Perform Global Query into Your Node".equalsIgnoreCase(msgType)){
                        String keyToQueryFromGlobalRequest = (String) serverObjectInputStream.readObject();
//                        String valueToSetWhere = (String) serverObjectInputStream.readObject();
                        Log.e(TAG,"keyToQueryFromGlobalRequest:"+keyToQueryFromGlobalRequest);
//                        justGlobalQuery = true;
                        MatrixCursor resultCursor = query(mUri,null,keyToQueryFromGlobalRequest,null,null);
                        Log.e("resultCursor.count:",Integer.toString(resultCursor.getCount()));
                        if(resultCursor.getCount()>0){
                            resultCursor.moveToFirst();
                            int valueIndex = resultCursor.getColumnIndex("value");
                            Log.e("valueIndex1=",Integer.toString(valueIndex));
                            Log.e("resultCursor.count:",Integer.toString(resultCursor.getCount()));
                            String returnedValue = resultCursor.getString(valueIndex);
                            Log.e("returnedValue:",returnedValue);
                            serverObjectOutputStream.writeObject(returnedValue);
                            serverObjectOutputStream.writeObject("Query Success in correctNode");
                        }
                    }else if("Perform Global Delete into Your Node".equalsIgnoreCase(msgType)){
                        String keyToInsertFromGlobalRequest = (String) serverObjectInputStream.readObject();
                            keyToInsertFromGlobalRequest = keyToInsertFromGlobalRequest+"--true";
                        delete(mUri,keyToInsertFromGlobalRequest,null);
                        serverObjectOutputStream.writeObject("Delete Success in correctNode");
                    }else if ("how many nodes alive?".equalsIgnoreCase(msgType)){
                       if(nodesInRing.contains("33d6357cfaaf0f72991b0ecd8c56da066613c089"))
                            serverObjectOutputStream.writeObject(true);
                        else
                            serverObjectOutputStream.writeObject(false);
                        if(nodesInRing.contains("208f7f72b198dadd244e61801abe1ec3a4857bc9"))
                            serverObjectOutputStream.writeObject(true);
                        else
                            serverObjectOutputStream.writeObject(false);
                        if(nodesInRing.contains("abf0fd8db03e5ecb199a9b82929e9db79b909643"))
                            serverObjectOutputStream.writeObject(true);
                        else
                            serverObjectOutputStream.writeObject(false);
                        if(nodesInRing.contains("c25ddd596aa7c81fa12378fa725f706d54325d12"))
                            serverObjectOutputStream.writeObject(true);
                        else
                            serverObjectOutputStream.writeObject(false);
                        if(nodesInRing.contains("177ccecaec32c54b82d5aaafc18a2dadb753e3b1"))
                            serverObjectOutputStream.writeObject(true);
                        else
                            serverObjectOutputStream.writeObject(false);

                        serverObjectOutputStream.writeObject("how many nodes alive? - Answered");
                        Log.e(TAG,"how many nodes alive? - Answered");
                    }else if ("Perform @ and return cursor".equalsIgnoreCase(msgType)){
                        Log.e(TAG,"Perform @ and return cursor");
                        String resultCursorToReturn = "";

                        Cursor resultCursor = query(mUri,null,"@",null,null);

                        int count = resultCursor.getCount();
                        Log.e(TAG,"count.resultCursor"+Integer.toString(count));
//                            resultCursor.moveToFirst();
                        int keyIndex = resultCursor.getColumnIndex("key");
                        Log.e(TAG,"keyIndex"+keyIndex);
                        int valueIndex = resultCursor.getColumnIndex("value");
                        Log.e(TAG,"valueIndex"+valueIndex);

                        if (count>0){
                            while (resultCursor.moveToNext()){
                                Log.e(TAG,"Inside while");
                                String key = resultCursor.getString(keyIndex);
                                Log.e(TAG,"key"+key);

                                String value = resultCursor.getString(valueIndex);
                                Log.e(TAG,"value"+value);

                                resultCursorToReturn = resultCursorToReturn+key+"$";
                                if (resultCursor.isLast()){
                                    resultCursorToReturn = resultCursorToReturn+value;
                                }else resultCursorToReturn = resultCursorToReturn+value+"$";
                                Log.e(TAG,"resultCursorToReturn#"+resultCursorToReturn);
                            }
                        }
                        serverObjectOutputStream.writeObject(resultCursorToReturn);
                        serverObjectOutputStream.writeObject("Returned individual resultCursorToReturn to requesting node");
                        Log.e(TAG,"Returned individual resultCursorToReturn to requesting node");
                    }
//                    }
                }
            }catch (SocketTimeoutException e){
                Log.e(TAG, "doInBackground");
            }
            catch (IOException e){
                Log.e(TAG, "doInBackground");
            }
            catch (ClassNotFoundException e){
                Log.e(TAG, "doInBackground");
            }
            return null;
        }
        protected void onProgressUpdate(String... strings) {
            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        String TAG = ClientTask.class.getSimpleName();
        @Override
        protected Void doInBackground(String... msgs) {
            try{
                String msg = msgs[3];
                if ("Perform global insert".equalsIgnoreCase(msg)){
                    performInsert(msgs);
                }  else if("Query Global node".equalsIgnoreCase(msg)){
                    Log.e(TAG,"Query Global node");
                    initialGlobalQueryCompleted = false;
                    Log.v(TAG,"initialGlobalQueryCompleted=false");
                    String nodeToQuery = msgs[0];
                    String originalKeyFromQuery = msgs[1];
                    String valueToSetWhere = msgs[2];
                    String correctPortNumber = "";
                    correctPortNumber = getHashToPort(nodeToQuery,correctPortNumber);
                    Log.e(TAG,"nodeToQuery = "+nodeToQuery);
                    Log.e(TAG,"correctPortNumber = "+correctPortNumber);
                    queryGlobalNode(originalKeyFromQuery, valueToSetWhere, correctPortNumber);
                } else if("Node Recovery".equalsIgnoreCase(msg)){
                    Log.i(TAG, "Node Recovery in Client");
                    String recoveryPort = msgs[1];
                    String avd0Storage="",avd1Storage="",avd2Storage="",avd3Storage="",avd4Storage="";
                    if("33d6357cfaaf0f72991b0ecd8c56da066613c089".equalsIgnoreCase(recoveryPort)){
                        Log.e(TAG,"Trying to connect to 1,2,a");
                        // 1,2,a
                        avd4Storage = askForlocalQueryAndCursor(true,REMOTE_PORT4);
                        Log.e(TAG,"avd4Storage got - Node Recovery");
                        avd1Storage = askForlocalQueryAndCursor(true,REMOTE_PORT1);
                        Log.e(TAG,"avd1Storage got - Node Recovery");
                        avd2Storage = askForlocalQueryAndCursor(true, REMOTE_PORT2);
                        Log.e(TAG,"avd2Storage got - Node Recovery");

                        createResultCursorFromStrings(avd0Storage,avd1Storage,avd2Storage,
                                avd3Storage,avd4Storage, recoveryCursor);
                    }else if ("208f7f72b198dadd244e61801abe1ec3a4857bc9".equalsIgnoreCase(recoveryPort)){
                        Log.e(TAG,"Trying to connect to 1,c,3");
                        // 1,c,3
                        avd4Storage = askForlocalQueryAndCursor(true,REMOTE_PORT4);
                        Log.e(TAG,"avd4Storage got - Node Recovery");
                        avd0Storage = askForlocalQueryAndCursor(true,REMOTE_PORT0);
                        Log.e(TAG,"avd0Storage got - Node Recovery");
                        avd3Storage = askForlocalQueryAndCursor(true, REMOTE_PORT3);
                        Log.e(TAG,"avd3Storage got - Node Recovery");

                        createResultCursorFromStrings(avd0Storage,avd1Storage,avd2Storage,
                                avd3Storage,avd4Storage, recoveryCursor);
                    }else if ("abf0fd8db03e5ecb199a9b82929e9db79b909643".equalsIgnoreCase(recoveryPort)){
                        Log.e(TAG,"Trying to connect to 3,2,c");
                        // 2,3, c
                        avd1Storage = askForlocalQueryAndCursor(true,REMOTE_PORT1);
                        Log.e(TAG,"avd1Storage got - Node Recovery");
                        avd0Storage = askForlocalQueryAndCursor(true,REMOTE_PORT0);
                        Log.e(TAG,"avd0Storage got - Node Recovery");
                        avd3Storage = askForlocalQueryAndCursor(true, REMOTE_PORT3);
                        Log.e(TAG,"avd3Storage got - Node Recovery");

                        createResultCursorFromStrings(avd0Storage,avd1Storage,avd2Storage,
                                avd3Storage,avd4Storage, recoveryCursor);
                    }else if ("c25ddd596aa7c81fa12378fa725f706d54325d12".equalsIgnoreCase(recoveryPort)){
                        Log.e(TAG,"Trying to connect to 1,3,a");
                        // 3,a, 1
                        avd2Storage = askForlocalQueryAndCursor(true,REMOTE_PORT2);
                        Log.e(TAG,"avd2Storage got - Node Recovery");
                        avd0Storage = askForlocalQueryAndCursor(true,REMOTE_PORT0);
                        Log.e(TAG,"avd0Storage got - Node Recovery");
                        avd4Storage = askForlocalQueryAndCursor(true, REMOTE_PORT4);
                        Log.e(TAG,"avd4Storage got - Node Recovery");

                        createResultCursorFromStrings(avd0Storage,avd1Storage,avd2Storage,
                                avd3Storage,avd4Storage, recoveryCursor);
                    }else if ("177ccecaec32c54b82d5aaafc18a2dadb753e3b1".equalsIgnoreCase(recoveryPort)){
                        Log.e(TAG,"Trying to connect to c,2,a");
                        // a, c, 2
                        avd2Storage = askForlocalQueryAndCursor(true,REMOTE_PORT2);
                        Log.e(TAG,"avd2Storage got - Node Recovery");
                        avd3Storage = askForlocalQueryAndCursor(true,REMOTE_PORT3);
                        Log.e(TAG,"avd3Storage got - Node Recovery");
                        avd1Storage = askForlocalQueryAndCursor(true, REMOTE_PORT1);
                        Log.e(TAG,"avd1Storage got - Node Recovery");

                        createResultCursorFromStrings(avd0Storage,avd1Storage,avd2Storage,
                                avd3Storage,avd4Storage, recoveryCursor);
                    }
                } else if("* for global query".equalsIgnoreCase(msg)){
                    Log.e(TAG,"* for global query");
                    String avd0Storage="",avd1Storage="",avd2Storage="",avd3Storage="",avd4Storage="";

                    avd0Storage = askForlocalQueryAndCursor(true,REMOTE_PORT0);
                    Log.e(TAG,"avd0Storage got");
                    avd1Storage = askForlocalQueryAndCursor(true,REMOTE_PORT1);
                    Log.e(TAG,"avd1Storage got");
                    avd2Storage = askForlocalQueryAndCursor(true, REMOTE_PORT2);
                    Log.e(TAG,"avd2Storage got");
                    avd3Storage = askForlocalQueryAndCursor(true, REMOTE_PORT3);
                    Log.e(TAG,"avd3Storage got");
                    avd4Storage = askForlocalQueryAndCursor(true, REMOTE_PORT4);
                    Log.e(TAG,"avd4Storage got");

                    createResultCursorFromStrings(avd0Storage,avd1Storage,avd2Storage,
                            avd3Storage,avd4Storage, globalStarQueryResult);

                }else if ("Delete Request".equalsIgnoreCase(msg)){
                    Log.e(TAG,"Delete Request");
                    initialGlobalDeleteCompleted = false;
                    Log.v(TAG,"initialGlobalDeleteCompleted=false");
                    String keyHashFromQuery = msgs[0];
                    String originalKeyFromQuery = msgs[1];
                    String NodeWhereDelete = msgs[2];

                    if ("33d6357cfaaf0f72991b0ecd8c56da066613c089".equalsIgnoreCase(NodeWhereDelete) ||
                            "208f7f72b198dadd244e61801abe1ec3a4857bc9".equalsIgnoreCase(NodeWhereDelete) ||
                            "abf0fd8db03e5ecb199a9b82929e9db79b909643".equalsIgnoreCase(NodeWhereDelete) ||
                            "c25ddd596aa7c81fa12378fa725f706d54325d12".equalsIgnoreCase(NodeWhereDelete) ||
                            "177ccecaec32c54b82d5aaafc18a2dadb753e3b1".equalsIgnoreCase(NodeWhereDelete) ){

                        String correctNode = "";
                        correctNode = getHashToPort(NodeWhereDelete, correctNode);
                        Socket socketForInsertToGlobalNode = new Socket
                                (InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(correctNode));
                        ObjectOutputStream objectOutputStreamForInsertToGlobalNode = new ObjectOutputStream(socketForInsertToGlobalNode.getOutputStream());
                        objectOutputStreamForInsertToGlobalNode.writeObject("Perform Global Delete into Your Node");
                        Log.e(TAG,"Perform Global Delete into Your Node");
                        objectOutputStreamForInsertToGlobalNode.writeObject(originalKeyFromQuery);
                        Log.e(TAG,"originalKeyFromDelete="+originalKeyFromQuery);
//                    objectOutputStreamForInsertToGlobalNode.writeObject(valueFromQuery);
                        objectOutputStreamForInsertToGlobalNode.flush();

                        ObjectInputStream inputStreamForInsertToGlobalNode = new ObjectInputStream(socketForInsertToGlobalNode.getInputStream());
                        String ackForInsertToGlobalNode = (String) inputStreamForInsertToGlobalNode.readObject();
                        if (ackForInsertToGlobalNode!=null && !ackForInsertToGlobalNode.isEmpty()
                                && "Delete Success in correctNode".equalsIgnoreCase(ackForInsertToGlobalNode)){
                            Log.e(TAG,"Delete Success in correctNode");
                            calledClientTaskForGlobalDelete=false;
                            Log.e(TAG, "calledClientTaskForGlobalQuery=false");
                            Log.e(TAG, "initialGlobalQueryCompleted=true");
                            initialGlobalDeleteCompleted = true;
                            objectOutputStreamForInsertToGlobalNode.close();
                            socketForInsertToGlobalNode.close();
                        }

                    }
////                    Ask AVD0 for correct position
//                    Socket socketForInsertLookUptoAvd0 = new Socket
//                            (InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0));
//                    ObjectOutputStream outputStreamForInsertLookUptoAvd0 = new ObjectOutputStream(socketForInsertLookUptoAvd0.getOutputStream());
//                    outputStreamForInsertLookUptoAvd0.writeObject("Insert Request - Lookup");
//                    outputStreamForInsertLookUptoAvd0.writeObject(keyHashFromQuery);
//                    outputStreamForInsertLookUptoAvd0.flush();
//
//                    ObjectInputStream inputStreamForInsertLookUptoAvd0 = new ObjectInputStream(socketForInsertLookUptoAvd0.getInputStream());
//                    String correctNode = (String) inputStreamForInsertLookUptoAvd0.readObject();
//                    String ack = (String) inputStreamForInsertLookUptoAvd0.readObject();
//                    if (ack!=null && !ack.isEmpty() && "LookUp done - Returned correctNode".equalsIgnoreCase(ack)){
//                        Log.e(TAG,"LookUp done - Returned correctNode for Query");
//                        Log.e(TAG,"Returned correctNode:"+correctNode);
//                        outputStreamForInsertLookUptoAvd0.close();
//                        socketForInsertLookUptoAvd0.close();
//                    }
//                    String correctPortNumber = "";
//                    correctPortNumber = getHashToPort(correctNode,correctPortNumber);
//
//                    Socket socketForInsertToGlobalNode = new Socket
//                            (InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(correctPortNumber));
//                    ObjectOutputStream objectOutputStreamForInsertToGlobalNode = new ObjectOutputStream(socketForInsertToGlobalNode.getOutputStream());
//                    objectOutputStreamForInsertToGlobalNode.writeObject("Perform Global Delete into Your Node");
//                    Log.e(TAG,"Perform Global Delete into Your Node");
//                    objectOutputStreamForInsertToGlobalNode.writeObject(originalKeyFromQuery);
//                    Log.e(TAG,"originalKeyFromDelete="+originalKeyFromQuery);
////                    objectOutputStreamForInsertToGlobalNode.writeObject(valueFromQuery);
//                    objectOutputStreamForInsertToGlobalNode.flush();
//
//                    ObjectInputStream inputStreamForInsertToGlobalNode = new ObjectInputStream(socketForInsertToGlobalNode.getInputStream());
//                    String ackForInsertToGlobalNode = (String) inputStreamForInsertToGlobalNode.readObject();
//                    if (ackForInsertToGlobalNode!=null && !ackForInsertToGlobalNode.isEmpty()
//                            && "Delete Success in correctNode".equalsIgnoreCase(ackForInsertToGlobalNode)){
//                        Log.e(TAG,"Delete Success in correctNode");
//                        calledClientTaskForGlobalDelete=false;
//                        Log.e(TAG, "calledClientTaskForGlobalQuery=false");
//                        Log.e(TAG, "initialGlobalQueryCompleted=true");
//                        initialGlobalDeleteCompleted = true;
//                        objectOutputStreamForInsertToGlobalNode.close();
//                        socketForInsertToGlobalNode.close();
//                    }
                }
            }catch (SocketTimeoutException e){
                Log.e(TAG, "doInBackground-SocketTimeoutException");
            }catch (UnknownHostException e){
                Log.e(TAG, "doInBackground-UnknownHostException");
            }catch (StreamCorruptedException e){
//                try{
//                    String nodeValue = genHash(Integer.toString(Integer.parseInt(globalMyAvdIDHash)/2)) ;
//                    node = new MyNode(nodeValue,null,null);}catch (NoSuchAlgorithmException e1){
//                    Log.e(TAG,"NoSuchAlgorithmException inside IOException");
//                }
//                isLocalStorageOnly=true;
//                Log.i(TAG, "isLocalStorageOnly = true");
                Log.e(TAG, "doInBackground - StreamCorruptedException");
            } catch (IOException e){
//                try{
//                    String nodeValue = genHash(Integer.toString(Integer.parseInt(globalMyAvdIDHash)/2)) ;
//                    node = new MyNode(nodeValue,null,null);}catch (NoSuchAlgorithmException e1){
//                    Log.e(TAG,"NoSuchAlgorithmException inside IOException");
//                }
//                isLocalStorageOnly=true;
//                Log.i(TAG, "isLocalStorageOnly = true");
                Log.e(TAG, "doInBackground - IOException");
            }catch (ClassNotFoundException e){
                Log.e(TAG, "doInBackground-ClassNotFoundException");
            }
            return null;
        }

        private void performInsert(String[] msgs) throws IOException, ClassNotFoundException {
            Log.i(TAG, "Perform global insert in Client");
            String port1 = msgs[0];
            String key = msgs[1];
            String value = msgs[2];
            if (port1!=null){
                String portToConnect = "";
                portToConnect = getHashToPort(port1, portToConnect);
                Socket socket = new Socket
                        (InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portToConnect));
                Log.i(TAG, "Connected to port = "+portToConnect);
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                outputStream.writeObject("Perform insert");
                outputStream.writeObject(key);
                outputStream.writeObject(value);
                outputStream.flush();

                ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                String ack = (String) objectInputStream.readObject();
                Log.i(TAG, "Received ack = "+ack);
                if (ack!=null && !ack.isEmpty() && "ack".equalsIgnoreCase(ack)){
                    outputStream.close();
                    socket.close();
                }
            }
            Log.i(TAG, "Completed global insert in Client");
        }

        private void queryGlobalNode(String originalKeyFromQuery, String valueToSetWhere, String correctPortNumber) throws ClassNotFoundException {
            try{
                Socket socketForInsertToGlobalNode = new Socket();
                socketForInsertToGlobalNode.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(correctPortNumber)),300);
//                    Socket socketForInsertToGlobalNode = new Socket
//                            (InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(correctPortNumber));
                ObjectOutputStream objectOutputStreamForInsertToGlobalNode = new ObjectOutputStream(socketForInsertToGlobalNode.getOutputStream());
                objectOutputStreamForInsertToGlobalNode.writeObject("Perform Global Query into Your Node");
                Log.e(TAG,"originalKeyFromQuery1 = "+originalKeyFromQuery);
                Log.e(TAG,"Perform Global Query into Your Node");
                objectOutputStreamForInsertToGlobalNode.writeObject(originalKeyFromQuery);
//                    objectOutputStreamForInsertToGlobalNode.writeObject(valueToSetWhere);
                objectOutputStreamForInsertToGlobalNode.flush();
                Log.e(TAG,"Write complete to server for Global query");

                ObjectInputStream inputStreamForInsertToGlobalNode = new ObjectInputStream(socketForInsertToGlobalNode.getInputStream());
//                socketForInsertToGlobalNode.setSoTimeout(200);
                String valueFromGlobalNode = (String) inputStreamForInsertToGlobalNode.readObject();
                String ackForInsertToGlobalNode = (String) inputStreamForInsertToGlobalNode.readObject();
                Log.e(TAG,"Read complete from server for Global query");
                if (ackForInsertToGlobalNode!=null && !ackForInsertToGlobalNode.isEmpty()
                        && "Query Success in correctNode".equalsIgnoreCase(ackForInsertToGlobalNode)){
                    Log.e(TAG,"Query Success in correctNode");
                    calledClientTaskForGlobalQuery=false;
                    Log.e(TAG, "calledClientTaskForGlobalQuery=false");
                    Log.e(TAG, "initialGlobalQueryCompleted=true");
                    initialGlobalQueryCompleted = true;
                    Log.e(TAG, "valueFromGlobalNode:"+valueFromGlobalNode);

                    if ("original".equalsIgnoreCase(valueToSetWhere)){
                        globalQueryOutput1 = valueFromGlobalNode;
                        Log.e(TAG,"globalQueryOutput1 = "+globalQueryOutput1);
                    }
                    objectOutputStreamForInsertToGlobalNode.close();
                    socketForInsertToGlobalNode.close();
                }
            }catch(SocketTimeoutException e){
                Log.e(TAG,"SocketTimeoutException - CorrectNode is dead");
                queryDeadNodesSuccessor(originalKeyFromQuery, correctPortNumber);
            }catch(StreamCorruptedException e){
                Log.e(TAG,"StreamCorruptedException - CorrectNode is dead");
                queryDeadNodesSuccessor(originalKeyFromQuery, correctPortNumber);
            }catch(IOException e){
                Log.e(TAG,"IOException - CorrectNode is dead");
                queryDeadNodesSuccessor(originalKeyFromQuery, correctPortNumber);
            }
        }

        private void createResultCursorFromStrings(String avd0Storage, String avd1Storage, String avd2Storage, String avd3Storage, String avd4Storage, MatrixCursor globalStarQueryResult) {
            MatrixCursor newMatrixCursor = new MatrixCursor(new String[]{"key", "value"});

            Log.e(TAG, "Inside createResultCursorFromStrings");
            giveStringAddedToCursor(avd0Storage, newMatrixCursor,globalStarQueryResult);
            Log.e(TAG, "Inside createResultCursorFromStrings + avd0Storage");
            giveStringAddedToCursor(avd1Storage, newMatrixCursor,globalStarQueryResult);
            Log.e(TAG, "Inside createResultCursorFromStrings + avd1Storage");
            giveStringAddedToCursor(avd2Storage, newMatrixCursor,globalStarQueryResult);
            Log.e(TAG, "Inside createResultCursorFromStrings+ avd2Storage");
            giveStringAddedToCursor(avd3Storage, newMatrixCursor,globalStarQueryResult);
            Log.e(TAG, "Inside createResultCursorFromStrings+ avd3Storage");
            giveStringAddedToCursor(avd4Storage, newMatrixCursor,globalStarQueryResult);
            Log.e(TAG, "Inside createResultCursorFromStrings+ avd4Storage");

            Log.e(TAG,"newMatrixCursor.getCount() = "+newMatrixCursor.getCount());
            globalStarQueryResult = newMatrixCursor;
            valueFromGlobalNodeForAVDStar = true;
            initialGlobalQueryCompletedStar=true;
            recoveryCursorSet = true;
            Log.e(TAG,"globalStarQueryResult is set");
            Log.e(TAG,"valueFromGlobalNodeForAVDStar=true");
        }

        private void giveStringAddedToCursor(String avd0Storage, MatrixCursor newMatrixCursor, MatrixCursor globalStarQueryResult) {
            if (avd0Storage!=null && !avd0Storage.isEmpty()){
                Log.e(TAG,"From AVD0 @"+avd0Storage);
                String avd0StorageKeysValues[] = avd0Storage.split("\\$");
                int x = avd0StorageKeysValues.length;
                Log.e(TAG,"avd0StorageKeysValues.length="+x);
                if (x%2==0){
                    Log.e(TAG,"Correct number of keys and values received in createResultCursorFromStrings");
                    for (int i=0;i+1<x;i=i+2){
                        String key = avd0StorageKeysValues[i];
                        Log.e(TAG,"giveStringAddedToCursor:Key="+key);
                        String value = avd0StorageKeysValues[i+1];
                        Log.e(TAG,"giveStringAddedToCursor:Value="+value);
                        newMatrixCursor.addRow(new String[] {key,value});
                        globalStarQueryResult.addRow(new String[] {key,value});
                        Log.e(TAG,"Adding finally:"+key+"-"+"Value:"+value);
                    }
                }

            }
        }

        private String askForlocalQueryAndCursor(Boolean avd0Alive, String remortPort)  {
            Log.e(TAG,"askForlocalQueryAndCursor");
            if (avd0Alive){
                try{
//                    Socket socketAvd0 = new Socket
//                        (InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remortPort));
//                    socketAvd0.setSoTimeout(100);
                    Socket socketAvd0 = new Socket();
                    socketAvd0.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remortPort)),300);
                    ObjectOutputStream outputStreamAvd0 = new ObjectOutputStream(socketAvd0.getOutputStream());
                    outputStreamAvd0.writeObject("Perform @ and return cursor");
                    outputStreamAvd0.flush();

                    ObjectInputStream objectInputStream0 = new ObjectInputStream(socketAvd0.getInputStream());
                    String localQueryResult = (String) objectInputStream0.readObject();
                    Log.e(TAG,"Returning query result as"+localQueryResult.toString());

                    String ack = (String) objectInputStream0.readObject();
                    if (ack!=null && !ack.isEmpty() && "Returned individual resultCursorToReturn to requesting node".equalsIgnoreCase(ack)){
                        Log.e(TAG,"Returned individual resultCursorToReturn to requesting node");
                        socketAvd0.close();
                    }
                    return localQueryResult;
                }catch (SocketTimeoutException e){
                    Log.e(TAG,"SocketTimeoutException in askForlocalQueryAndCursor");
                    return null;
                }catch (ClassNotFoundException e){
                    Log.e(TAG,"ClassNotFoundException in askForlocalQueryAndCursor");
                }catch (IOException e){
                    Log.e(TAG,"IOException in askForlocalQueryAndCursor");
                    return null;
                }catch (Exception e){
                    Log.e(TAG,"Exception in askForlocalQueryAndCursor");
//                    return null;
                }
            }
            return null;
        }
    }

    private void queryDeadNodesSuccessor(String originalKeyFromQuery, String correctPortNumber) {
        String TAG = "queryDeadNodesSuccessor";
            if (REMOTE_PORT0.equalsIgnoreCase(correctPortNumber)){
                Log.e(TAG,"AVD0 is dead - calling client for AVD2");
//                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "abf0fd8db03e5ecb199a9b82929e9db79b909643",
//                        originalKeyFromQuery,"original","Query Global node");

                querySuccSucc(REMOTE_PORT2, REMOTE_PORT3, originalKeyFromQuery);
            } else if (REMOTE_PORT1.equalsIgnoreCase(correctPortNumber)){
                Log.e(TAG,"AVD1 is dead - calling client for AVD0");
//                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "33d6357cfaaf0f72991b0ecd8c56da066613c089",
//                        originalKeyFromQuery,"original","Query Global node");
                querySuccSucc(REMOTE_PORT0, REMOTE_PORT2, originalKeyFromQuery);
                Log.e(TAG, "Successor - AVD0 returned result");
            }else if (REMOTE_PORT2.equalsIgnoreCase(correctPortNumber)){
                Log.e(TAG,"AVD2 is dead - calling client for AVD3");
//                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "c25ddd596aa7c81fa12378fa725f706d54325d12",
//                        originalKeyFromQuery,"original","Query Global node");
                querySuccSucc(REMOTE_PORT3, REMOTE_PORT4, originalKeyFromQuery);
                Log.e(TAG, "Successor - AVD3 returned result");
            }else if (REMOTE_PORT3.equalsIgnoreCase(correctPortNumber)){
                Log.e(TAG,"AVD3 is dead - calling client for AVD4");
//                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "177ccecaec32c54b82d5aaafc18a2dadb753e3b1",
//                        originalKeyFromQuery,"original","Query Global node");
                querySuccSucc(REMOTE_PORT4, REMOTE_PORT1, originalKeyFromQuery);
                Log.e(TAG, "Successor - AVD4 returned result");
            }else if (REMOTE_PORT4.equalsIgnoreCase(correctPortNumber)){
                Log.e(TAG,"AVD4 is dead - calling client for AVD1");
//                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "208f7f72b198dadd244e61801abe1ec3a4857bc9",
//                        originalKeyFromQuery,"original","Query Global node");
                querySuccSucc(REMOTE_PORT1, REMOTE_PORT0, originalKeyFromQuery);
                Log.e(TAG, "Successor - AVD1 returned result");
            }
    }

    private void querySuccSucc(String port1,String port2,String originalKeyFromQuery) {
        String TAG = "querySuccSucc";
    try{
            Socket socketForInsertToGlobalNode = new Socket();
            socketForInsertToGlobalNode.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port1)),300);
            ObjectOutputStream objectOutputStreamForInsertToGlobalNode = new ObjectOutputStream(socketForInsertToGlobalNode.getOutputStream());
            objectOutputStreamForInsertToGlobalNode.writeObject("Perform Global Query into Your Node");
            Log.e(TAG,"originalKeyFromQuery1 = "+originalKeyFromQuery);
            Log.e(TAG,"Perform Global Query into Your Node");
            objectOutputStreamForInsertToGlobalNode.writeObject(originalKeyFromQuery);
            objectOutputStreamForInsertToGlobalNode.flush();
            Log.e(TAG,"Write complete to server for Global query");

            ObjectInputStream inputStreamForInsertToGlobalNode = new ObjectInputStream(socketForInsertToGlobalNode.getInputStream());
//            socketForInsertToGlobalNode.setSoTimeout(200);
            String valueFromGlobalNode = (String) inputStreamForInsertToGlobalNode.readObject();
            String ackForInsertToGlobalNode = (String) inputStreamForInsertToGlobalNode.readObject();
            Log.e(TAG,"Read complete from server for Global query");
            if (ackForInsertToGlobalNode!=null && !ackForInsertToGlobalNode.isEmpty()
                    && "Query Success in correctNode".equalsIgnoreCase(ackForInsertToGlobalNode)){
                Log.e(TAG,"Query Success in correctNode");
                calledClientTaskForGlobalQuery=false;
                Log.e(TAG, "calledClientTaskForGlobalQuery=false");
                Log.e(TAG, "initialGlobalQueryCompleted=true");
                initialGlobalQueryCompleted = true;
                Log.e(TAG, "valueFromGlobalNode:"+valueFromGlobalNode);

//                        if ("original".equalsIgnoreCase(valueToSetWhere)){
                    globalQueryOutput1 = valueFromGlobalNode;
                    Log.e(TAG,"globalQueryOutput1 = "+globalQueryOutput1);
//                        }
                objectOutputStreamForInsertToGlobalNode.close();
                socketForInsertToGlobalNode.close();
            }
            Log.e(TAG, "Successor - AVD2 returned result");
    }catch (ClassNotFoundException e){

        } catch (SocketTimeoutException e){
            try{
                Socket socketForInsertToGlobalNode = new Socket();
                socketForInsertToGlobalNode.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port2)),300);
                ObjectOutputStream objectOutputStreamForInsertToGlobalNode = new ObjectOutputStream(socketForInsertToGlobalNode.getOutputStream());
                objectOutputStreamForInsertToGlobalNode.writeObject("Perform Global Query into Your Node");
                Log.e(TAG,"originalKeyFromQuery1 = "+originalKeyFromQuery);
                Log.e(TAG,"Perform Global Query into Your Node");
                objectOutputStreamForInsertToGlobalNode.writeObject(originalKeyFromQuery);
                objectOutputStreamForInsertToGlobalNode.flush();
                Log.e(TAG,"Write complete to server for Global query");

                ObjectInputStream inputStreamForInsertToGlobalNode = new ObjectInputStream(socketForInsertToGlobalNode.getInputStream());
//                socketForInsertToGlobalNode.setSoTimeout(200);
                String valueFromGlobalNode = (String) inputStreamForInsertToGlobalNode.readObject();
                String ackForInsertToGlobalNode = (String) inputStreamForInsertToGlobalNode.readObject();
                Log.e(TAG,"Read complete from server for Global query");
                if (ackForInsertToGlobalNode!=null && !ackForInsertToGlobalNode.isEmpty()
                        && "Query Success in correctNode".equalsIgnoreCase(ackForInsertToGlobalNode)){
                    Log.e(TAG,"Query Success in correctNode");
                    calledClientTaskForGlobalQuery=false;
                    Log.e(TAG, "calledClientTaskForGlobalQuery=false");
                    Log.e(TAG, "initialGlobalQueryCompleted=true");
                    initialGlobalQueryCompleted = true;
                    Log.e(TAG, "valueFromGlobalNode:"+valueFromGlobalNode);
                    globalQueryOutput1 = valueFromGlobalNode;
                    Log.e(TAG,"globalQueryOutput1 = "+globalQueryOutput1);
                    objectOutputStreamForInsertToGlobalNode.close();
                    socketForInsertToGlobalNode.close();
                }
                Log.e(TAG, "Successor - AVD2 returned result");
            }catch (ClassNotFoundException e1){

            } catch (SocketTimeoutException e1){

            }
            catch (IOException e1){

            }
        }
    catch (IOException e){
        try{
            Socket socketForInsertToGlobalNode = new Socket();
            socketForInsertToGlobalNode.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port2)),300);
            ObjectOutputStream objectOutputStreamForInsertToGlobalNode = new ObjectOutputStream(socketForInsertToGlobalNode.getOutputStream());
            objectOutputStreamForInsertToGlobalNode.writeObject("Perform Global Query into Your Node");
            Log.e(TAG,"originalKeyFromQuery1 = "+originalKeyFromQuery);
            Log.e(TAG,"Perform Global Query into Your Node");
            objectOutputStreamForInsertToGlobalNode.writeObject(originalKeyFromQuery);
            objectOutputStreamForInsertToGlobalNode.flush();
            Log.e(TAG,"Write complete to server for Global query");

            ObjectInputStream inputStreamForInsertToGlobalNode = new ObjectInputStream(socketForInsertToGlobalNode.getInputStream());
//            socketForInsertToGlobalNode.setSoTimeout(200);
            String valueFromGlobalNode = (String) inputStreamForInsertToGlobalNode.readObject();
            String ackForInsertToGlobalNode = (String) inputStreamForInsertToGlobalNode.readObject();
            Log.e(TAG,"Read complete from server for Global query");
            if (ackForInsertToGlobalNode!=null && !ackForInsertToGlobalNode.isEmpty()
                    && "Query Success in correctNode".equalsIgnoreCase(ackForInsertToGlobalNode)){
                Log.e(TAG,"Query Success in correctNode");
                calledClientTaskForGlobalQuery=false;
                Log.e(TAG, "calledClientTaskForGlobalQuery=false");
                Log.e(TAG, "initialGlobalQueryCompleted=true");
                initialGlobalQueryCompleted = true;
                Log.e(TAG, "valueFromGlobalNode:"+valueFromGlobalNode);
                globalQueryOutput1 = valueFromGlobalNode;
                Log.e(TAG,"globalQueryOutput1 = "+globalQueryOutput1);
                objectOutputStreamForInsertToGlobalNode.close();
                socketForInsertToGlobalNode.close();
            }
            Log.e(TAG, "Successor - AVD2 returned result");
        }catch (ClassNotFoundException e2){

        } catch (SocketTimeoutException e2){

        }
        catch (IOException e2){

        }
        }
    }

    private class MyNode {
        private String hashValue;
        private String next;
        private String prev;

        public String getNext() {
            return next;
        }

        public void setNext(String next) {
            this.next = next;
        }

        public String getPrev() {
            return prev;
        }

        public void setPrev(String prev) {
            this.prev = prev;
        }

        public String getHashValue() {
            return hashValue;
        }

        public void setHashValue(String hashValue) {
            this.hashValue = hashValue;
        }

        MyNode(){}

        MyNode(String hashValue, String next, String prev){
            this.hashValue = hashValue;
            this.next = next;
            this.prev = prev;
        }
    }
    private String getHashToPort(String next, String updatesToNextPort) {
        try{
            if (genHash("5554").equalsIgnoreCase(next))
                return REMOTE_PORT0;
            else if (genHash("5556").equalsIgnoreCase(next))
                return REMOTE_PORT1;
            else if (genHash("5558").equalsIgnoreCase(next))
                return REMOTE_PORT2;
            else if (genHash("5560").equalsIgnoreCase(next))
                return REMOTE_PORT3;
            else if (genHash("5562").equalsIgnoreCase(next))
                return REMOTE_PORT4;
        }catch (NoSuchAlgorithmException e){
            Log.e("getHashToPort", "NoSuchAlgorithmException");
        }
        return null;
    }
//    private void checkAndUpdateAliveNodes(String portNumber, String hashedValue){
//        try{
//            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portNumber));
//            ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
//            outputStream.writeObject("Are you alive?");
//            ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
//            String aliveAck = (String) inputStream.readObject();
//            socket.setSoTimeout(3000);
//            if(aliveAck!=null && !aliveAck.isEmpty() && aliveAck.equalsIgnoreCase("I am alive")){
//                outputStream.close();
//                inputStream.close();
//                socket.close();
//                nodesInRing.add(hashedValue);
//                Collections.sort(nodesInRing);
//                Log.i(TAG, "nodesInRing updated with - "+hashedValue);
//            }
//        }catch (UnknownHostException e){
//            Log.e(TAG,"UnknownHostException in checkAndUpdateAliveNodes");
//        }catch (SocketTimeoutException e){
//            Log.e(TAG,"SocketTimeoutException in checkAndUpdateAliveNodes");
//        }catch (IOException e){
//            Log.e(TAG,"IOException in checkAndUpdateAliveNodes");
//        }catch (ClassNotFoundException e){
//            Log.e(TAG,"ClassNotFoundException in checkAndUpdateAliveNodes");
//        }
//    }
}