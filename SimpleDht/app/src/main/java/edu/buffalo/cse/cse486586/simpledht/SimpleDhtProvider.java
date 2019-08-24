package edu.buffalo.cse.cse486586.simpledht;

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
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Formatter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import android.annotation.SuppressLint;
import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.nfc.Tag;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

public class SimpleDhtProvider extends ContentProvider {

    String globalMyPort = "";
    Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    Boolean isLocalStorageOnly = false;
    Boolean initialGlobalInsertCompleted = true;
    Boolean calledClientTaskForGlobalInsert = false;
    Boolean initialGlobalQueryCompleted = true;
    Boolean calledClientTaskForGlobalQuery = false;
    String valueFromGlobalNodeForAVD = "";
    MyNode node = new MyNode();
    static final int SERVER_PORT = 10000;
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    String TAG = SimpleDhtProvider.class.getSimpleName();
    ArrayList<String> nodesInRing = new ArrayList<String>();
    Boolean calledClientTaskForGlobalQueryStar=false;
    Boolean initialGlobalQueryCompletedStar=true;
    Boolean valueFromGlobalNodeForAVDStar = false;
    MatrixCursor globalStarQueryResult = new MatrixCursor(new String[]{"key", "value"});
    Boolean calledClientTaskForGlobalDelete = false;
    Boolean initialGlobalDeleteCompleted=true;


    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }
    @SuppressLint("LongLogTag")
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
        String key = "";
        String value = "";
        FileInputStream inputStream;

        Context context = getContext();

        try {
            String currentKeyHash = genHash(selection);

            if (!"*".equalsIgnoreCase(selection) && !"@".equalsIgnoreCase(selection)) {
                Log.e(TAG, "Inside 1");
                Log.e("currentKeyHash",currentKeyHash);
                Log.e("node.getHashValue()",node.getHashValue());
                if((node.getHashValue().compareTo(currentKeyHash)>0 && node.getPrev()!=null && !node.getPrev().isEmpty() &&
                        node.getPrev().compareTo(currentKeyHash)<0)){
                    Log.e("key in Local storage","Normal range");
                    InputStream is = context.openFileInput(currentKeyHash);
                    InputStreamReader ir = new InputStreamReader(is);
                    BufferedReader br = new BufferedReader(ir);
                    String contentsOfFile = br.readLine();
                    String [] keyValues = contentsOfFile.split("-");
                    key = keyValues[0];
                    value = keyValues[1];
                    is.close();
                    matrixCursor.addRow(new String[] {key, value});
                } else if(node.getHashValue().compareTo(currentKeyHash)<0 && node.getPrev()!=null && !node.getPrev().isEmpty() &&
                        node.getPrev().compareTo(currentKeyHash)<0 && node.getPrev().compareTo(node.getHashValue())>0){
                    Log.e("key in Local storage","default node 0");
                    InputStream is = context.openFileInput(currentKeyHash);
                    InputStreamReader ir = new InputStreamReader(is);
                    BufferedReader br = new BufferedReader(ir);
                    String contentsOfFile = br.readLine();
                    String [] keyValues = contentsOfFile.split("-");
                    key = keyValues[0];
                    value = keyValues[1];
                    is.close();
                    matrixCursor.addRow(new String[] {key, value});
                } else if(node.getHashValue().compareTo(currentKeyHash)>0 && node.getPrev()!=null && !node.getPrev().isEmpty() &&
                        node.getPrev().compareTo(currentKeyHash)>0 && node.getPrev().compareTo(node.getHashValue())>0){
                    Log.e("key in Local storage","wrap around condition");
                    InputStream is = context.openFileInput(currentKeyHash);
                    InputStreamReader ir = new InputStreamReader(is);
                    BufferedReader br = new BufferedReader(ir);
                    String contentsOfFile = br.readLine();
                    String [] keyValues = contentsOfFile.split("-");
                    key = keyValues[0];
                    value = keyValues[1];
                    is.close();
                    matrixCursor.addRow(new String[] {key, value});
                }else if(node.getPrev()==null && node.getNext()==null && isLocalStorageOnly){
                    Log.e("key in Local storage","isLocalStorageOnly=true");
                    InputStream is = context.openFileInput(currentKeyHash);
                    InputStreamReader ir = new InputStreamReader(is);
                    BufferedReader br = new BufferedReader(ir);
                    String contentsOfFile = br.readLine();
                    String [] keyValues = contentsOfFile.split("-");
                    key = keyValues[0];
                    value = keyValues[1];
                    is.close();
                    matrixCursor.addRow(new String[] {key, value});
                } else {
                    Log.e("Global node for Query:","keyhash:"+currentKeyHash+"-"+"key:"+key);
                    if(initialGlobalQueryCompleted){
                        Log.e(TAG, "Inside cond1");
                        if (valueFromGlobalNodeForAVD!=null && !valueFromGlobalNodeForAVD.isEmpty()){
                            Log.e(TAG,"valueFromGlobalNodeForAVD:"+valueFromGlobalNodeForAVD+"-"+"key:"+selection);
                            matrixCursor.addRow(new String[] {selection, valueFromGlobalNodeForAVD});
                        }
                        Log.e(TAG,initialGlobalQueryCompleted.toString());
                        Log.e(TAG,"Calling clientTask for Query Request - Lookup");
                        calledClientTaskForGlobalQuery = true;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, currentKeyHash,selection,"dummy","Query Request - Lookup");
                        try{
                            if (calledClientTaskForGlobalQuery) Thread.sleep(1000);
                        }catch (InterruptedException e) {
                            Log.e("Query", "InterruptedException");
                        }
                        if(initialGlobalQueryCompleted){
                            Log.e(TAG, "Inside cond11");
                            if (valueFromGlobalNodeForAVD!=null && !valueFromGlobalNodeForAVD.isEmpty()){
                                Log.e(TAG,"valueFromGlobalNodeForAVD1:"+valueFromGlobalNodeForAVD+"-"+"key1:"+selection);
                                matrixCursor.addRow(new String[] {selection, valueFromGlobalNodeForAVD});
                                return matrixCursor;
                            }
                        }
                    }
                }
                Log.v("Query", "Success in individual key query");
            }else if ("*".equalsIgnoreCase(selection) && !isLocalStorageOnly){
                Log.e(TAG,"* for global query");
                calledClientTaskForGlobalQueryStar = true;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "dummy","dummy","dummy","* for global query");

                try{
                    if (calledClientTaskForGlobalQueryStar) Thread.sleep(10000);
                }catch (InterruptedException e) {
                    Log.e("* Global Query", "InterruptedException");
                }
                if(initialGlobalQueryCompletedStar){
                    Log.e(TAG, "Inside cond11 - initialGlobalQueryCompletedStar");
                    if (valueFromGlobalNodeForAVDStar){
                        Log.e(TAG,"valueFromGlobalNodeForAVDStar:"+valueFromGlobalNodeForAVDStar);
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

//        try{
//            if (calledClientTaskForGlobalQueryStar) Thread.sleep(10000);
//        }catch (InterruptedException e) {
//            Log.e("* Global Query", "InterruptedException");
//        }
        Log.v("query", selection);
        return matrixCursor;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        try{
            if (calledClientTaskForGlobalDelete) Thread.sleep(1000);
        }catch (InterruptedException e) {
            Log.e("delete", "InterruptedException");
        }
        if (node.prev==null && node.next==null) isLocalStorageOnly=true;
        Context context = getContext();
        try{
            String currentKeyHash = genHash(selection);

            if(node.getNext()==null && node.getPrev()==null && isLocalStorageOnly){
                Log.e(TAG,"isLocalStorageOnly"+"=true");
                InputStream is = context.openFileInput(currentKeyHash);
                if (is!=null) {
                    File x = context.getFilesDir();
                    File actualFile = new File(x, currentKeyHash);
                    actualFile.delete();
                    }
                } else if((node.getHashValue().compareTo(currentKeyHash)>0 && node.getPrev()!=null && !node.getPrev().isEmpty() &&
                    node.getPrev().compareTo(currentKeyHash)<0)){
                Log.e("key in Local storage","Normal range");

                if (deleteMyFile(context, currentKeyHash)) return 1;
            }else if(node.getHashValue().compareTo(currentKeyHash)<0 && node.getPrev()!=null && !node.getPrev().isEmpty() &&
                    node.getPrev().compareTo(currentKeyHash)<0 && node.getPrev().compareTo(node.getHashValue())>0){
                Log.e(TAG,"Cond2");
                if (deleteMyFile(context, currentKeyHash)) return 1;
            } else if(node.getHashValue().compareTo(currentKeyHash)>0 && node.getPrev()!=null && !node.getPrev().isEmpty() &&
                    node.getPrev().compareTo(currentKeyHash)>0 && node.getPrev().compareTo(node.getHashValue())>0) {
                Log.e(TAG,"Cond3");
                if (deleteMyFile(context, currentKeyHash)) return 1;
            } else {
                Log.e("Global node to delete","keyhash:"+currentKeyHash+"key"+selection);
                if(initialGlobalDeleteCompleted){
                    Log.e(TAG,initialGlobalDeleteCompleted.toString());
                    Log.e(TAG,"Calling clientTask for delete Request");
                    calledClientTaskForGlobalDelete = true;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, currentKeyHash,selection,"dummy","Delete Request");
                }
            }
        }catch (NoSuchAlgorithmException e){
            Log.e(TAG,"NoSuchAlgorithmException in delete");
        }catch (FileNotFoundException e){
            Log.e(TAG,"FileNotFoundException in delete");
        }return 0;
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

    @SuppressLint("LongLogTag")
    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        try{
            if (calledClientTaskForGlobalInsert) Thread.sleep(1000);
        }catch (InterruptedException e) {
            Log.e("insert", "InterruptedException");
        }

//        For avd0 local storage
        if (node.prev==null && node.next==null) isLocalStorageOnly=true;

        Context context = getContext();
//        Iterating over the content values to get the <key,Value> pairs for insertion
        Set<String> keySet = values.keySet();
        Iterator<String> iteratorKeys = keySet.iterator();
        String x = "";
        while(iteratorKeys.hasNext()) {

//            if(!isLocalStorageOnly &/& initialGlobalInsertCompleted)
            x = iteratorKeys.next();
            FileOutputStream outputStream;
            try {
                String fileValue = values.get(x).toString();
//              Referred the file write method from PA1
                String key = values.get(iteratorKeys.next()).toString();
                String keyHash = genHash(key);

                if(isLocalStorageOnly){
                    Log.v("either local storage or my hash space", "Inserting");
                    outputStream = context.openFileOutput(keyHash, Context.MODE_PRIVATE);
                    key = key+"-";
                    outputStream.write(key.getBytes());
                    outputStream.write(fileValue.getBytes());
                    Log.v("inserted in local storage", values.toString());
                    outputStream.close();
                }else if((node.getHashValue().compareTo(keyHash)>0 && node.getPrev()!=null && !node.getPrev().isEmpty() &&
                        node.getPrev().compareTo(keyHash)<0)){
                    outputStream = context.openFileOutput(keyHash, Context.MODE_PRIVATE);
                    key = key+"-";
                    outputStream.write(key.getBytes());
                    outputStream.write(fileValue.getBytes());
                    Log.v("inserted in local storage - Normal condition", values.toString());
                    outputStream.close();
                } else if(node.getHashValue().compareTo(keyHash)<0 && node.getPrev()!=null && !node.getPrev().isEmpty() &&
                        node.getPrev().compareTo(keyHash)<0 && node.getPrev().compareTo(node.getHashValue())>0){
                    outputStream = context.openFileOutput(keyHash, Context.MODE_PRIVATE);
                    key = key+"-";
                    outputStream.write(key.getBytes());
                    outputStream.write(fileValue.getBytes());
                    Log.v("keyhash > all nodes hash - default node 0", values.toString());
                    outputStream.close();
                } else if(node.getHashValue().compareTo(keyHash)>0 && node.getPrev()!=null && !node.getPrev().isEmpty() &&
                        node.getPrev().compareTo(keyHash)>0 && node.getPrev().compareTo(node.getHashValue())>0){
                    outputStream = context.openFileOutput(keyHash, Context.MODE_PRIVATE);
                    key = key+"-";
                    outputStream.write(key.getBytes());
                    outputStream.write(fileValue.getBytes());
                    Log.v("keyHash < all nodes hash but wrap around condition", values.toString());
                    outputStream.close();
                }else {
                            Log.e("Global node","keyhash:"+keyHash+"-"+"key"+key);
                        if(initialGlobalInsertCompleted){
                            Log.e(TAG,initialGlobalInsertCompleted.toString());
                            Log.e(TAG,"Calling clientTask for Insert Request - Lookup");
                            calledClientTaskForGlobalInsert = true;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyHash,key,fileValue,"Insert Request - Lookup");
                        }
                }
            } catch (Exception e) {
                Log.e("insert", "File write failed");
            }
        }
        Log.v("insert called", values.toString());
        return uri;
    }
    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        String myPortHash = "";
        try{
            TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

            globalMyPort = myPort;
            myPortHash = genHash(myPort);
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, myPortHash, portStr, "Node Join");
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }catch (NoSuchAlgorithmException e){
            Log.e(TAG,"onCreate()");
            return false;
        }catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }
        return true;
    }
    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
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
                    if("Node Join Request".equalsIgnoreCase(msgType)){
                    String myPort = (String) serverObjectInputStream.readObject();
                    String clientPort = (String) serverObjectInputStream.readObject();

                    String newHashedPort = "";
                    if (myPort.equalsIgnoreCase(REMOTE_PORT0)) {
                        newHashedPort = (String) serverObjectInputStream.readObject();

                        int x = Integer.parseInt(clientPort)/2;
                        String actualAvdId = Integer.toString(x);
                        newHashedPort = genHash(actualAvdId);
                        Log.i(TAG, "newHashedPort:" + newHashedPort);

                        nodesInRing.add(newHashedPort);
                        Log.i(TAG, "nodesInRing=" + nodesInRing.size());

                        Collections.sort(nodesInRing);

                    int newNodeIndex = nodesInRing.indexOf(newHashedPort);
                    MyNode newNode = new MyNode();
                    if(newNodeIndex!=0 && newNodeIndex!=nodesInRing.size()-1){
                        if(newNodeIndex+1<nodesInRing.size()){
                            newNode = new MyNode(nodesInRing.get(newNodeIndex)
                                        , nodesInRing.get(newNodeIndex+1), nodesInRing.get(newNodeIndex-1));
                        }
                        } else if(newNodeIndex==0 && nodesInRing.size()==1){
                        newNode = new MyNode(nodesInRing.get(newNodeIndex),null,null);
                        }else if(newNodeIndex==0 && nodesInRing.size()>1){
                        if(newNodeIndex+1<nodesInRing.size()){
                            newNode = new MyNode(nodesInRing.get(newNodeIndex),
                                nodesInRing.get(newNodeIndex+1),nodesInRing.get(nodesInRing.size()-1));
                        }
                        }else if(newNodeIndex==nodesInRing.size()-1){
                        if(newNodeIndex-1>=0){
                            newNode = new MyNode(nodesInRing.get(newNodeIndex),nodesInRing.get(0),
                                        nodesInRing.get(newNodeIndex-1));
                        }
                        }

                        if (myPort.equalsIgnoreCase(clientPort)) {
                            node.setNext(newNode.getNext());
                            node.setPrev(newNode.getPrev());
                            node.setHashValue(newNode.getHashValue());
                        }

                        serverObjectOutputStream.writeObject(newNode.getPrev());
                        serverObjectOutputStream.writeObject(newNode.getHashValue());
                        serverObjectOutputStream.writeObject(newNode.getNext());
                        serverObjectOutputStream.writeObject("Your hashed port received");
                    }
                } else if("Update Next".equalsIgnoreCase(msgType)){
                        String portToUpdate = (String) serverObjectInputStream.readObject();
                        String itsNext = (String) serverObjectInputStream.readObject();
                        if(node.getHashValue().equalsIgnoreCase(portToUpdate))
                            node.setNext(itsNext);

                        serverObjectOutputStream.writeObject("Successfully updated Next");
                    }else if("Update Prev".equalsIgnoreCase(msgType)){
                        String portToUpdate = (String) serverObjectInputStream.readObject();
                        String itsPrev = (String) serverObjectInputStream.readObject();
                        if(node.getHashValue().equalsIgnoreCase(portToUpdate))
                            node.setPrev(itsPrev);

                        serverObjectOutputStream.writeObject("Successfully updated Prev");
                    } else if("Insert Request - Lookup".equalsIgnoreCase(msgType)){
                        String myPortFromHashValue = "";
                        myPortFromHashValue = getHashToPort(node.getHashValue(),myPortFromHashValue);

                        if (REMOTE_PORT0.equalsIgnoreCase(myPortFromHashValue)){
                            String keyValueToLookUpNodeFor = (String) serverObjectInputStream.readObject();

                            String correctNodeToReturn = "";
//                            Perform Lookup
                            int sizeOfRing = nodesInRing.size();

                            int j = 0;
                            for (int i=0; i<sizeOfRing;i++){
                                j = i+1;
                                if(j<sizeOfRing && (nodesInRing.get(j).compareTo(keyValueToLookUpNodeFor)>0 &&
                                            nodesInRing.get(i).compareTo(keyValueToLookUpNodeFor)<0)){
                                    Log.e(TAG, "Found correct Node" + nodesInRing.get(j) + "For key" + keyValueToLookUpNodeFor
                                    + "Prev node" + nodesInRing.get(i));
                                    break;
                                }else if(j==sizeOfRing && i==sizeOfRing-1) {
                                        Log.e("Not found correct node","for Key"+keyValueToLookUpNodeFor+
                                                "Return avd0 as correct node");
                                        j=0;
                                        break;
                                    }
                            }
                            correctNodeToReturn=nodesInRing.get(j);
                            serverObjectOutputStream.writeObject(correctNodeToReturn);
                            serverObjectOutputStream.writeObject("LookUp done - Returned correctNode");
                            }
                        }else if("Perform Global Delete into Your Node".equalsIgnoreCase(msgType)){
                            String keyToInsertFromGlobalRequest = (String) serverObjectInputStream.readObject();
                            delete(mUri,keyToInsertFromGlobalRequest,null);
                            serverObjectOutputStream.writeObject("Delete Success in correctNode");
                        }else if("Perform Global Insert into Your Node".equalsIgnoreCase(msgType)){
                                String keyToInsertFromGlobalRequest = (String) serverObjectInputStream.readObject();
                                String valueToInsertFromGlobalRequest = (String) serverObjectInputStream.readObject();
        //                        Perform insert for global request
                                ContentValues cv = new ContentValues();
                                cv.put("key", keyToInsertFromGlobalRequest);
                                cv.put("value", valueToInsertFromGlobalRequest);
                                insert(mUri,cv);

                                serverObjectOutputStream.writeObject("Insert Success in correctNode");
                        }else if("Perform Global Query into Your Node".equalsIgnoreCase(msgType)){
                        String keyToQueryFromGlobalRequest = (String) serverObjectInputStream.readObject();
                        String valueToQueryFromGlobalRequest = (String) serverObjectInputStream.readObject();
                        //                        Perform insert for global request

                        Log.e(TAG,"keyToQueryFromGlobalRequest:"+keyToQueryFromGlobalRequest);
                        Cursor resultCursor = query(mUri,null,keyToQueryFromGlobalRequest,null,null);

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
                    }else if ("how many nodes alive?".equalsIgnoreCase(msgType)){
//                        String keyToQueryFromGlobalRequest = (String) serverObjectInputStream.readObject();
//                        if (keyToQueryFromGlobalRequest!=null && !keyToQueryFromGlobalRequest.isEmpty()
//                        && "how many nodes alive?".equalsIgnoreCase(keyToQueryFromGlobalRequest)){
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
            }
            catch (NoSuchAlgorithmException e){
                Log.e(TAG, "doInBackground-NoSuchAlgorithmException");
            }
            catch (SocketTimeoutException e){
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
//                Node joins
                String msg = msgs[3];
                if("Node Join".equalsIgnoreCase(msg)){
                    Log.i(TAG, "Node Join Request in Client");
                    String myPort = msgs[0];
                    String myPortHashed = msgs[1];
                    String myAvdId = msgs[2];

                Socket socket0 = new Socket();
                socket0.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0)),300);
                Log.e(TAG,"Connected To Server");
                if(socket0!=null && socket0.isConnected()){
                    //            Check if AVD0 is alive = Yes
                    //            Enter ring
                    ObjectOutputStream objectOutputStream0 = new ObjectOutputStream(socket0.getOutputStream());
                    objectOutputStream0.writeObject("Node Join Request");
                    objectOutputStream0.writeObject(REMOTE_PORT0);
                    objectOutputStream0.writeObject(myPort);
                    objectOutputStream0.writeObject(myPortHashed);
                    objectOutputStream0.flush();

                    ObjectInputStream objectInputStream0 = new ObjectInputStream(socket0.getInputStream());
                    String prev = (String) objectInputStream0.readObject();
                    if (prev!=null)Log.e("My prev",prev);
                    String nodeValue = (String) objectInputStream0.readObject();
                    Log.e("My value",nodeValue);
                    String next = (String) objectInputStream0.readObject();
                    if (next!=null)Log.e("My next",next);

                    node = new MyNode(nodeValue,next,prev);

                    String ack = (String) objectInputStream0.readObject();
                    if(ack!=null && !ack.isEmpty() && ack.equalsIgnoreCase("Your hashed port received")){
                        socket0.close();
                        Log.i(TAG,"Closing socket from ClientTask");
                    }

//                    Send update to next and prev nodes
                    String updatesToNextPort = "";
                    updatesToNextPort = getHashToPort(next, updatesToNextPort);

                    String updatesToPrevPort = "";
                    updatesToPrevPort = getHashToPort(prev, updatesToPrevPort);

                    if (updatesToNextPort!=null && !updatesToNextPort.isEmpty()) {
                        Socket socketForUpdateNext = new Socket
                                (InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(updatesToNextPort));
                        ObjectOutputStream objectOutputStreamForUpdateNext = new ObjectOutputStream(socketForUpdateNext.getOutputStream());
                        objectOutputStreamForUpdateNext.writeObject("Update Prev");
                        objectOutputStreamForUpdateNext.writeObject(next);
                        objectOutputStreamForUpdateNext.writeObject(nodeValue);
                        objectOutputStreamForUpdateNext.flush();

                        ObjectInputStream objectInputStreamForUpdateNext = new ObjectInputStream(socketForUpdateNext.getInputStream());
                        String ackForUpdateNext = (String) objectInputStreamForUpdateNext.readObject();
                        if (ackForUpdateNext != null && !ackForUpdateNext.isEmpty()
                                && "Successfully updated Prev".equalsIgnoreCase(ackForUpdateNext)) {
                            Log.i(TAG, "Closing socket for Update Next");
                            socketForUpdateNext.close();
                        }
                    }

                    if (updatesToPrevPort!=null && !updatesToPrevPort.isEmpty()){
                    Socket socketForUpdatePrev = new Socket
                            (InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(updatesToPrevPort));
                    ObjectOutputStream objectOutputStreamForUpdatePrev = new ObjectOutputStream(socketForUpdatePrev.getOutputStream());
                    objectOutputStreamForUpdatePrev.writeObject("Update Next");
                    objectOutputStreamForUpdatePrev.writeObject(prev);
                    objectOutputStreamForUpdatePrev.writeObject(nodeValue);
                    objectOutputStreamForUpdatePrev.flush();

                    ObjectInputStream objectInputStreamForUpdatePrev = new ObjectInputStream(socketForUpdatePrev.getInputStream());
                    String ackForUpdatePrev = (String) objectInputStreamForUpdatePrev.readObject();
                    if (ackForUpdatePrev!=null && !ackForUpdatePrev.isEmpty()
                            && "Successfully updated Next".equalsIgnoreCase(ackForUpdatePrev)){
                        Log.i(TAG,"Closing socket for Update Prev");
                        socketForUpdatePrev.close();
                    }
                    }
                }else{
                    //            Check if AVD0 is alive = No
                    //            Local storage only
                    String nodeValue = genHash(Integer.toString(Integer.parseInt(myPort)/2)) ;
                    node = new MyNode(nodeValue,null,null);
                    Log.i(TAG, "isLocalStorageOnly = true");
                    isLocalStorageOnly = true;
                    }
                }else if("Insert Request - Lookup".equalsIgnoreCase(msg)){
                    Log.e(TAG,"Insert Request - Lookup");
                    initialGlobalInsertCompleted = false;
                    Log.v(TAG,"initialGlobalInsertCompleted=false");
                    String keyHashFromInsert = msgs[0];
                    String originalKeyFromInsert = msgs[1];
                    String valueFromInsert = msgs[2];
//                    Ask AVD0 for correct position
                    Socket socketForInsertLookUptoAvd0 = new Socket
                            (InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0));
                    ObjectOutputStream outputStreamForInsertLookUptoAvd0 = new ObjectOutputStream(socketForInsertLookUptoAvd0.getOutputStream());
                    outputStreamForInsertLookUptoAvd0.writeObject("Insert Request - Lookup");
                    outputStreamForInsertLookUptoAvd0.writeObject(keyHashFromInsert);
                    outputStreamForInsertLookUptoAvd0.flush();

                    ObjectInputStream inputStreamForInsertLookUptoAvd0 = new ObjectInputStream(socketForInsertLookUptoAvd0.getInputStream());
                    String correctNode = (String) inputStreamForInsertLookUptoAvd0.readObject();
                    String ack = (String) inputStreamForInsertLookUptoAvd0.readObject();
                    if (ack!=null && !ack.isEmpty() && "LookUp done - Returned correctNode".equalsIgnoreCase(ack)){
                        Log.e(TAG,"LookUp done - Returned correctNode");
                        Log.e(TAG,"Returned correctNode:"+correctNode);
                        outputStreamForInsertLookUptoAvd0.close();
                        socketForInsertLookUptoAvd0.close();
                    }
                    String correctPortNumber = "";
                    correctPortNumber = getHashToPort(correctNode,correctPortNumber);

//                    Ask Correct Node to insert
                    try{
                        Socket socketForInsertToGlobalNode = new Socket
                                (InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(correctPortNumber));

                        ObjectOutputStream objectOutputStreamForInsertToGlobalNode = new ObjectOutputStream(socketForInsertToGlobalNode.getOutputStream());
                        objectOutputStreamForInsertToGlobalNode.writeObject("Perform Global Insert into Your Node");
                        Log.e(TAG,"Perform Global Insert into Your Node");
                        objectOutputStreamForInsertToGlobalNode.flush();
                        objectOutputStreamForInsertToGlobalNode.writeObject(originalKeyFromInsert);
                        objectOutputStreamForInsertToGlobalNode.writeObject(valueFromInsert);
                        objectOutputStreamForInsertToGlobalNode.flush();

                        ObjectInputStream inputStreamForInsertToGlobalNode = new ObjectInputStream(socketForInsertToGlobalNode.getInputStream());
                        String ackForInsertToGlobalNode = (String) inputStreamForInsertToGlobalNode.readObject();
                        if (ackForInsertToGlobalNode!=null && !ackForInsertToGlobalNode.isEmpty()
                                && "Insert Success in correctNode".equalsIgnoreCase(ackForInsertToGlobalNode)){
                            Log.e(TAG,"Insert Success in correctNode");
                            calledClientTaskForGlobalInsert=false;
                            Log.e(TAG, "calledClientTaskForGlobalInsert=false");
                            Log.e(TAG, "initialGlobalInsertCompleted=true");
                            initialGlobalInsertCompleted = true;
                            objectOutputStreamForInsertToGlobalNode.close();
                            socketForInsertToGlobalNode.close();
                        }
                    } catch (StreamCorruptedException e){
                        Log.e("DANGER","StreamCorruptedException"+"-"+correctPortNumber);
                    }

                } else if("* for global query".equalsIgnoreCase(msg)){
                    Log.e(TAG,"* for global query");
                    initialGlobalQueryCompletedStar=false;
                    Log.e(TAG,"initialGlobalQueryCompletedStar=false");
//                    Ask AVD0 how many nodes are alive
                    Socket socketForInsertLookUptoAvd0 = new Socket
                            (InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0));
                    ObjectOutputStream outputStreamForInsertLookUptoAvd0 = new ObjectOutputStream(socketForInsertLookUptoAvd0.getOutputStream());
                    outputStreamForInsertLookUptoAvd0.writeObject("how many nodes alive?");
                    outputStreamForInsertLookUptoAvd0.flush();

                    ObjectInputStream inputStreamForInsertLookUptoAvd0 = new ObjectInputStream(socketForInsertLookUptoAvd0.getInputStream());
                    Boolean avd0Alive = (Boolean) inputStreamForInsertLookUptoAvd0.readObject();
                    Boolean avd1Alive = (Boolean) inputStreamForInsertLookUptoAvd0.readObject();
                    Boolean avd2Alive = (Boolean) inputStreamForInsertLookUptoAvd0.readObject();
                    Boolean avd3Alive = (Boolean) inputStreamForInsertLookUptoAvd0.readObject();
                    Boolean avd4Alive = (Boolean) inputStreamForInsertLookUptoAvd0.readObject();
                    String ack = (String) inputStreamForInsertLookUptoAvd0.readObject();
                    if (ack!=null && !ack.isEmpty() && "how many nodes alive? - Answered".equalsIgnoreCase(ack)){
                        Log.e(TAG,"how many nodes alive? - Answered");
//                        Log.e(TAG,"Returned correctNode:"+correctNode);
                        outputStreamForInsertLookUptoAvd0.close();
                        socketForInsertLookUptoAvd0.close();
                    }
                    String avd0Storage="",avd1Storage="",avd2Storage="",avd3Storage="",avd4Storage="";

                    if (avd0Alive) avd0Storage = askForlocalQueryAndCursor(avd0Alive,REMOTE_PORT0);
                    Log.e(TAG,"avd0Storage got");
                    if (avd1Alive) avd1Storage = askForlocalQueryAndCursor(avd1Alive,REMOTE_PORT1);
                    Log.e(TAG,"avd1Storage got");
                    if (avd2Alive) avd2Storage = askForlocalQueryAndCursor(avd2Alive, REMOTE_PORT2);
                    Log.e(TAG,"avd2Storage got");
                    if (avd3Alive) avd3Storage = askForlocalQueryAndCursor(avd3Alive, REMOTE_PORT3);
                    Log.e(TAG,"avd3Storage got");
                    if (avd4Alive) avd4Storage = askForlocalQueryAndCursor(avd4Alive, REMOTE_PORT4);
                    Log.e(TAG,"avd4Storage got");

                    createResultCursorFromStrings(avd0Storage,avd1Storage,avd2Storage,
                            avd3Storage,avd4Storage);

                } else if("Query Request - Lookup".equalsIgnoreCase(msg)){
                    Log.e(TAG,"Query Request - Lookup");
                    initialGlobalQueryCompleted = false;
                    Log.v(TAG,"initialGlobalQueryCompleted=false");
                    String keyHashFromQuery = msgs[0];
                    String originalKeyFromQuery = msgs[1];
                    String valueFromQuery = msgs[2];
//                    Ask AVD0 for correct position
                    Socket socketForInsertLookUptoAvd0 = new Socket
                            (InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0));
                    ObjectOutputStream outputStreamForInsertLookUptoAvd0 = new ObjectOutputStream(socketForInsertLookUptoAvd0.getOutputStream());
                    outputStreamForInsertLookUptoAvd0.writeObject("Insert Request - Lookup");
                    outputStreamForInsertLookUptoAvd0.writeObject(keyHashFromQuery);
                    outputStreamForInsertLookUptoAvd0.flush();

                    ObjectInputStream inputStreamForInsertLookUptoAvd0 = new ObjectInputStream(socketForInsertLookUptoAvd0.getInputStream());
                    String correctNode = (String) inputStreamForInsertLookUptoAvd0.readObject();
                    String ack = (String) inputStreamForInsertLookUptoAvd0.readObject();
                    if (ack!=null && !ack.isEmpty() && "LookUp done - Returned correctNode".equalsIgnoreCase(ack)){
                        Log.e(TAG,"LookUp done - Returned correctNode for Query");
                        Log.e(TAG,"Returned correctNode:"+correctNode);
                        outputStreamForInsertLookUptoAvd0.close();
                        socketForInsertLookUptoAvd0.close();
                    }
                    String correctPortNumber = "";
                    correctPortNumber = getHashToPort(correctNode,correctPortNumber);

                    //                    Ask Correct Node to Query
                    Socket socketForInsertToGlobalNode = new Socket
                            (InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(correctPortNumber));
                    ObjectOutputStream objectOutputStreamForInsertToGlobalNode = new ObjectOutputStream(socketForInsertToGlobalNode.getOutputStream());
                    objectOutputStreamForInsertToGlobalNode.writeObject("Perform Global Query into Your Node");
                    Log.e(TAG,"originalKeyFromQuery1 = "+originalKeyFromQuery);
                    Log.e(TAG,"Perform Global Query into Your Node");
                    objectOutputStreamForInsertToGlobalNode.writeObject(originalKeyFromQuery);
                    Log.e(TAG,"originalKeyFromQuery2="+originalKeyFromQuery);
                    objectOutputStreamForInsertToGlobalNode.writeObject(valueFromQuery);
                    objectOutputStreamForInsertToGlobalNode.flush();

                    ObjectInputStream inputStreamForInsertToGlobalNode = new ObjectInputStream(socketForInsertToGlobalNode.getInputStream());
                    String valueFromGlobalNode = (String) inputStreamForInsertToGlobalNode.readObject();
                    String ackForInsertToGlobalNode = (String) inputStreamForInsertToGlobalNode.readObject();
                    if (ackForInsertToGlobalNode!=null && !ackForInsertToGlobalNode.isEmpty()
                            && "Query Success in correctNode".equalsIgnoreCase(ackForInsertToGlobalNode)){
                        Log.e(TAG,"Query Success in correctNode");
                        calledClientTaskForGlobalQuery=false;
                        Log.e(TAG, "calledClientTaskForGlobalQuery=false");
                        Log.e(TAG, "initialGlobalQueryCompleted=true");
                        initialGlobalQueryCompleted = true;
                        Log.e(TAG, "valueFromGlobalNode:"+valueFromGlobalNode);
                        valueFromGlobalNodeForAVD = valueFromGlobalNode;
                        objectOutputStreamForInsertToGlobalNode.close();
                        socketForInsertToGlobalNode.close();
                    }
                }else if ("Delete Request".equalsIgnoreCase(msg)){
                    Log.e(TAG,"Delete Request");
                    initialGlobalDeleteCompleted = false;
                    Log.v(TAG,"initialGlobalDeleteCompleted=false");
                    String keyHashFromQuery = msgs[0];
                    String originalKeyFromQuery = msgs[1];
                    String valueFromQuery = msgs[2];
//                    Ask AVD0 for correct position
                    Socket socketForInsertLookUptoAvd0 = new Socket
                            (InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0));
                    ObjectOutputStream outputStreamForInsertLookUptoAvd0 = new ObjectOutputStream(socketForInsertLookUptoAvd0.getOutputStream());
                    outputStreamForInsertLookUptoAvd0.writeObject("Insert Request - Lookup");
                    outputStreamForInsertLookUptoAvd0.writeObject(keyHashFromQuery);
                    outputStreamForInsertLookUptoAvd0.flush();

                    ObjectInputStream inputStreamForInsertLookUptoAvd0 = new ObjectInputStream(socketForInsertLookUptoAvd0.getInputStream());
                    String correctNode = (String) inputStreamForInsertLookUptoAvd0.readObject();
                    String ack = (String) inputStreamForInsertLookUptoAvd0.readObject();
                    if (ack!=null && !ack.isEmpty() && "LookUp done - Returned correctNode".equalsIgnoreCase(ack)){
                        Log.e(TAG,"LookUp done - Returned correctNode for Query");
                        Log.e(TAG,"Returned correctNode:"+correctNode);
                        outputStreamForInsertLookUptoAvd0.close();
                        socketForInsertLookUptoAvd0.close();
                    }
                    String correctPortNumber = "";
                    correctPortNumber = getHashToPort(correctNode,correctPortNumber);

                    Socket socketForInsertToGlobalNode = new Socket
                            (InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(correctPortNumber));
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
            }catch (SocketTimeoutException e){
                Log.e(TAG, "doInBackground-SocketTimeoutException");
            }catch (UnknownHostException e){
                Log.e(TAG, "doInBackground-UnknownHostException");
            }catch (StreamCorruptedException e){
                try{
                    String nodeValue = genHash(Integer.toString(Integer.parseInt(globalMyPort)/2)) ;
                    node = new MyNode(nodeValue,null,null);}catch (NoSuchAlgorithmException e1){
                    Log.e(TAG,"NoSuchAlgorithmException inside IOException");
                }
                isLocalStorageOnly=true;
                Log.i(TAG, "isLocalStorageOnly = true");
                Log.e(TAG, "doInBackground - StreamCorruptedException");
            } catch (IOException e){
                try{
                String nodeValue = genHash(Integer.toString(Integer.parseInt(globalMyPort)/2)) ;
                node = new MyNode(nodeValue,null,null);}catch (NoSuchAlgorithmException e1){
                    Log.e(TAG,"NoSuchAlgorithmException inside IOException");
                }
                isLocalStorageOnly=true;
                Log.i(TAG, "isLocalStorageOnly = true");
                Log.e(TAG, "doInBackground - IOException");
            }catch (ClassNotFoundException e){
                Log.e(TAG, "doInBackground-ClassNotFoundException");
            }catch (NoSuchAlgorithmException e){
                Log.e(TAG, "doInBackground-NoSuchAlgorithmException");
            }
            return null;
        }

        private void createResultCursorFromStrings(String avd0Storage, String avd1Storage, String avd2Storage, String avd3Storage, String avd4Storage) {
            MatrixCursor newMatrixCursor = new MatrixCursor(new String[]{"key", "value"});

            Log.e(TAG, "Inside createResultCursorFromStrings");
            giveStringAddedToCursor(avd0Storage, newMatrixCursor);
            Log.e(TAG, "Inside createResultCursorFromStrings + avd0Storage");
            giveStringAddedToCursor(avd1Storage, newMatrixCursor);
            Log.e(TAG, "Inside createResultCursorFromStrings + avd1Storage");
            giveStringAddedToCursor(avd2Storage, newMatrixCursor);
            Log.e(TAG, "Inside createResultCursorFromStrings+ avd2Storage");
            giveStringAddedToCursor(avd3Storage, newMatrixCursor);
            Log.e(TAG, "Inside createResultCursorFromStrings+ avd3Storage");
            giveStringAddedToCursor(avd4Storage, newMatrixCursor);
            Log.e(TAG, "Inside createResultCursorFromStrings+ avd4Storage");

            Log.e(TAG,"newMatrixCursor.getCount() = "+newMatrixCursor.getCount());
//            globalStarQueryResult = newMatrixCursor;
            valueFromGlobalNodeForAVDStar = true;
            initialGlobalQueryCompletedStar=true;
            Log.e(TAG,"globalStarQueryResult is set");
            Log.e(TAG,"valueFromGlobalNodeForAVDStar=true");
        }

        private void giveStringAddedToCursor(String avd0Storage, MatrixCursor newMatrixCursor) {
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

        private String askForlocalQueryAndCursor(Boolean avd0Alive, String remortPort) throws IOException {
            Log.e(TAG,"askForlocalQueryAndCursor");
            if (avd0Alive){
                Socket socketAvd0 = new Socket
                        (InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remortPort));
                ObjectOutputStream outputStreamAvd0 = new ObjectOutputStream(socketAvd0.getOutputStream());
                outputStreamAvd0.writeObject("Perform @ and return cursor");
                outputStreamAvd0.flush();
                try{
                    ObjectInputStream objectInputStream0 = new ObjectInputStream(socketAvd0.getInputStream());
                    String localQueryResult = (String) objectInputStream0.readObject();
                    Log.e(TAG,"Returning query result as"+localQueryResult.toString());

                    String ack = (String) objectInputStream0.readObject();
                    if (ack!=null && !ack.isEmpty() && "Returned individual resultCursorToReturn to requesting node".equalsIgnoreCase(ack)){
                        Log.e(TAG,"Returned individual resultCursorToReturn to requesting node");
                        socketAvd0.close();
                    }
                    return localQueryResult;
                }catch (ClassNotFoundException e){
                    Log.e(TAG,"ClassNotFoundException in askForlocalQueryAndCursor");
                }
            }
            return null;
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

}
