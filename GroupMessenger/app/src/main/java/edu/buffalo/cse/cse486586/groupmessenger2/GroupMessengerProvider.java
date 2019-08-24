package edu.buffalo.cse.cse486586.groupmessenger2;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.util.Log;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Set;

/**
 * GroupMessengerProvider is a key-value table. Once again, please note that we do not implement
 * full support for SQL as a usual ContentProvider does. We re-purpose ContentProvider's interface
 * to use it as a key-value table.
 * 
 * Please read:
 * 
 * http://developer.android.com/guide/topics/providers/content-providers.html
 * http://developer.android.com/reference/android/content/ContentProvider.html
 * 
 * before you start to get yourself familiarized with ContentProvider.
 * 
 * There are two methods you need to implement---insert() and query(). Others are optional and
 * will not be tested.
 * 
 * @author stevko
 *
 */
public class GroupMessengerProvider extends ContentProvider {

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // You do not need to implement this.
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        /*
         * TODO: You need to implement this method. Note that values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         *
         * For actual storage, you can use any option. If you know how to use SQL, then you can use
         * SQLite. But this is not a requirement. You can use other storage options, such as the
         * internal storage option that we used in PA1. If you want to use that option, please
         * take a look at the code for PA1.
         */
        // For getting the application context
        Context context = getContext();
//        Iterating over the content values to get the <key,Value> pairs for insertion
        Set<String> keySet = values.keySet();
        Iterator<String> iteratorKeys = keySet.iterator();
        String x = "";
        while(iteratorKeys.hasNext()) {
            x = iteratorKeys.next();
            FileOutputStream outputStream;
            try {
                String fileValue = values.get(x).toString();
//              Referred the file write method from PA1
//              And because this is not the main activity, we need to use application context to perform the file creation and write.
                outputStream = context.openFileOutput(values.get(iteratorKeys.next()).toString(), Context.MODE_PRIVATE);
                outputStream.write(fileValue.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e("insert", "File write failed");
            }
        }
        Log.v("insert", values.toString());
        return uri;
    }

    @Override
    public boolean onCreate() {
        // If you need to perform any one-time initialization task, please do it here.
        return false;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        /*
         * TODO: You need to implement this method. Note that you need to return a Cursor object
         * with the right format. If the formatting is not correct, then it is not going to work.
         *
         * If you use SQLite, whatever is returned from SQLite is a Cursor object. However, you
         * still need to be careful because the formatting might still be incorrect.
         *
         * If you use a file storage option, then it is your job to build a Cursor * object. I
         * recommend building a MatrixCursor described at:
         * http://developer.android.com/reference/android/database/MatrixCursor.html
         */
//      https://developer.android.com/reference/android/database/MatrixCursor.html#MatrixCursor(java.lang.String[])
//      Used the following constructor to define a matrixCursor object
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
        String value = "";
        FileInputStream inputStream;

        Context context = getContext();
        try {
// https://developer.android.com/reference/android/content/Context#openFileInput(java.lang.String)
//            Reference from the above Url, to use the method for reading a file
            InputStream is = context.openFileInput(selection);
            InputStreamReader ir = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(ir);
            value = br.readLine();
            is.close();
        } catch (FileNotFoundException e) {
            Log.v("query", "File Not Found");
        }catch (Exception e) {
            Log.v("query", "File read failed");
        }
// https://developer.android.com/reference/android/database/MatrixCursor.html#addRow(java.lang.Object[])
//      Inserting the data into the matrix cursor to return
        matrixCursor.addRow(new String[] {selection, value});
        Log.v("query", selection);
        return matrixCursor;
    }
}
