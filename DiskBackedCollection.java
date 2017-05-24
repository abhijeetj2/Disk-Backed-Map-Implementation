
package com.my.diskbackedcollection;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

/**
 * @author hp
 *
 * @param <K>
 * @param <V>
 */
/**
 * @author hp
 *
 * @param <K>
 * @param <V>
 */
/**
 * @author hp
 *
 * @param <K>
 * @param <V>
 */
public class DiskBackedCollection<K extends Serializable, V extends Serializable> extends TreeMap<K, V> implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	// Local Database for storing the collections
	private DB database;
	// internal map
	private BTreeMap<Object,Object> treeMap;
	// name of the database
	private static final String DB_NAME = "DB_Collection";
	// name of the internal map
	private static final String MAP_NAME = "internal_map";

	private void initiliazeDB() {
		this.database = DBMaker.newFileDB(new File(DB_NAME)).closeOnJvmShutdown().make();
	}

	private void initiliazeTreeMap() {
		this.treeMap = this.database.getTreeMap(MAP_NAME);
	}

	/**
	 * Constructor which initialize DB and Map
	 */
	public DiskBackedCollection() {
		this.initiliazeDB();
		this.initiliazeTreeMap();
	}

	@SuppressWarnings("unchecked")
	@Override
	public V put(K key, V value) {
		synchronized (treeMap) {
			return (V) this.treeMap.put(key, value);
		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		synchronized (treeMap) {
			return (V) this.treeMap.get(key);
		}

	}
	
	
	
	/**
	 * method to serialize map in SerializedMap file
	 * @param TreeMap
	 */
	@SuppressWarnings("rawtypes")
	public void serializeMap(TreeMap map) {
		 try
         {
                FileOutputStream fos =
                   new FileOutputStream("SerializedMap.ser");
                ObjectOutputStream oos = new ObjectOutputStream(fos);
                oos.writeObject(map);
                oos.close();
                fos.close();
                System.out.printf("Serialized Map data is saved in hashmap.ser");
         }catch(IOException ex)
          {
                ex.printStackTrace();
          }
	}

/*	 public static void main(String [] args)
     {
          TreeMap<Integer, String> treeMap = new TreeMap<Integer, String>();
          //Adding elements to TreeMap
          treeMap.put(11, "AB");
          treeMap.put(2, "CD");
          treeMap.put(33, "EF");
          treeMap.put(9, "GH");
          treeMap.put(3, "IJ");
          DiskBackedCollection map = new DiskBackedCollection();
          map.serializeMap(treeMap);
     }*/
}