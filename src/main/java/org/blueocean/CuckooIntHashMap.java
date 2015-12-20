import java.io.BufferedReader;

/*
 * this class solves a specific problem, that is, given millions of ids and their assoiciated data, how to build a in-memory cache
 * to store the mapping from id to the data.
 *
 * ids are positive numeric numbers
 *
 * Cuckoo hash is part of open addressing family HashTable, the key of open addressing is that cache collision is controlled or avoided, so the
 * worse case time complexity of GET operation is constant time.
 *
 * id to tags hash table cache implemented by Cuckoo algorithm, cache can be preloaded by parsing a file
 *
 * add a stash to accommodate a few overflow entry
 *
 *
 * for simplicity, all keys are positive
 *
 * @author junminliu@bloomberg.net
 */
public class CuckooIntHashMap<V> {
 private static final float DEFAULT_LOAD_FACTOR = 0.75f;
 private static final int DEFAULT_INIT_CAPACITY = 16;
 private volatile static boolean ready;
 
 /*
 * public method to create cache singleton
 */
 public static <T> CuckooIntHashMap<T> getInstance() throws FileNotFoundException, IOException{
  
  return (CuckooIntHashMap<T>)INSTANCE;
 }
 
 //the key index array, map indices of buckets to keys
 private int[] keys;
 //the buckets
 private V[] cache;
 //the stash
 private Entry<V>[] stash;
 private int stashSize;
 //the size of the buckets
 private int capacity;
 //the number of filled buckets
 private int size;
 //the maximum ratio of size over capacity until resizing 
 private final float loadFactor;
 
 private final transient IntHashFuncI[] hashFunctions;
 static final int PRIME_NUMBER2 =0xb4b82e39;
 static final int PRIME_NUMBER3 =0xced1c241;
 private static final IntHashFuncI[] HASH_FUNCS = new IntHashFuncI[]{new BitOpHash(PRIME_NUMBER2) , new BitOpHash(PRIME_NUMBER3)};
 
 private static final CuckooIntHashMap<?> INSTANCE = new CuckooIntHashMap();
 


 
 //this two packaged protected constructors are mainly for unit tests purpose
 CuckooIntHashMap() {
  this(DEFAULT_INIT_CAPACITY, DEFAULT_LOAD_FACTOR, HASH_FUNCS);
 }
 
 CuckooIntHashMap(int iniCapacity, float loadfactor, IntHashFuncI[] hashFuncs){
  keys = new int[iniCapacity];
  cache = (V[])new Object[iniCapacity];
  stash = (Entry<V>[]) new Entry[(int) Math.max(3, Math.log(iniCapacity))];
  loadFactor = loadfactor;
  capacity = iniCapacity;
  hashFunctions = hashFuncs; 
  hashFunctions[0].reset(capacity);
  hashFunctions[1].reset(capacity);  
  ready = true;
 }


 
 public V get(int id){
  if(!ready)
   throw new IllegalStateException("cache is not ready yet");
  for(int i=0; i<2; i++){
   int index = //keys[id];
     hashFunctions[i].hash(id, capacity);
   if(keys[index] == id &&  cache[index]!=null)
    return cache[index];
  }
    
  for(Entry e: stash){
   if(e!=null && e.id == id)
    return (V)e.tags;
  }
  
  return null;
 }
 
 boolean insert(int id, V tags){
  return insert(id, tags, true);
 }
 
 /*
 * boolean flag indicating if it is new insertion (true) or rehashing (false)
 */
 boolean insert(int id, V tags, boolean flag){
  for(int i=0; i<2; i++){
   int index = hashFunctions[i].hash(id, capacity);
   if(cache[index]==null){
    cache[index] = tags;
    keys[index] = id; // map index to key
    if(flag)
     this.size++;
    return true;
   }
  }
  return false;
 }
 
 void put(int id, V tags){
  put(id, tags, true);
 }
 
 void put(int id, V tags, boolean flag){
  //tags = Collections.unmodifiableSet(tags);
  ensureCapacity(id);
  
  if(this.insert(id, tags, flag))
   return;
  //start the cuckoo bullying process
  V insert = tags;
  V current = tags;
  
  int currentId = id;
  int counter = 0;
  int index = hashFunctions[0].hash(id, capacity);
  while(counter++<this.capacity || current!=insert ){
   if(cache[index]==null){
    cache[index] = current;
    keys[index] = currentId;
    if(flag)
     size++;
    return;
   }
   
   int tempId = keys[index];
   V tempSet = cache[index];
   
   keys[index] = currentId;
   cache[index] = current;
   
   current = tempSet;
   currentId = tempId;
   
   if(index == hashFunctions[0].hash(currentId, capacity))
    index = hashFunctions[1].hash(currentId, capacity);
   else
    index = hashFunctions[0].hash(currentId, capacity);
  }
  
  //try stashing before rehash
  if(stash(id, tags, flag))
   return;
  System.out.println("stash is full " + this.stashSize);
  rehash(this.capacity<<1);
  put(id, tags, flag);
 }
 
 boolean stash(int id, V tags, boolean flag){
  if(stashSize+1<=stash.length){
   stash[stashSize++] = new Entry<V>(id, tags);
   return true;
  }
  return false;
 }
 
 /*
 * since stash size is small, it won't count toward loadFactor
 */
 private void ensureCapacity (int id) {
  if(this.size>=this.loadFactor*this.capacity){
   System.out.format("ensureCapacity %d, %d %d %f", id, this.size, this.capacity,  this.loadFactor);
   rehash(this.capacity<<1);
  }
 }
 
 /*
 * rehash the entries by increasing hash table size to next power of 2
 */
 private void rehash(int newSize) {
  System.out.println("rehash to " + newSize);
  int temp = this.size;
  this.capacity = newSize;
  hashFunctions[0].reset(capacity);
  hashFunctions[1].reset(capacity);
  V[] oldCache = cache;
  Entry<V>[] oldStash = stash;
  int[] oldKeys = keys;
  
  cache = (V[])new Object[newSize];
  stash = (Entry<V>[])new Entry[(int) Math.max(3, Math.log(capacity))];
  stashSize = 0;
  keys = new int[newSize];
  for(int i=0; i< oldKeys.length; i++){
   if(oldKeys[i]!=0 && oldCache[i]!=null)
    this.put(oldKeys[i], oldCache[i], false);
  }
  
  for(Entry<V> e : oldStash){
   if(e!=null)
    this.put(e.id, e.tags, false);
  }
  
  this.size = temp;
  System.out.format("rehash done and size excluding stash is %d and stash size is %d \n",  this.size, this.stashSize);
 }

 //int-keyed entity tags HashMap entry, immutable
 final class Entry<V> {
  private final int id;
  private final V tags;
  public Entry(final int id, final V tags) {
   this.id = id;
   this.tags = tags;
  }
  int getId(){
   return id;
  }
  
  public V getTags() {
   return tags;
  }  
 }
  
 //implementation of hash function using bit operation
 static class BitOpHash implements IntHashFuncI {
  private final int prime;
  private int shift;
  
  BitOpHash(int prime){
   this.prime = prime;
  }
  @Override
  public int hash(int key, int range){
   key *= prime;
     return (key ^ (key >>> shift)) & (range - 1);
  }

  @Override
  public void reset(int range) {
   shift = 31 - Integer.numberOfTrailingZeros(range);
  }
 }
 
 //implementation of hash function using random generator
 static class HashFunc implements IntHashFuncI {
  private static final Random GENERATOR = new Random();
  private int round;
  HashFunc(int loop){
   round = loop;
  }
  @Override
  public int hash(int key, int range){
   GENERATOR.setSeed(key);
   int hash = GENERATOR.nextInt(range);
   for(int i=0; i<this.round; i++)
    hash = GENERATOR.nextInt(range);
   return hash;
  } 
  @Override
  public void reset(int range){}
 }
 
 static interface IntHashFuncI {
  public int hash(int key, int range);
  public void reset(int range);
 }

 /*
 * a special implementation of string intern which is faster than JDK version
 */
 private static final ConcurrentMap<String, String> TAG_POOL = new ConcurrentHashMap<String, String>();
 public static String intern(String s) {
  String result = TAG_POOL.get(s);
  if (result == null) {
   result = TAG_POOL.putIfAbsent(s, s);
   if (result == null)
    result = s;
  }
  return result;
 }
 
 public int size(){
  return this.size;
 }
 
 public int capacity(){
  return this.capacity;
 }
}
