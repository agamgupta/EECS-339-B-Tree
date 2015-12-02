#include <assert.h>
#include <string.h>
#include "btree.h"

KeyValuePair::KeyValuePair()
{}


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) : 
  key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
  key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize, 
		       SIZE_T valuesize,
		       BufferCache *cache,
		       bool unique) 
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now
}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) { 
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+2;
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) { 
      return rc;
    }
    
    BTreeNode newrootnode(BTREE_ROOT_NODE,
			  superblock.info.keysize,
			  superblock.info.valuesize,
			  buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) { 
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) { 
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;
      
      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
	return rc;
      }

    }
  }

  // OK, now, mounting the btree is simply a matter of reading the superblock 

  return superblock.Unserialize(buffercache,initblock);
}
    

ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}
 

ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
					   const BTreeOp op,
					   const KEY_T &key,
					   VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey || key==testkey) {
	// OK, so we now have the first key that's larger
	// so we ned to recurse on the ptr immediately previous to 
	// this one, if it exists
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	return LookupOrUpdateInternal(ptr,op,key,value);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return LookupOrUpdateInternal(ptr,op,key,value);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) { 
	if (op==BTREE_OP_LOOKUP) { 
	  return b.GetVal(offset,value);
	} else { 
	  // BTREE_OP_UPDATE
	  // WRITE ME
    // b.SetVal(offset,value);
    rc = b.SetVal(offset,value);
    if(rc){return rc;}
    return b.Serialize(buffercache, node);
	}
      }
    }
    return ERROR_NONEXISTENT;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }  

  return ERROR_INSANE;
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;

  if (dt==BTREE_DEPTH_DOT) { 
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) { 
      } else { 
	os << "Interior: ";
      }
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	os << "*" << ptr << " ";
	// Last pointer
	if (offset==b.info.numkeys) break;
	rc=b.GetKey(offset,key);
	if (rc) {  return rc; }
	for (i=0;i<b.info.keysize;i++) { 
	  os << key.data[i];
	}
	os << " ";
      }
    }
    break;
  case BTREE_LEAF_NODE:
    if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) { 
    } else {
      os << "Leaf: ";
    }
    for (offset=0;offset<b.info.numkeys;offset++) { 
      if (offset==0) { 
	// special case for first pointer
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (dt!=BTREE_SORTED_KEYVAL) { 
	  os << "*" << ptr << " ";
	}
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << "(";
      }
      rc=b.GetKey(offset,key);
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) { 
	os << key.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ",";
      } else {
	os << " ";
      }
      rc=b.GetVal(offset,value);
      if (rc) {  return rc; }
      for (i=0;i<b.info.valuesize;i++) { 
	os << value.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ")\n";
      } else {
	os << " ";
      }
    }
    break;
  default:
    if (dt==BTREE_DEPTH_DOT) { 
      os << "Unknown("<<b.info.nodetype<<")";
    } else {
      os << "Unsupported Node Type " << b.info.nodetype ;
    }
  }
  if (dt==BTREE_DEPTH_DOT) { 
    os << "\" ]";
  }
  return ERROR_NOERROR;
}
  
ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}

ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
    // WRITE ME
    ERROR_T rc;
    BTreeNode root;
    root.Unserialize(buffercache,superblock.info.rootnode);

    // Variables to hold values in case we need to split the root node (Subcase 2B)
    VALUE_T temp;
    SIZE_T originalRoot = superblock.info.rootnode;
    SIZE_T newNode;
    KEY_T promotedKey;

    // CASE 1: Root is empty, nothing has been inserted yet
    if(root.info.numkeys == 0)
    {
      // Similar to Attach function. So we want to attach a leaf node to the root
      // I.e. When we insert our first item, let's make a leaf node to contain it,
      // so we don't have a tree with just a root with values.
      BTreeNode leaf(BTREE_LEAF_NODE,
        superblock.info.keysize,
        superblock.info.valuesize,
        buffercache->GetBlockSize());

      // Now let us allocate two leaf nodes to get the linked list started
      SIZE_T firstNode;
      SIZE_T secondNode;
      rc = AllocateNode(firstNode);
      if(rc){return rc;}
      rc = AllocateNode(secondNode);
      if(rc){return rc;}

      // Write the leaves to the disk
      leaf.Serialize(buffercache, firstNode);
      leaf.Serialize(buffercache, secondNode);
      // increment the number of keys in the root by 1 since we are adding a Key
      root.info.numkeys += 1;
      // Take care of setting the first key in root to our input key, and then
      // set the first key's value to point to the first leaf node. 
      root.SetKey(0,key);
      root.SetPtr(0,firstNode);
      // Set the next pointer (in the position right after the inserted value pointer) 
      // to point to the second leaf node.
      root.SetPtr(1,secondNode);
      // Now write the entire root node block to the disk
      root.Serialize(buffercache, superblock.info.rootnode);

    }

    // CASE 2: The key does not exist, so we can insert normally using SearchInternal2
    // First, we must check that the key does not exist in the Btree already, using Lookup
    if(Lookup(key,temp)==ERROR_NONEXISTENT)
    {
      // SUBCASE 2A: "Normal" insert. We do not have to split the root node
      // (Make a single call here to SearchInternal2, which handles all the recursion and value-placing)
      rc = SearchInternal2(superblock.info.rootnode, key, value, superblock.info.rootnode);

      // SUBCASE 2B: We need to split the root node 
      // idea: We can load the original root node data into two interior nodes, and then save
      // the two new interior nodes onto the disk, and then make a new root node, setting
      // its first key to the promoted key, and then setting its first pointer (0) to the 
      // left interior node of split and its second pointer (1) to the right interior node of split
      // (Sort of a similar idea to Case 1)
      if(NeedToSplit(superblock.info.rootnode))
      {
        SplitNode(originalRoot, newNode, promotedKey);
        // Similar to Attach function. So we want to initialize an interior node type
        // in order to hold the information previously in the root
        BTreeNode interior(BTREE_INTERIOR_NODE, 
          superblock.info.keysize,
          superblock.info.valuesize,
          buffercache->GetBlockSize());

        // Read the data from the original root (split over originalRoot and newNode from the SplitNode function)
        // Write the two new interior nodes to the disk, using the original root data
        interior.Unserialize(buffercache, originalRoot);
        interior.Serialize(buffercache, originalRoot);
        interior.Unserialize(buffercache, newNode);
        interior.Serialize(buffercache, newNode);




        // We want to allocate a new empty root node
        rc = AllocateNode(superblock.info.rootnode);
        if(rc){return rc;}

        // increment the number of keys in the root by 1 since we are adding a Key
        // Since this is the first key in the root node, just set numkeys to 1
        root.info.numkeys = 1;
        // Take care of setting the first key in root to our promoted key that we found from SplitNode, 
        // and then set the first key's value to point to the left interior node that used to be the root (originalRoot) 
        root.SetKey(0,promotedKey);
        root.SetPtr(0,originalRoot);
        // Set the next pointer (in the position right after the inserted value pointer) 
        // to point to the right interior node that used to be the root (newNode).
        root.SetPtr(1,newNode);
        // Now write the entire root node block to the disk
        root.Serialize(buffercache, superblock.info.rootnode);

      }
      return rc;


    }
    
    // If we reach this point, then the key must already exist in the tree. There is a conflict.
    // Instead, Update should be used to solve this issue and change the key's value instead of
    // Inserting a new key/value pair
    return ERROR_CONFLICT;
}

// Checks if a node needs to be split (i.e. it is full)
// Returns True if yes, False if no
bool BTreeIndex::NeedToSplit(const SIZE_T node)
{
  // WRITE ME
  BTreeNode b;
  b.Unserialize(buffercache, node);

  // If a node is completely full (i.e. the number keys = the number of slots in the node), return true
  switch(b.info.nodetype) {
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE:
      return (b.info.GetNumSlotsAsInterior() == b.info.numkeys);
    case BTREE_LEAF_NODE:
      return (b.info.GetNumSlotsAsLeaf() == b.info.numkeys);
  }
  // else return false
  return false;
}

// Splits a node and returns the node number for the new (second) node, 
// as well as the key to be promoted (moved up a level) by the split
ERROR_T BTreeIndex::SplitNode(const SIZE_T node, SIZE_T &secondNode, KEY_T &promotedKey)
  {
    // WRITE ME
    BTreeNode left; // "Old"/first node
    SIZE_T leftKeys;
    SIZE_T rightKeys;
    left.Unserialize(buffercache, node);
    BTreeNode right = left; // "New"/second node
    ERROR_T error;

    // Allocate and Serialize secondNode
    // If they do not evaluate to 0 (ERROR_NOERROR), return error
    if ((error = AllocateNode(secondNode)))
    {
      return error;
    }

    if ((error = right.Serialize(buffercache,secondNode)))
    {
      return error;
    }

    // If the node is a leaf node
    if (left.info.nodetype == BTREE_LEAF_NODE)
    {
      // ceiling of (n+1) / 2 [since leaf nodes have extra pointer at beginning]
      // n is the number of keys in the original node (left.info.numkeys)
      leftKeys = (left.info.numkeys + 2) / 2;
      rightKeys = left.info.numkeys - leftKeys; // remaining keys

      // The key to be promoted by the split
      left.GetKey(leftKeys - 1, promotedKey);

      // Find the location of the first key in the old (first) node to be moved
      // into the new (second) node
      char *oldLoc = left.ResolvePtr(leftKeys);
      char *newLoc = right.ResolvePtr(0); // first slot in new/second node
    
      // copy the keys from the old location into the new location
      // The amount will be the number of right keys times the summed size of a key and a value
      memcpy(newLoc, oldLoc, rightKeys * (left.info.keysize + left.info.valuesize));
    }

    // If the node is an interior or root node
    else 
    {
      // floor of n/2 
      leftKeys = (left.info.numkeys / 2);
      rightKeys = left.info.numkeys - leftKeys - 1; // promote one key

      // The key to be promoted by the split
      left.GetKey(leftKeys, promotedKey);
      
      // Find the location of the first key in the old (first) node to be moved
      // into the new (second) node
      char *oldLoc = left.ResolvePtr(leftKeys + 1);
      char *newLoc = right.ResolvePtr(0); // first slot in new/second node
      
      // copy the keys from the old location into the new location
      // The amount will be the number of right keys times the summed size of a key
      // plus two times the size of SIZE_T on the machine
      memcpy(newLoc, oldLoc, rightKeys * (left.info.keysize + sizeof(SIZE_T) + sizeof(SIZE_T)));
    }

    // Update the number of keys in the old and new nodes
    left.info.numkeys = leftKeys;
    right.info.numkeys = rightKeys;

    if ((error = left.Serialize(buffercache,node)))
    {
      return error;
    }

    // Write the new node into the disk
    return right.Serialize(buffercache,secondNode);
  }

  
ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME
  VALUE_T updateValue = value;
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, updateValue);
}

  
ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
  // This is optional extra credit 
  //
  // 
  return ERROR_UNIMPL;
}

ERROR_T BTreeIndex::SearchInternal(const SIZE_T &node,
             const KEY_T &key,
             const VALUE_T &value,
             KEY_T &promotedKey)  
{
  BTreeNode b; // the current node
  SIZE_T newnode = node; // may be used if need to allocate first leaf node
  BTreeNode n;  // may be used as new leaf node
  BTreeNode s; // may be used as the secondnode after split
  SIZE_T secondNode; // used for splitting
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;
  KEY_T tempKey;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey) {
  // OK, so we now have the first key that's larger
  // so we need to recurse on the ptr immediately previous to 
  // this one, if it exists
      rc=b.GetPtr(offset,ptr);
      if (rc) { return rc; }
      // check if a key needs to be promoted from leaf
      rc = SearchInternal(ptr, key, value, promotedKey);
      
      if(rc == ERROR_NOERROR) // if no insertion necessary, return no error
      {
        return rc;
      }
      // if there is a key to be promoted, split the node if necessary and insert the promoted key
      else
      {
        if(NeedToSplit(node))
        {
          tempKey = promotedKey;
          SplitNode(node, secondNode, promotedKey);
          if(tempKey<promotedKey) // then check in node where key needs to be inserted
          {
            for (offset=0;offset<b.info.numkeys;offset++) { 
              rc=b.GetKey(offset,testkey);
              if (rc) {  return rc; }
              if (tempKey<testkey) { //find the first key that is larger than the key to be inserted
                rc=b.GetPtr(offset,ptr);
                if (rc) { return rc; }
                rc = AddKeyVal(node, tempKey, VALUE_T(), ptr);
                if(rc != ERROR_NOERROR)
                {
                  return rc;
                }
                return ERROR_NOSPACE; // need to promote it
              }
            }
          }
          else  // check in secondNode where key needs to be inserted
          {
            rc = s.Unserialize(buffercache, secondNode);
            if (rc!=ERROR_NOERROR) { 
              return rc;
            }
            else
            {
              for (offset=0;offset<s.info.numkeys;offset++) { 
                rc=s.GetKey(offset,testkey);
                if (rc) {  return rc; }
                if (tempKey<testkey) { //find the first key that is larger than the key to be inserted
                rc = s.GetPtr(offset, ptr);
                if (rc) { return rc; }
                rc = AddKeyVal(secondNode, tempKey, VALUE_T(), ptr);
                if(rc != ERROR_NOERROR)
                {
                  return rc;
                }
                return ERROR_NOSPACE; // need to promote it
                }
              } 
            }
          }
        }
        else // no need to split the node, just insert
        {
          rc = AddKeyVal(node, promotedKey, VALUE_T(), ptr);
          if(rc != ERROR_NOERROR)
          {
            return rc;
          }
          return ERROR_NOERROR; // no need to promote it
        }
        }
      }
    }
  
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return SearchInternal(ptr,key,value, promotedKey);
    } 
    else {
      // there are no keys on the node, so this is the first insert. Need to make a leaf node too
      AllocateNode(newnode); 
      rc = n.Unserialize(buffercache, newnode);
      if (rc!=ERROR_NOERROR) { 
        return rc;
      }
      else
      {
        n.info.nodetype = BTREE_LEAF_NODE;      // make it a leaf node
      }
      rc = SearchInternal(newnode, key, value, promotedKey);
      if (rc!=ERROR_NOERROR) { 
        return rc;
      }
      else
      {
        AddKeyVal(node, promotedKey, VALUE_T(), ptr);
      }
    }
    break;
  case BTREE_LEAF_NODE:
    // check if this is the first key of the node
    if(b.info.numkeys == 0)
    {
      AddKeyVal(node, key, value, 0);
      promotedKey = key; // update the promoted key as the inserted key
      return ERROR_NOSPACE; // ***make a new ERROR_T***
    }
    
    // Otherwise check first if the node needs to be split
    if(NeedToSplit(node))
    {
      rc = SplitNode(node, secondNode, promotedKey); //this will update promotedKey
      if(rc != ERROR_NOERROR)
      {
        return rc;
      }
      if(key<promotedKey) // then check in node where key needs to be inserted
      {
            AddKeyVal(node, key, value, 0);
            return ERROR_NOSPACE; // need to promote it
      }
        
      
      else  // check in secondNode where key needs to be inserted
      {
        rc = s.Unserialize(buffercache, secondNode);
        if (rc!=ERROR_NOERROR) { 
          return rc;
        }
        else
        {
              AddKeyVal(secondNode, key, value, 0);
              return ERROR_NOSPACE; // need to promote it
            
          }
        }
      }
    
    else  // no need to split node, no promotion, just insert it
    {
        AddKeyVal(node, key, value, 0);
        return ERROR_NOERROR;
            
    }
    
    return ERROR_NONEXISTENT;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }  

  return ERROR_INSANE;
}

// Handles the recursive traversal of the tree, and placing the key/value pair in the correct node
ERROR_T BTreeIndex::SearchInternal2(SIZE_T node,
             const KEY_T &key,
             const VALUE_T &value,
             SIZE_T parentNode)  
{
  BTreeNode b; // the current node
  SIZE_T secondNode; 
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;
  KEY_T promotedKey;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey || key == testkey) {
  // OK, so we now have the first key that's larger
  // so we need to recurse on the ptr immediately previous to 
  // this one, if it exists
      rc=b.GetPtr(offset,ptr);
      if (rc) { return rc; }
      // check if a key needs to be promoted from leaf
      rc = SearchInternal2(ptr, key, value, node);
      
      if(rc){return rc;}
      // if there is a key to be promoted, split the node if necessary and insert the promoted key
      
        // check if we need to split the node
        if(NeedToSplit(ptr))
        {
          rc = SplitNode(ptr, secondNode, promotedKey);
          if(rc){return rc;}
          // Add the key/value (pointer) pair into an interior node
          return AddKeyVal(node, promotedKey, VALUE_T(), secondNode);

          
          
        }
        else // no need to split the node, just insert
        {
          return rc;
        }
        
      }
    }
  
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      rc = SearchInternal2(ptr,key,value, node);
      if (rc) {return rc;}
      if (NeedToSplit(ptr)){
        rc = SplitNode(ptr, secondNode, promotedKey);
        if (rc){return rc;}
        // Add the key/value (pointer) pair into an interior node
        return AddKeyVal(node, promotedKey, VALUE_T(), secondNode);
      } 
      else 
        {
          return rc;
        }
    } 
    else {
      // there are no keys on the node, so this is the first insert. Need to make a leaf node too
     return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Add a key/value pair into a leaf node
    return AddKeyVal(node,key,value,0);
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }  

  return ERROR_INSANE;
}

// This adds the new key/value pair to a node
ERROR_T BTreeIndex::AddKeyVal(const SIZE_T node, const KEY_T &key, const VALUE_T &value, SIZE_T newNode)
{
  BTreeNode b;
  b.Unserialize(buffercache, node);
  KEY_T testkey; // This will keep track of the key our current position in the loop
  SIZE_T numkeys = b.info.numkeys; // the number of keys in the node before we add the new key
  SIZE_T numToMove; // the number of keys we will need to move over to place our key
  SIZE_T offset; // This is our current position in the block
  SIZE_T pairSize; // The size of a key/value pair
  ERROR_T rc;

  // Check that we have a feasible node type
  switch(b.info.nodetype){
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE:
      pairSize = b.info.keysize + sizeof(SIZE_T);
      break;
    case BTREE_LEAF_NODE:
      pairSize = b.info.keysize + b.info.valuesize;
      break;
    default: // We can't be looking at anything other than a root, internal, or leaf
      return ERROR_INSANE;
  }

  // increase the number of keys in b by one, since we are adding a key
  b.info.numkeys++;

  // if the node already has keys in it
  if(numkeys > 0)
  {
    for (offset=0, numToMove = numkeys; offset < numkeys; offset++, numToMove--)
    {
      rc = b.GetKey(offset,testkey);
      if(rc)
      {
        return rc;
      }
      // If the key we want to add is less than the current key (testkey),
      // move the keys > key up by a position to make room for key. 
      // Place key into the current position
      if(key < testkey)
      {
        void *oldLoc = b.ResolveKey(offset);
        void *newLoc = b.ResolveKey(offset+1);
        memmove(newLoc,oldLoc,numToMove*pairSize);
        // if we have problems setting the key, return an error
        rc = b.SetKey(offset, key);
        if(rc){return rc;}
        // Do a few checks based on whether we are dealing with a root or interior node
        if(b.info.nodetype == BTREE_LEAF_NODE)
        {
          rc = b.SetVal(offset,value);
          if(rc){return rc;}
        }
        else // interior node
        {
          rc = b.SetPtr(offset+1,newNode);
          if(rc){return rc;}
        }
        break;
      }
      // If we get here, we have reached the final slot in the node
      // Therefore, key becomes the final key in the node
      if(offset == numkeys - 1)
      {
        rc = b.SetKey(numkeys, key);
        if(rc){return rc;}
        // Do a few checks based on whether we are dealing with a root or interior node
        if(b.info.nodetype == BTREE_LEAF_NODE)
        {
          rc = b.SetVal(numkeys,value);
          if(rc){return rc;}
        }
        else // interior node
        {
          rc = b.SetPtr(numkeys+1,newNode);
          if(rc){return rc;}
        }
        break;
      }
    }
  }
  else // numkeys == 0, so node is currently empty
  {
    rc = b.SetKey(0,key);
    if(rc){return rc;}
    rc = b.SetVal(0,value);
    if(rc){return rc;}
  }

  // Write the node back into the disk
  return b.Serialize(buffercache, node);

}

//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
				    ostream &o,
				    BTreeDisplayType display_type) const
{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  rc = PrintNode(o,node,b,display_type);
  
  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) { 
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0) { 
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (display_type==BTREE_DEPTH_DOT) { 
	  o << node << " -> "<<ptr<<";\n";
	}
	rc=DisplayInternal(ptr,o,display_type);
	if (rc) { return rc; }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    return ERROR_NOERROR;
    break;
  default:
    if (display_type==BTREE_DEPTH_DOT) { 
    } else {
      o << "Unsupported Node Type " << b.info.nodetype ;
    }
    return ERROR_INSANE;
  }

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "digraph tree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "}\n";
  }
  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheck() const
{
  // WRITE ME
  // values in leaf nodes are increasing
  // every pointer in interior nodes can be traced to a value in a leaf node
  // every pointer in a leaf node is in the data file (change if implement delete?) 
  // check that it is balanced
  // check valid use ratio of leaf - 1/2 full? 2/3?
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey1;
  KEY_T testkey2;
  SIZE_T ptr;
  VALUE_T value;


  rc = b.Unserialize(buffercache, superblock.info.rootnode);  // start at the root
  if (rc) {  return rc; }

  if(b.info.numkeys == 0) // if the tree is empty, it is fine
  {
    return ERROR_NOERROR;
  }

  rc = b.GetKey(0, testkey1);  // the first key in the root
  if (rc) {  return rc; }

  for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetPtr(offset,ptr);    // will return error if the pairs are not key-ptr
      if (rc) {  return rc; }
      rc = b.GetKey(offset, testkey2);
      if (rc) {  return rc; }
      if((!(offset == 0)) && ((testkey1 == testkey2) || !(testkey1 < testkey2))) // check that the keys are increasing and unique
      {
        return ERROR_BADCONFIG;
      }
      rc = b.GetKey(offset, testkey1);
      if (rc) {  return rc; }

      // check that the key exists with a value in a leaf
      rc = ConstLookup(ptr, testkey1);
      if (rc) {  return rc; }

      // check for errors on each of the children
      rc = SanityCheckRecurse(ptr, testkey1);
      if (rc) {  return rc; }
    }
  // if it made it this far there are no errors
  return ERROR_NOERROR;
}
  
ERROR_T BTreeIndex::SanityCheckRecurse(const SIZE_T node, const KEY_T key) const
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey1;
  KEY_T testkey2;
  SIZE_T ptr;
  VALUE_T value;
  ERROR_T rc1 = ERROR_BADCONFIG;

  rc = b.Unserialize(buffercache, node);
  if (rc) {  return rc; }

  rc = b.GetKey(0, testkey1);
  if (rc) {  return rc; }

  // the root node pointed to this node so there should be at least one key in it, and the first key should be the one from the parent
  if(b.info.numkeys == 0 || (key < testkey1))
  {
    return ERROR_BADCONFIG;
  }

  switch(b.info.nodetype){
    case BTREE_ROOT_NODE:
    // can't have two roots
      return ERROR_BADCONFIG;
    case BTREE_INTERIOR_NODE:
      for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetPtr(offset,ptr);    // will return error if the pairs are not key-ptr
      if (rc) {  return rc; }
      
      rc = b.GetKey(offset, testkey2);
      if (rc) {  return rc; }
      
      if((!(offset == 0)) && ((testkey1 == testkey2) || !(testkey1 < testkey2))) // check that the keys are increasing and unique
      {
        return ERROR_BADCONFIG;
      }
      
      rc = b.GetKey(offset, testkey1);  // update the comparison key
      if (rc) {  return rc; }

      // check that the key exists with a value in a leaf
      rc = ConstLookup(ptr, testkey1);
      if (rc) {  return rc; }

      // check for errors on each of the children
      rc = SanityCheckRecurse(ptr, testkey1);
      if (rc) {  return rc; }
    }
    //if it got here there are no errors
    return ERROR_NOERROR;
    break;
    
    case BTREE_LEAF_NODE:
      for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetVal(offset,value);    // will return error if the pairs are not key-value
      if (rc) {  return rc; }
      
      rc = b.GetKey(offset, testkey2);
      if (rc) {  return rc; }
      
      if((!(offset == 0)) && ((testkey1 == testkey2) || !(testkey1 < testkey2)))  // check that the keys are increasing and unique
      {
        return ERROR_BADCONFIG;
      }
      
      if(testkey2 == key)   // the key from the parent node must be somewhere in the leaf
      {
        rc1 = ERROR_NOERROR;
      }

      rc = b.GetKey(offset, testkey1);  // update the comparison key
      if (rc) {  return rc; }
    }
    // if the key from the parent was never found in the leaf, return an error
    if(rc1) {return rc1;}
    
    //if it got here there are no errors
    return ERROR_NOERROR;
    break;
    
    default:
    // can only be a root, interior, or leaf node
    return ERROR_INSANE;
    break;
  }
  return ERROR_INSANE;
}

ERROR_T BTreeIndex::ConstLookup(const SIZE_T node, const KEY_T key) const
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey || key==testkey) {
  // OK, so we now have the first key that's larger
  // so we need to recurse on the ptr immediately previous to 
  // this one, if it exists
  rc=b.GetPtr(offset,ptr);
  if (rc) { return rc; }
  return ConstLookup(ptr, key);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return ConstLookup(ptr,key);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) { 
        return ERROR_NOERROR;
      } 
      }
    
    return ERROR_NONEXISTENT;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }  

  return ERROR_INSANE;
}


ostream & BTreeIndex::Print(ostream &os) const
{
  // WRITE ME
  return os;
}




