(module dust.hash-store

;;;;; Exports ;;;;;

(;; Constants
 hash-size
 
 ;; Hash store
 hash-store-open
 hash-store-txn
 hash-store-hash-db
 hash-store-dirty-db
 hash-store-cursor
 hash-store-max-key-size
 ;; low-level but useful for testing:
 hashes-dbi-open
 dirty-dbi-open
 
 ;; Node
 node->blob
 blob->node
 make-node
 node?
 node-parent-key
 node-subkey
 node-key
 node-stored-hash
 node-hash
 node-leaf
 node-children
 node-first-child
 find-child-by-first-byte
 find-child-with-empty-subkey

 ;; Tree
 root-node
 search
 root-hash
 prefix-hash
 child-hashes
 leaf-hashes
 leaf-nodes

 ;; Hash store operations
 hash-get
 hash-put
 hash-delete
 rehash

 ;; Diff of two hash trees
 hash-diff
 hash-diff-accept
 hash-diff-accept-handle-message
)


;;;;; Dependencies ;;;;;

(import chicken scheme foreign)

(use lmdb-lolevel
     matchable
     bencode
     sodium
     srfi-4
     srfi-117
     lolevel
     ports
     lazy-seq
     data-structures
     dust.connection
     dust.u8vector-utils)

(foreign-declare "#include <lmdb.h>")
(foreign-declare "#include <string.h>")


;;;;; Constants ;;;;;

(define-constant HASHES_DBI_NAME "hashes")
(define-constant DIRTY_DBI_NAME "dirty")
(define-constant HASH_BYTES 20)

;; Node metadata includes hash + boolean leaf flag
(foreign-declare "#define META_BYTES 21")
(define-constant META_BYTES 21)

;; Redefine without using define-constant so it can be exported
(define hash-size HASH_BYTES)


;;;;; Utils ;;;;;

(define (hash-store-condition location args message)
  (make-composite-condition
   (make-property-condition
    'exn
    'location location
    'arguments args
    'message message)
   (make-property-condition 'dust)
   (make-property-condition 'hash-store)))

;; Resolve the entire lazy-sequence and return the final item. Assumes
;; at least one item in sequence.
(define (lazy-last seq)
  (let loop ((head (lazy-head seq))
	     (tail (lazy-tail seq)))
    (if (lazy-null? tail)
	head
	(loop (lazy-head tail)
	      (lazy-tail tail)))))

;; Like lazy-last, but returns the last two items from the sequence
;; via multiple return values. If there is only one item in the
;; sequence, the first of the two values will be #f
(define (lazy-last2 seq)
  (let loop ((prev #f)
	     (head (lazy-head seq))
	     (tail (lazy-tail seq)))
    (if (lazy-null? tail)
	(values prev head)
	(loop head (lazy-head tail) (lazy-tail tail)))))

;; Like lazy-map, but only true values are saved.
(define (lazy-filter-map f seq)
  (lazy-filter identity (lazy-map f seq)))

;; Positions subkey suitably in a blob so that it can be used for
;; look-ups via the custom sort function used in the hash store.
(define (offset-subkey subkey)
  (u8vector->blob/shared
   (u8vector-append (make-u8vector META_BYTES) subkey)))


;;;;; Hash store ;;;;;

(define-record hash-store txn hash-db dirty-db cursor)
(define-record-printer (store x port)
  (fprintf port "#<store txn:~S hash-db:~S dirty-db:~S cursor:~S>"
	   (hash-store-txn x)
	   (hash-store-hash-db x)
	   (hash-store-dirty-db x)
	   (hash-store-cursor x)))

;; Since the hash store uses LMDB's dupsort, with a custom comparator
;; for duplicates, the first few bytes are skipped over so only the
;; node's subkey is compared. This seems to eat into the overall key
;; limit. This function returns the reduced maximum key size for the
;; given environment when using the hash store.
(define (hash-store-max-key-size env)
  (- (mdb-env-get-maxkeysize env) META_BYTES))

(define (hash-store-open txn)
  (let ((hashes (hashes-dbi-open txn)))
    (make-hash-store txn
		     hashes
		     (dirty-dbi-open txn)
		     (mdb-cursor-open txn hashes))))

;; Custom comparator for nodes, which ignores the meta information at
;; the start of the serialized node blob and compares only the node's
;; subkey for sorting purposes
(foreign-declare
 "int hash_store_cmp(const MDB_val *a, const MDB_val *b) {
    size_t min_len = a->mv_size < b->mv_size ? a->mv_size : b->mv_size;
    int r = memcmp(
      &((char *)a->mv_data)[META_BYTES],
      &((char *)b->mv_data)[META_BYTES],
      min_len - META_BYTES
    );
    if (r == 0) {
      if (a->mv_size == b->mv_size) return 0;
      if (a->mv_size < b->mv_size) return -1;
      return 1;
    }
    return r;
  }")

;; Scheme reference to the custom C comparator function
(define hash_store_cmp
  (foreign-value "&hash_store_cmp"
		 (function int ((const (c-pointer (struct MDB_val)))
				(const (c-pointer (struct MDB_val)))))))

;; Opens hashes dbi with appropriate comparator and other config
;; options
(define (hashes-dbi-open txn)
  (let ((dbi (mdb-dbi-open txn
			   HASHES_DBI_NAME
			   (bitwise-ior MDB_DUPSORT MDB_CREATE))))
    (mdb-set-dupsort txn dbi hash_store_cmp)
    dbi))

;; Custom comparator for the dirty node markers. Sorts by length
;; (longest first) and for keys of equal length, uses memcmp for
;; consistent ordering. This ensures nodes futher down the tree are
;; re-hashed before re-hashing their parents.
(foreign-declare
 "int hash_store_dirty_cmp(const MDB_val *a, const MDB_val *b) {
    if (a->mv_size == b->mv_size) {
      return memcmp(a->mv_data, b->mv_data, a->mv_size);
    }
    return b->mv_size - a->mv_size;
  }")

;; Scheme reference to the custom C comparator function
(define hash_store_dirty_cmp
  (foreign-value "&hash_store_dirty_cmp"
		 (function int ((const (c-pointer (struct MDB_val)))
				(const (c-pointer (struct MDB_val)))))))

;; Opens dirty marker dbi with appropriate comparator and other config
;; options
(define (dirty-dbi-open txn)
  (let ((dbi (mdb-dbi-open txn DIRTY_DBI_NAME MDB_CREATE)))
    (mdb-set-compare txn dbi hash_store_dirty_cmp)
    dbi))


;;;;; Node ;;;;;

;; Represents a node in the hash tree, could be a leaf or non-leaf
;; node and stores a cached hash of it's child nodes
(define-record node parent-key subkey stored-hash leaf)
(define-record-printer (node x port)
  (fprintf port "#<node parent-key:~S subkey:~S stored-hash:~S leaf:~S>"
	   (node-parent-key x)
	   (node-subkey x)
	   (node-stored-hash x)
	   (node-leaf x)))

;; Node which represents the root of the tree (it's a non-leaf node,
;; where its parent key and subkey are set to an empty u8vector, and
;; it has no stored hash). This node does not get persisted to the
;; database and cannot be marked as dirty (as its hash is always
;; recalculated).
(define root-node
  (make-node #u8{} #u8{} #f #f))

;; Parse a blob of data previously read from LMDB into a node record
(define (blob->node parent blob)
  (when (< (blob-size blob) META_BYTES)
    (abort
     (hash-store-condition
      'blob->node
      (list blob)
      "Blob too small to parse as node")))
  (let* ((data (blob->u8vector/shared blob))
	 (leaf (not (fx= (u8vector-ref data 0) 0)))
	 (hash (u8vector->blob/shared (subu8vector data 1 (fx+ 1 HASH_BYTES))))
	 (subkey (subu8vector data (+ 1 HASH_BYTES) (u8vector-length data))))
    (make-node (node-key parent) subkey hash leaf)))

;; Serialize a node record as a blob suitable for writing to LMDB
(define (node->blob node)
  (unless (= HASH_BYTES (blob-size (node-stored-hash node)))
    (abort
     (hash-store-condition
      'node->blob
      (list node)
      (sprintf "Invalid hash size: expected ~S bytes, got ~S bytes"
	       HASH_BYTES
	       (blob-size (node-stored-hash node))))))
  (let* ((subkey-length (u8vector-length (node-subkey node)))
	 (subkey-offset (+ 1 HASH_BYTES))
	 (output (make-u8vector (+ 1 HASH_BYTES subkey-length))))
    (u8vector-set! output 0 (if (node-leaf node) 1 0))
    (move-memory! (node-stored-hash node) output HASH_BYTES 0 1)
    (move-memory! (node-subkey node) output subkey-length 0 subkey-offset)
    (u8vector->blob/shared output)))

;; Combines the node's parent-key and subkey to produce the full key
;; of the node
(define (node-key node)
  (u8vector-append (node-parent-key node) (node-subkey node)))

;; Checks node's children for a child with an empty subkey (used when
;; a key is both a prefix and a leaf, e.g. inserting 'test' and
;; 'tests' would result in 'test' being both a leaf and non-leaf node.
;; In this case the leaf node is inserted into the non-leaf 'test'
;; node with an empty subkey). If no child is found with an empty
;; subkey, it returns false.
(define (find-child-with-empty-subkey store node)
  (let ((cursor (hash-store-cursor store))
	(key (format-key (node-key node)))
	;; offset the duplicate data entry we're looking up at
	;; META_BYTES so the custom hash_store_cmp function reads
	;; the right part of the buffer
	(val (make-blob META_BYTES)))
    (condition-case
	(begin
	  (mdb-cursor-get cursor key val MDB_GET_BOTH)
	  (blob->node node (mdb-cursor-data cursor)))
      ((exn lmdb MDB_NOTFOUND) #f))))

;; Checks node's children for the first child with a matching first
;; byte. Each child should have a unique first byte since otherwise
;; the tree would insert a new node with the shared prefix of those
;; keys.
(define (find-child-by-first-byte store node first)
  (let ((cursor (hash-store-cursor store))
	(key (format-key (node-key node)))
	;; offset the duplicate data entry we're looking up at
	;; META_BYTES so the custom hash_store_cmp function reads
	;; the right part of the buffer
	(val (u8vector->blob/shared
	      (make-u8vector (+ META_BYTES 1) first))))
    (condition-case
	(begin
	  (mdb-cursor-get cursor key val MDB_GET_BOTH_RANGE)
	  (let ((child (blob->node node (mdb-cursor-data cursor))))
	    (and (= first (u8vector-ref (node-subkey child) 0))
		 child)))
      ((exn lmdb MDB_NOTFOUND) #f))))

;; Returns a hash for the given node. If it's a leaf node, this is
;; always the stored hash. If upper and/or lower bounds are provided
;; it will recursively calculate the hashes of it's children (only
;; recursing further when upper and lower bounds are prefixes of the
;; childs key). For non-leaf nodes without upper/lower bound it will
;; return the stored hash.
(define (node-hash store node #!optional lower-bound upper-bound)
  (if (node-leaf node)
      (values (node-stored-hash node)
	      (node-key node)
	      #t)
      (if (or lower-bound
	      upper-bound
	      (not (node-stored-hash node)))
	  (hash-children store node lower-bound upper-bound)
	  (values (node-stored-hash node)
		  (node-key node)
		  (node-leaf node)))))

;; Keep reading nodes from the cursor and moving onto the next dupsort
;; item until the end of the dupsort data is reached
(define (remaining-children cursor parent #!optional (first #t))
  (condition-case
      (begin
	(unless first
	  (mdb-cursor-get cursor #f #f MDB_NEXT_DUP))
	(lazy-seq
	 (cons (blob->node parent (mdb-cursor-data cursor))
	       (remaining-children cursor parent #f))))
    ;; stop when end of duplicates is hit
    ((exn lmdb MDB_NOTFOUND) lazy-null)))

;; Returns children which fall inside parent's upper/lower bounds, or
;; which may have their own children which fall inside parent's
;; upper/lower bounds. If no upper/lower bounds are provided, returns
;; all children. NOTE: this means that some children returned by this
;; function may not be relevant to a bounds query, but there will be
;; no way to be sure until all children have been recursively
;; searched.
(define (node-children store parent #!optional lower-bound upper-bound)
  (let* ((key (node-key parent))
	 (key-len (u8vector-length key))
	 ;; trim parent's subkey from start of boundaries
	 (child-lower
	  (and lower-bound
	       (u8vector-prefix? key lower-bound)
	       (u8vector-drop lower-bound key-len)))
	 (child-upper
	  (and upper-bound
	       (u8vector-prefix? key upper-bound)
	       (u8vector-drop upper-bound key-len))))
    (call/cc
     (lambda (k)
       (let ((cursor (mdb-cursor-open (hash-store-txn store)
				      (hash-store-hash-db store))))
	 ;; position cursor
	 (condition-case
	     (begin
	       ;; find dupsort container
	       (mdb-cursor-get cursor
			       (format-key (node-key parent))
			       #f
			       MDB_SET_KEY)
	       (if (and child-lower (> (u8vector-length child-lower) 0))
		   ;; if parent's lower bound is set, find first
		   ;; child with subkey >= first byte of parent's
		   ;; lower bound. only compare first byte as the
		   ;; subkey may only be a prefix, but it's children
		   ;; may still fall within bounds
		   (mdb-cursor-get cursor
				   #f
				   (offset-subkey
				    (u8vector-truncate child-lower 1))
				   MDB_GET_BOTH_RANGE)
		   ;; otherwise, position at first child node
		   (mdb-cursor-get cursor #f #f MDB_FIRST_DUP)))
	   ;; if the start point is not found, there are no children
	   ((exn lmdb MDB_NOTFOUND) (k lazy-null)))
	 ;; return all remaining nodes within upper and lower bounds
	 (lazy-take-while
	  (lambda (child)
	    (or (not child-upper)
		(u8vector<=? (node-subkey child) child-upper)))
	  (lazy-drop-while
	   (lambda (child)
	     (and child-lower
		  (u8vector<? (node-subkey child) child-lower)
		  (not (and (not (node-leaf child))
			    (u8vector-prefix? (node-subkey child)
					      child-lower)))))
	   (remaining-children cursor parent))))))))

;; LMDB does not allow us to insert empty keys (e.g. for the root), so
;; all keys need to be prefixed to give us the equivalent of an empty
;; key. This procedure prefixes '/' to the key and returns it as a
;; blob, suitable for passing to lmdb-lolevel procedures.
(define (format-key key)
  (u8vector->blob/shared (u8vector-append #u8{47} key)))

;; Inserts a new child node under the parent-key. Will raise an
;; exception if an update of an existing subkey is attempted.
(define (insert-child store parent-key subkey hash leaf)
  (mdb-cursor-put (hash-store-cursor store)
		  (format-key parent-key)
		  (node->blob (make-node parent-key
					 subkey
					 hash
					 leaf))
		  MDB_NODUPDATA))

;; Same as insert-child, but allows overwriting of existing subkeys
(define (update-child store parent-key subkey hash leaf)
  (mdb-cursor-put (hash-store-cursor store)
		  (format-key parent-key)
		  (node->blob (make-node parent-key
					 subkey
					 hash
					 leaf))
		  0))

;; Remove a specific child node by subkey
(define (delete-child store parent-key subkey)
  (mdb-cursor-get (hash-store-cursor store)
		  (format-key parent-key)
		  (offset-subkey subkey)
		  MDB_GET_BOTH)
  (mdb-cursor-del (hash-store-cursor store) 0))

;; Swaps old subkey with new. If the new and old subkeys are the same
;; it will use update-child to update the hash and leaf flag on the
;; existing child node. Otherwise it will insert a new child node and
;; delete the child with the old subkey.
(define (replace-child store parent-key old-subkey new-subkey hash leaf)
  (if (equal? old-subkey new-subkey)
      (update-child store parent-key old-subkey hash leaf)
      (begin
	(insert-child store parent-key new-subkey hash leaf)
	(delete-child store parent-key old-subkey))))

;; Returns the total number of child nodes stored under a parent key.
(define (child-count store parent-key)
  (let ((key (format-key parent-key)))
    (condition-case
	(begin
	  (mdb-cursor-get (hash-store-cursor store) key #f MDB_SET_KEY)
	  (mdb-cursor-count (hash-store-cursor store)))
      ((exn lmdb MDB_NOTFOUND) 0))))

;; Returns the first child (sorted by subkey) of a parent node
(define (node-first-child store parent)
  (let ((cursor (hash-store-cursor store)))
    (mdb-cursor-get cursor (format-key (node-key parent)) #f MDB_SET_KEY)
    (blob->node parent (mdb-cursor-data cursor))))

;; Return the hash of each child node's node-hash (within bounds), if
;; only one child is present return it's hash directly, if no children
;; are within bounds return false.
(define (hash-children store parent #!optional lower-bound upper-bound)
  (let* ((children (node-children store parent lower-bound upper-bound))
	 (hashes
	  (lazy-filter-map
	   (lambda (child)
	     (let-values (((hash prefix leaf)
			   (node-hash store child lower-bound upper-bound)))
	       (and hash (list hash prefix leaf))))
	   children)))
    (cond
     ;; no children in range
     ((lazy-null? hashes)
      (values #f #f #f))
     ;; only one child in range, this level wouldn't exist for this
     ;; range, so return the child hash directly
     ((lazy-null? (lazy-tail hashes))
      (apply values (lazy-head hashes)))
     ;; multiple child hashes, return a hash of these hashes
     (else
      (let ((hash-state (generic-hash-init size: HASH_BYTES)))
	(lazy-each (cut generic-hash-update hash-state <>)
		   (lazy-map car hashes))
	(values (generic-hash-final hash-state)
		(node-key parent)
		#f))))))


;;;;; Tree ;;;;;

;; Move onto the next node in the search
(define (next-node store target leaf node)
  (if (not node)
      root-node
      (if (= (u8vector-length target) 0)
	  (and leaf (find-child-with-empty-subkey store node))
	  (find-child-by-first-byte store node (u8vector-ref target 0)))))

;; Walks the tree looking for a target key, emits a result of (type .
;; node) for each step taken. Type can be a prefix-match (and exact
;; matching prefix while recursing), prefix-partial (a partially
;; matching prefix, which ends the search) or an exact match (a leaf
;; node fully matching the target key). If the leaf flag is set it
;; will recurse until a match fails or a leaf is found, if not it will
;; stop as soon as the full key has been matched, even if there may
;; also be a leaf node associated with this key via a child with an
;; empty subkey.
(define (search store target leaf #!optional node)
  (let ((child (next-node store target leaf node)))
    (cond
     ;; no matching child, stop search
     ((not child) lazy-null)
     ;; child is a leaf node, do not recurse
     ((node-leaf child)
      (list->lazy-seq
       ;; check if we've fully matched target
       (if (u8vector=? (node-subkey child) target)
	   `((exact-match . ,child))
	   `((prefix-partial . ,child)))))
     ;; if child's subkey is a prefix of our target then recurse
     ((u8vector-prefix? (node-subkey child) target)
      (let* ((subkey-len (u8vector-length (node-subkey child)))
	     (next-target (u8vector-drop target subkey-len)))
	(lazy-seq
	 `((prefix-match . ,child) .
	   ,(search store next-target leaf child)))))
     ;; the child's subkey must have at least its first byte in common
     ;; with target, return child as partial match
     (else
      (list->lazy-seq `((prefix-partial . ,child)))))))
      

;; Returns a hash for all entries below a given prefix key (optionally
;; within given upper and lower bounds).
(define (prefix-hash store prefix #!optional lower-bound upper-bound)
  (rehash store)
  (let* ((result (lazy-last (search store prefix #f)))
	 (node (cdr result))
	 (key (node-key node))
	 (key-len (u8vector-length key)))
    (node-hash store node lower-bound upper-bound)))

;; Returns a lazy-seq of child keys and their hashes for a given
;; prefix (optionally within bounds). Each result is a vector
;; containing the full subkey, the hash and the leaf flag of each child.
(define (child-hashes store prefix #!optional lower-bound upper-bound)
  (rehash store)
  (let* ((result (lazy-last (search store prefix #f)))
	 (node (cdr result)))
    (lazy-filter-map
     (lambda (child)
       (let-values (((hash key leaf)
		     (node-hash store child lower-bound upper-bound)))
	 (and hash (vector key hash leaf))))
     (node-children store node lower-bound upper-bound))))

(define (leaf-nodes store parent #!optional lower-bound upper-bound)
  (lazy-append-map
   (lambda (x)
     (if (node-leaf x)
	 (list->lazy-seq (list x))
	 (leaf-nodes store x lower-bound upper-bound)))
   (node-children store parent lower-bound upper-bound)))

(define (leaf-hashes store prefix #!optional lower-bound upper-bound)
  (rehash store)
  (let* ((result (lazy-last (search store prefix #f)))
	 (node (cdr result)))
    (lazy-map
     (lambda (x)
       (let-values (((hash key leaf)
		     (node-hash store x lower-bound upper-bound)))
	 (vector key hash leaf)))
     (leaf-nodes store node lower-bound upper-bound))))
     


;;;;; Store operations ;;;;;

;; Get a specific hash from the store for a given key. Only for exact
;; matching leaf nodes.
(define (hash-get store key)
  (let ((result (lazy-last (search store (blob->u8vector/shared key) #t))))
    (if (eq? (car result) 'exact-match)
	(node-hash store (cdr result))
	(abort
	 (hash-store-condition
	  'hash-get
	  (list store key)
	  "Key not found")))))

;; Split a node using a new key which shares a common prefix.
(define (split-node store node key hash)
  (receive (common old-rest new-rest)
      (u8vector-shared-prefix (node-subkey node) key)
    (when (= (u8vector-length common) 0)
      (abort
       (hash-store-condition
	'split-node
	(list store node key hash)
	"Cannot split node, no common bytes with provided key")))
    (let ((placeholder-hash (make-blob HASH_BYTES))
	  (new-parent-key (u8vector-append (node-parent-key node) common)))
      ;; replace this original node in it's parent with a new non-leaf node
      ;; using common characters as its subkey
      (replace-child store
		     (node-parent-key node)
		     (node-subkey node)
		     common
		     placeholder-hash
		     #f)
      ;; insert new subkey as child of new parent
      (insert-child store
		    new-parent-key
		    new-rest
		    hash
		    #t)
      ;; copy original node to children of new parent
      (insert-child store
		    new-parent-key
		    old-rest
		    (node-stored-hash node)
		    (node-leaf node))
      new-parent-key)))

;; Insert a new hash into the store under the given key
(define (hash-put store key hash)
  (let* ((target (blob->u8vector/shared key))
	 (result (lazy-last (search store target #t)))
	 (type (car result))
	 (node (cdr result)))
    (case type
      ((exact-match)
       (update-child store
		     (node-parent-key node)
		     (node-subkey node)
		     hash
		     #t)
       (mark-dirty store (node-parent-key node)))
      ((prefix-match)
       (let* ((parent-key (node-key node))
	      (remaining (u8vector-drop
			  target
			  (u8vector-length parent-key))))
	 (insert-child store
		       parent-key
		       remaining
		       hash
		       #t)
	 (mark-dirty store parent-key)))
      ((prefix-partial)
       (let* ((remaining (u8vector-drop
			  target
			  (u8vector-length (node-parent-key node))))
	      (parent-key (split-node store
				      node
				      remaining
				      hash)))
	 (mark-dirty store parent-key))))))

;; Join a parent node with a single remaining child node, combining
;; their subkeys.
(define (collapse-node store parent)
  (let ((child (node-first-child store parent)))
    (if (= (u8vector-length (node-subkey child)) 0)
	;; if child subkey is empty, we have to handle keys slightly differently
	;; since a bucket with the joined parent + child subkeys will already exist
	(update-child store
		      (node-parent-key parent)
		      (node-subkey parent)
		      (node-stored-hash child)
		      (node-leaf child))
	(let ((joined-subkey (u8vector-append
			      (node-subkey parent) (node-subkey child))))
	  (insert-child store
			(node-parent-key parent)
			joined-subkey
			(node-stored-hash child)
			(node-leaf child))
	  (delete-child store
			(node-parent-key parent)
			(node-subkey parent))))
    (delete-child store
    		(node-parent-key child)
    		(node-subkey child))
    (mark-clean store (node-parent-key child))
    (mark-dirty store (node-parent-key parent))))

(define (hash-delete store key)
  (let* ((target (blob->u8vector/shared key))
	 (results (search store target #t)))
    (receive (parent-result result) (lazy-last2 results)
      (if (equal? (car result) 'exact-match)
	  (let ((node (cdr result)))
	    (delete-child store
			  (node-parent-key node)
			  (node-subkey node))
	    (mark-dirty store
			(node-parent-key node))
	    ;; collapse non-root nodes with only 1 remaining child
	    (let ((parent (and parent-result (cdr parent-result))))
	      (when (and parent
			 (not (equal? parent root-node))
			 (= 1 (child-count store (node-parent-key node))))
		(collapse-node store parent))))
	  (abort
	   (hash-store-condition
	    'hash-delete
	    (list store key)
	    "Key not found"))))))

;; Add a key to the dirty marker db
(define (mark-dirty store key)
  ;; check it's not the root node
  (unless (= (u8vector-length key) 0)
    (mdb-put (hash-store-txn store)
	     (hash-store-dirty-db store)
	     (u8vector->blob/shared key)
	     #${1}
	     0)))

;; Remove a key to the dirty marker db
(define (mark-clean store key)
  ;; check it's not the root node
  (unless (= (u8vector-length key) 0)
    (mdb-del (hash-store-txn store)
	     (hash-store-dirty-db store)
	     (u8vector->blob/shared key))))

;; Calculate the root hash for the store, or for a range of keys
(define (root-hash store #!optional lower-bound upper-bound)
  (rehash store)
  (node-hash store root-node lower-bound upper-bound))

;; Re-calculate and store the hash for a given node (does not
;; recalculate recursively, it assumes child nodes are up to date)
(define (rehash-node store key)
  (let* ((target (blob->u8vector/shared key))
	 (result (lazy-last (search store target #f)))
	 (type (car result))
	 (node (cdr result)))
    (when (node-leaf node)
      (abort
       (hash-store-condition
	'rehash-node
	(list store target)
	"Cannot re-hash leaf nodes")))
    (let-values (((hash key leaf) (hash-children store node)))
      (update-child store
		    (node-parent-key node)
		    (node-subkey node)
		    hash
		    #f))
    (mark-clean store target)
    (mark-dirty store (node-parent-key node))))

;; Recalculate hashes and all parent hashes for keys marked as
;; 'dirty', starting with the longest keys and working back up to the
;; root.
(define (rehash store)
  (let ((cursor (mdb-cursor-open (hash-store-txn store)
				 (hash-store-dirty-db store))))
    (condition-case
	(let loop ()
	  (mdb-cursor-get cursor #f #f MDB_FIRST)
	  (rehash-node store (mdb-cursor-key cursor))
	  (loop))
      ((exn lmdb MDB_NOTFOUND) #t))))





(define (lazy-filter-map f seq)
  (lazy-filter identity (lazy-map f seq)))

;; (define (lazy-join seqs)
;;   (if (lazy-null? seqs)
;;       lazy-null
;;       (lazy-append (lazy-head seqs) (lazy-tail seqs))))


(define (hash-diff store conn #!optional lower upper)
  ;; start by add request for children of root node to queue
  (list-queue-add-front!
   (connection-pending conn)
   (delay (compare-children store conn #u8{} lower upper)))
  ;; take values from queue while available and join all output into
  ;; a single lazy sequence
  (let loop ((q (connection-pending conn)))
    (lazy-seq
     (if (not (list-queue-empty? q))
	 (begin
	   (let ((results (force (list-queue-remove-front! q))))
	     (lazy-append results (loop q))))
	 (begin
	   '())))))

(define (compare-children store conn prefix lower upper)
  (diff-children store
		 conn
		 (child-hashes store prefix lower upper)
		 (request-child-hashes conn prefix lower upper)
		 lower
		 upper))

(define (mark-leaf-nodes-new conn prefix lower upper)
  (lazy-map
   (match-lambda (#(key-r hash-r leaf-r)
		  (vector 'new (u8vector->blob key-r) hash-r)))
   (request-leaf-hashes conn prefix lower upper)))

(define (mark-leaf-nodes-missing store prefix lower upper)
  (lazy-map
   (match-lambda (#(key-l hash-l leaf-l)
		  (vector 'missing (u8vector->blob key-l) #f)))
   (leaf-hashes store prefix lower upper)))

(define (diff-children store conn local remote lower upper)
  (cond
   ((lazy-null? local)
    ;; all remaining remote children are not in local store
    (lazy-filter-map
     (match-lambda
       (#(key-r hash-r leaf-r)
	(if leaf-r
	    (vector 'new (u8vector->blob key-r) hash-r)
	    (begin
	      (list-queue-add-back!
	       (connection-pending conn)
	       (delay (mark-leaf-nodes-new conn key-r lower upper)))
	      #f))))
     remote))
   ((lazy-null? remote)
    ;; all remaining local children are not in remote store
    (lazy-filter-map
     (match-lambda
       (#(key-l hash-l leaf-l)
	(if leaf-l
	    (vector 'missing (u8vector->blob key-l) #f)
	    (begin
	      (list-queue-add-back!
	       (connection-pending conn)
	       (delay (mark-leaf-nodes-missing store key-l lower upper)))
	      #f))))
     local))
   (else
    (match-let ((#(key-l hash-l leaf-l) (lazy-head local))
		(#(key-r hash-r leaf-r) (lazy-head remote)))
      (cond
       ((u8vector=? key-l key-r)
	(cond
	 ((and leaf-l leaf-r)
	  (if (blob=? hash-l hash-r)
	      (diff-children store
			     conn
			     (lazy-tail local)
			     (lazy-tail remote)
			     lower
			     upper)
	      (lazy-seq
	       (cons (vector 'different (u8vector->blob key-l) hash-r)
		     (diff-children store
				    conn
				    (lazy-tail local)
				    (lazy-tail remote)
				    lower
				    upper)))))
	 ((and (not leaf-l) (not leaf-r))
	  (if (blob=? hash-l hash-r)
	      (diff-children store
			     conn
			     (lazy-tail local)
			     (lazy-tail remote)
			     lower
			     upper)
	      (begin
		(list-queue-add-back!
		 (connection-pending conn)
		 (delay (compare-children store conn key-l lower upper)))
		(diff-children store
			       conn
			       (lazy-tail local)
			       (lazy-tail remote)
			       lower
			       upper))))
	 ((and leaf-l (not leaf-r))
	  ;; treat it like R is a prefix of L:
	  ;; compare leaf hashes of R to the single hash of L
	  (list-queue-add-back!
	   (connection-pending conn)
	   (delay (diff-leaf-hashes
		   (list->lazy-seq
		    (list (vector key-l hash-l leaf-l)))
		   (request-leaf-hashes conn key-r lower upper))))
	  (diff-children store
			 conn
			 (lazy-tail local)
			 (lazy-tail remote)
			 lower
			 upper))
	 (else ;; (and (not leaf-l) leaf-r)
	  ;; treat it like L is prefix of R:
	  ;; compare leaf hashes of L to the single hash of R
	  (list-queue-add-back!
	   (connection-pending conn)
	   (delay (diff-leaf-hashes
		   (leaf-hashes store key-l lower upper)
		   (list->lazy-seq
		    (list (vector key-r hash-r leaf-r))))))
	  (diff-children store
			 conn
			 (lazy-tail local)
			 (lazy-tail remote)
			 lower
			 upper))))
       ((u8vector<? key-l key-r)
	(if (u8vector-prefix? key-l key-r)
	    (if leaf-l
		(lazy-seq
		 (cons (vector 'missing (u8vector->blob key-l) #f)
		       (diff-children store
				      conn
				      (lazy-tail local)
				      remote
				      lower
				      upper)))
		(if leaf-r
		    (begin
		      ;; compare leaf hashes of L to the single hash of R
		      (list-queue-add-back!
		       (connection-pending conn)
		       (delay (diff-leaf-hashes
			       (leaf-hashes store key-l lower upper)
			       (list->lazy-seq
				(list (vector key-r hash-r leaf-r))))))
		      (diff-children store
				     conn
				     (lazy-tail local)
				     (lazy-tail remote)
				     lower
				     upper))
		    (begin
		      ;; compare children of L to a sequence containing
		      ;; the single child R
		      (list-queue-add-back!
		       (connection-pending conn)
		       (delay (diff-children
			       store
			       conn
			       (child-hashes store key-l lower upper)
			       (list->lazy-seq
				(list (vector key-r hash-r leaf-r)))
			       lower
			       upper)))
		      (diff-children store
				     conn
				     (lazy-tail local)
				     (lazy-tail remote)
				     lower
				     upper))))
	    (if leaf-l
		(lazy-seq
		 (cons (vector 'missing (u8vector->blob key-l) #f)
		       (diff-children
			store
			conn
			(lazy-tail local)
			remote
			lower
			upper)))
		(begin
		  (list-queue-add-back!
		   (connection-pending conn)
		   (delay (mark-leaf-nodes-missing store key-l lower upper)))
		  (diff-children store
				 conn
				 (lazy-tail local)
				 remote
				 lower
				 upper)))))
       ((u8vector>? key-l key-r)
	(if (u8vector-prefix? key-r key-l)
	    (if leaf-r
		(lazy-seq
		 (cons (vector 'new (u8vector->blob key-r) hash-r)
		       (diff-children store
				      conn
				      local
				      (lazy-tail remote)
				      lower
				      upper)))
		(if leaf-l
		    (begin
		      ;; compare leaf hashes of R to the single hash of L
		      (list-queue-add-back!
		       (connection-pending conn)
		       (delay (diff-leaf-hashes
			       (list->lazy-seq
				(list (vector key-l hash-l leaf-l)))
			       (request-leaf-hashes conn key-r lower upper))))
		      (diff-children store
				     conn
				     (lazy-tail local)
				     (lazy-tail remote)
				     lower
				     upper))
		    (begin
		      ;; compare children of R to a sequence containing
		      ;; the single child L
		      (list-queue-add-back!
		       (connection-pending conn)
		       (delay (diff-children
			       store
			       conn
			       (list->lazy-seq
				(list (vector key-l hash-l leaf-l)))
			       (request-child-hashes conn key-r lower upper)
			       lower
			       upper)))
		      (diff-children store
				     conn
				     (lazy-tail local)
				     (lazy-tail remote)
				     lower
				     upper))))
	    (if leaf-r
		(lazy-seq
		 (cons (vector 'new (u8vector->blob key-r) hash-r)
		       (diff-children store
				      conn
				      local
				      (lazy-tail remote)
				      lower
				      upper)))
		(begin
		  (list-queue-add-back!
		   (connection-pending conn)
		   (delay (mark-leaf-nodes-new conn key-r lower upper)))
		  (diff-children store
				 conn
				 local
				 (lazy-tail remote)
				 lower
				 upper))))))))))

(define (diff-leaf-hashes local remote)
  (cond
   ((lazy-null? local)
    (lazy-map
     (match-lambda (#(key-r hash-r leaf-r)
		    (vector 'new (u8vector->blob key-r) hash-r)))
     remote))
   ((lazy-null? remote)
    (lazy-map
     (match-lambda (#(key-l hash-l leaf-l)
		    (vector 'missing (u8vector->blob key-l) #f)))
     local))
   (else
    (match-let ((#(key-l hash-l leaf-l) (lazy-head local))
		(#(key-r hash-r leaf-r) (lazy-head remote)))
      (cond
       ((u8vector=? key-l key-r)
	(if (blob=? hash-l hash-r)
	    (diff-leaf-hashes (lazy-tail local) (lazy-tail remote))
	    (lazy-seq
	     (cons (vector 'different (u8vector->blob key-l) hash-r)
		   (diff-leaf-hashes (lazy-tail local) (lazy-tail remote))))))
       ((u8vector<? key-l key-r)
	(lazy-seq
	 (cons (vector 'missing (u8vector->blob key-l) #f)
	       (diff-leaf-hashes (lazy-tail local) remote))))
       ((u8vector>? key-l key-r)
	(lazy-seq
	 (cons (vector 'new (u8vector->blob key-r) hash-r)
	       (diff-leaf-hashes local (lazy-tail remote))))))))))

(define (request-leaf-hashes conn prefix lower upper)
  (write-bencode (vector "LEAF-HASHES"
			 (u8vector->string prefix)
			 (if lower (u8vector->string lower) 0)
			 (if upper (u8vector->string upper) 0))
		 (connection-out conn))
  (match (receive-bencode conn)
    ;; TODO: use ,prefix instead of __prefix_ to sanity check returned values?
    (#("LEAF-HASHES" __prefix__ children)
     (read-children prefix children))))

(define (request-child-hashes conn prefix lower upper)
  (write-bencode (vector "PREFIX"
			 (u8vector->string prefix)
			 (if lower (u8vector->string lower) 0)
			 (if upper (u8vector->string upper) 0))
		 (connection-out conn))
  (match (receive-bencode conn)
    ;; TODO: use ,prefix instead of __prefix_ to sanity check returned values?
    (#("HASHES" __prefix__ children)
     (read-children prefix children))))

(define (read-children prefix children)
  (lazy-map
   (match-lambda
       (#(subkey hash leaf)
	(vector (u8vector-append prefix (string->u8vector subkey))
		(string->blob hash)
		(not (= 0 leaf)))))
   (list->lazy-seq (vector->list children))))


(define (hash-diff-accept-handle-message store conn msg)
  (match msg
    (#("PREFIX" prefix lower upper)
     (let ((lower (and (not (equal? 0 lower))
		       (string->u8vector lower)))
	   (upper (and (not (equal? 0 upper))
		       (string->u8vector upper))))
       (write-child-hashes store
			   conn
			   (string->u8vector prefix)
			   lower
			   upper)))
    (#("LEAF-HASHES" prefix lower upper)
     (let ((lower (and (not (equal? 0 lower))
		       (string->u8vector lower)))
	   (upper (and (not (equal? 0 upper))
		       (string->u8vector upper))))
       (write-leaf-hashes store
			  conn
			  (string->u8vector prefix)
			  lower
			  upper)))))

(define (hash-diff-accept store conn)
  (let loop ()
    (and-let* ((data (receive-bencode conn)))
      (hash-diff-accept-handle-message store conn data)
      (loop))))

(define (write-child-hashes store conn prefix lower upper)
  (with-output-to-port (connection-out conn)
    (lambda ()
      (write-char #\l)
      (write-bencode "HASHES")
      (write-bencode (u8vector->string prefix))
      (write-char #\l)
      (lazy-each
       (match-lambda
	   (#(key hash leaf)
	    (write-bencode
	     (vector (u8vector->string
		      (u8vector-drop key (u8vector-length prefix)))
		     (blob->string hash)
		     (if leaf 1 0))
	     )))
       (child-hashes store prefix lower upper))
      (write-char #\e)
      (write-char #\e))))

(define (write-leaf-hashes store conn prefix lower upper)
  (with-output-to-port (connection-out conn)
    (lambda ()
      (write-char #\l)
      (write-bencode "LEAF-HASHES")
      (write-bencode (u8vector->string prefix))
      (write-char #\l)
      (lazy-each
       (match-lambda
	   (#(key hash leaf)
	    (write-bencode
	     (vector (u8vector->string
		      (u8vector-drop key (u8vector-length prefix)))
		     (blob->string hash)
		     (if leaf 1 0))
	     )))
       (leaf-hashes store prefix lower upper))
      (write-char #\e)
      (write-char #\e))))

)
