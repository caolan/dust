(module dust.kv-store

;;;;; Exports ;;;;;

(kv-store-open
 kv-store-txn
 kv-store-dbi
 kv-store-hash-store
 with-kv-store
 kv-put
 kv-get
 kv-delete
 kv-pairs
 kv-env-sync
 kv-env-sync-accept
 )

;;;;; Dependencies ;;;;;

(import chicken scheme)

(use lmdb-lolevel
     lazy-seq
     sodium
     bencode
     matchable
     miscmacros
     data-structures
     dust.connection
     dust.hash-store)

(define-constant DBI_NAME "kv")

(define-record kv-store txn dbi hash-store)

(define (kv-store-open txn)
  (make-kv-store
   txn
   (mdb-dbi-open txn DBI_NAME MDB_CREATE)
   (hash-store-open txn)))

(define (with-kv-store env flags thunk)
  (let* ((txn (mdb-txn-begin env #f flags))
	 (store (kv-store-open txn)))
    (handle-exceptions exn (begin (mdb-txn-abort txn)
				  (abort exn))
		       (begin0
			   (thunk store)
			 (mdb-txn-commit txn)))))

(define (kv-get store key)
  (mdb-get (kv-store-txn store)
	   (kv-store-dbi store)
	   key))

(define (kv-put store key data)
  (mdb-put (kv-store-txn store)
	   (kv-store-dbi store)
	   key
	   data
	   0)
  (hash-put (kv-store-hash-store store)
	    key
	    (generic-hash data size: hash-size)))

(define (kv-delete store key)
  (mdb-del (kv-store-txn store)
	   (kv-store-dbi store)
	   key)
  (hash-delete (kv-store-hash-store store) key))

(define (kv-pairs* cursor)
  (let loop ((op MDB_FIRST))
    (condition-case
	(begin
	  (mdb-cursor-get cursor #f #f op)
	  (lazy-seq
	   (cons (cons (mdb-cursor-key cursor)
		       (mdb-cursor-data cursor))
		 (loop MDB_NEXT))))
      ((exn lmdb MDB_NOTFOUND) lazy-null))))

(define (kv-pairs store)
  (kv-pairs*
   (mdb-cursor-open (kv-store-txn store)
		    (kv-store-dbi store))))



(define (kv-env-rehash env)
  (with-kv-store env 0 (compose rehash kv-store-hash-store)))

(define (request-data conn key)
  (write-bencode (vector "GET" (blob->string key))
		 (connection-out conn))
  (match (receive-bencode conn)
    (#("DATA" _ data)
     (string->blob data))))

;; The default sync change handler will attmept to match the remote
;; store exactly, including deleting local keys which don't exist in
;; the remote store
(define (default-change-handler store conn type key remote-hash)
  (case type
    ((missing)
     (kv-delete store key))
    ((new)
     (let ((data (request-data conn key)))
       (kv-put store key data)))
    ((different)
     (let ((data (request-data conn key)))
       (kv-put store key data)))))

;; protocol versions supported by this implementation
(define supported-versions #(1))

;; TODO: bounded sync
(define (kv-env-sync env conn #!key
		     (change-handler default-change-handler)
		     lower-bound
		     upper-bound)
  (kv-env-rehash env)
  (with-kv-store env MDB_RDONLY
    (lambda (read-store)
      (write-bencode (vector "SYNC" supported-versions)
		     (connection-out conn))
      (match (receive-bencode conn)
	(#("ROOT" version hash)
	 ;; TODO: test sync with empty store
	 (unless (blob=? (string->blob hash)
			 (root-hash (kv-store-hash-store read-store)))
	   (lazy-each
	    (match-lambda
		(#(type key remote-hash)
		 (with-kv-store env 0
		   (lambda (write-store)
		     (change-handler write-store conn type key remote-hash)))))
	    (hash-diff (kv-store-hash-store read-store)
		       conn
		       lower-bound
		       upper-bound))))))))

(define (kv-env-sync-accept-handle-message store conn msg)
  (match msg
    (#("SYNC" #(1))
     (write-bencode
      ;; TODO: test sync with empty store
      (vector "ROOT" 1 (blob->string
			(root-hash (kv-store-hash-store store))))
      (connection-out conn)))
    (#("GET" key)
     (write-bencode
      (vector "DATA" key (blob->string
			  (kv-get store (string->blob key))))
      (connection-out conn)))
    (else
     (hash-diff-accept-handle-message
      (kv-store-hash-store store)
      conn
      msg))))

(define (kv-env-sync-accept env conn)
  (kv-env-rehash env)
  (with-kv-store env MDB_RDONLY
   (lambda (read-store)
     (let loop ()
       (and-let* ((data (receive-bencode conn)))
	 (kv-env-sync-accept-handle-message read-store conn data)
	 (loop))))))

)
