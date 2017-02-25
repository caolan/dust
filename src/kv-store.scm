(module dust.kv-store

;;;;; Exports ;;;;;

(kv-store-open
 kv-store-txn
 kv-store-dbi
 kv-store-hash-store
 kv-put
 kv-get
 kv-delete
 )

;;;;; Dependencies ;;;;;

(import chicken scheme)

(use lmdb-lolevel
     sodium
     dust.hash-store)

(define-constant DBI_NAME "kv")

(define-record kv-store txn dbi hash-store)

(define (kv-store-open txn)
  (make-kv-store
   txn
   (mdb-dbi-open txn DBI_NAME MDB_CREATE)
   (hash-store-open txn)))

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

)
