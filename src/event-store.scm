(module dust.event-store

;;;;; Exports ;;;;;

(event-store-open
 with-event-store
 meta-dbi-open
 event-store-id
 event-store-txn
 event-store-meta-dbi
 event-store-kv-store
 event-store-clock
 event-put
 events
 event-pairs
 event-store-env-sync
 event-store-env-sync-accept
 event-time->blob
 blob->event-time)

;;;;; Dependencies ;;;;;

(import chicken scheme)

(use lmdb-lolevel
     endian-blob
     srfi-4
     extras
     sodium
     bencode
     lazy-seq
     matchable
     miscmacros
     dust.u8vector-utils
     dust.connection
     dust.kv-store)


(define-constant META_DBI_NAME "meta")

(define-record event-store id txn meta-dbi kv-store)

(define (store-id txn dbi)
  (condition-case
      (mdb-get txn dbi (string->blob "id"))
    ((exn lmdb MDB_NOTFOUND)
     (let ((id (gen-store-id)))
       (mdb-put txn dbi (string->blob "id") id 0)
       id))))

(define (gen-store-id)
  (random-blob 20))

(define (event-store-open txn)
  (let ((meta-dbi (meta-dbi-open txn)))
    (make-event-store
     (store-id txn meta-dbi)
     txn
     meta-dbi
     (kv-store-open txn))))

(define (counter->blob t)
  (u8vector->blob/shared (counter->u8vector t)))
  
(define (counter->u8vector t)
  (endian-blob->u8vector (uint4->endian-blob t MSB)))

(define (event-time->blob time id)
  (u8vector->blob/shared
   (u8vector-append
    (counter->u8vector time)
    (blob->u8vector/shared id))))

(define (blob->counter b)
  (u8vector->counter (blob->u8vector/shared b)))
  
(define (u8vector->counter d)
  (endian-blob->uint4
   (u8vector->endian-blob (u8vector-take d 4) MSB)))

(define (blob->event-time b)
  (let ((data (blob->u8vector/shared b)))
    (values
     (u8vector->counter data)
     (u8vector-drop data 4))))

(define (set-clock store t)
  (mdb-put (event-store-txn store)
	   (event-store-meta-dbi store)
	   (string->blob "clock")
	   (counter->blob t)
	   0))

(define (event-store-clock store)
  (condition-case
      (blob->counter
       (mdb-get (event-store-txn store)
		(event-store-meta-dbi store)
		(string->blob "clock")))
    ((exn lmdb MDB_NOTFOUND) 1)))

(define (tick store)
  (set-clock store (+ 1 (event-store-clock store))))

(define (meta-dbi-open txn)
  (mdb-dbi-open txn META_DBI_NAME MDB_CREATE))

(define (with-event-store env flags thunk)
  (let* ((txn (mdb-txn-begin env #f flags))
	 (store (event-store-open txn)))
    (handle-exceptions exn (begin (mdb-txn-abort txn)
				  (abort exn))
		       (begin0
			   (thunk store)
			 (mdb-txn-commit txn)))))

(define (event-put store data)
  (kv-put (event-store-kv-store store)
	  (event-time->blob
	   (event-store-clock store)
	   (event-store-id store))
	  data)
  (tick store))

(define (event-get store timestamp)
  (kv-get (event-store-kv-store store) timestamp))

(define (events store #!key start end)
  (kv-values (event-store-kv-store store)
	     start: start
	     end: end))

(define (event-pairs store #!key start end)
  (kv-pairs (event-store-kv-store store)
	    start: start
	    end: end))

(define (last-timestamp store)
  (let* ((kv-store (event-store-kv-store store))
	 (cursor (mdb-cursor-open (kv-store-txn kv-store)
				  (kv-store-dbi kv-store))))
    (condition-case
	(begin
	  (mdb-cursor-get cursor #f #f MDB_LAST)
	  (mdb-cursor-key cursor))
      ((exn lmdb MDB_NOTFOUND) #f))))

(define (request-event conn timestamp)
  (write-bencode (vector "GET" (blob->string timestamp))
		 (connection-out conn))
  (match (receive-bencode conn)
    (#("DATA" _ data)
     (string->blob data))))

(define (request-since conn timestamp)
  (write-bencode (vector "SINCE" (blob->string timestamp))
		 (connection-out conn))
  (list->lazy-seq (vector->list (receive-bencode conn))))

(define (receive-event store timestamp event)
  (receive (counter id) (blob->event-time timestamp)
    (when (>= counter (event-store-clock store))
      (set-clock store (+ counter 1)))
    (kv-put (event-store-kv-store store)
	    timestamp
	    event
	    flags: MDB_NOOVERWRITE)))

(define (send-event conn timestamp event)
  (write-bencode (vector "PAIR"
			 (blob->string timestamp)
			 (blob->string event))
		 (connection-out conn)))

(define (event-store-change-handler env conn type timestamp remote-hash)
  (with-event-store
   env 0
   (lambda (store)
     (case type
       ((missing)
	(let ((event (event-get store timestamp)))
	  (send-event conn timestamp event)))
       ((new)
	(let ((event (request-event conn timestamp)))
	  (receive-event store timestamp event)))
       ((different)
	;; this shouldn't happen, events are immutable!
	(abort
	 (make-property-condition 'exn
				  'message "Modified event detected")))))))

(define (event-store-env-sync env conn)
  (let ((local-last-timestamp
	 (with-event-store env 0 last-timestamp)))
    (when local-last-timestamp
      (lazy-each
       (lambda (p)
	 (let ((timestamp (string->blob (vector-ref p 0)))
	       (event (string->blob (vector-ref p 1))))
	   (with-event-store
	    env 0
	    (lambda (store)
	      (receive-event store timestamp event)))))
       (request-since conn local-last-timestamp)))
    (kv-env-sync env
		 conn
		 change-handler: event-store-change-handler
		 upper-bound:
		 (and local-last-timestamp
		      (blob->u8vector/shared local-last-timestamp)))))

(define (event-store-env-sync-accept-handle-message store conn msg)
  (match msg
    (#("SINCE" timestamp)
     (let ((t (string->blob timestamp)))
       (write-string "l" #f (connection-out conn))
       (lazy-each
	(lambda (p)
	  (write-bencode
	   (vector (blob->string (car p)) (blob->string (cdr p)))
	   (connection-out conn)))
	(lazy-filter
	 (lambda (p) (not (blob=? t (car p))))
	 (event-pairs store start: t)))
       (write-string "e" #f (connection-out conn))))
    (#("PAIR" timestamp event)
     (receive-event store
		    (string->blob timestamp)
		    (string->blob event)))
    (else
     (kv-env-sync-accept-handle-message
      (event-store-kv-store store)
      conn
      msg))))

(define (event-store-env-sync-accept env conn)
  (kv-env-rehash env)
  (with-event-store
   env 0
   (lambda (store)
     (let loop ()
       (and-let* ((data (receive-bencode conn)))
	 (event-store-env-sync-accept-handle-message store conn data)
	 (loop))))))

)
