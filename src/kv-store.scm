(module dust.kv-store

;;;;; Exports ;;;;;

(kv-store-open
 kv-store-txn
 kv-store-dbi
 kv-store-hash-store
 kv-put
 kv-get
 kv-delete
 kv-diff
 kv-diff-accept
 )

;;;;; Dependencies ;;;;;

(import chicken scheme)

(use lmdb-lolevel
     sodium
     bencode
     matchable
     srfi-4
     srfi-18
     srfi-117
     lazy-seq
     extras
     ports
     data-structures
     dust.hash-store
     dust.u8vector-utils)

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



(define (lazy-filter-map f seq)
  (lazy-filter identity (lazy-map f seq)))

;; (define (lazy-join seqs)
;;   (if (lazy-null? seqs)
;;       lazy-null
;;       (lazy-append (lazy-head seqs) (lazy-tail seqs))))



(define-record kv-connection in out pending)

;; protocol versions supported by this implementation
(define supported-versions #(1))

(define (kv-diff store in out)
  ;; (printf "~S: kv-diff: ~S ~S ~S~n" (current-thread) store in out)
  (let ((conn (make-kv-connection in out (list-queue))))
    ;; start by add request for children of root node to queue
    (list-queue-add-front!
     (kv-connection-pending conn)
     (delay (compare-children store conn #u8{})))
    ;; take values from queue while available and join all output into
    ;; a single lazy sequence
    (let loop ((q (kv-connection-pending conn)))
      ;; (printf "loop: ~S~n" q)
      (lazy-seq
       ;; (printf "loop lazy-seq~n")
       (if (not (list-queue-empty? q))
	   (begin
	     ;; (print "not empty")
	     (let ((results (force (list-queue-remove-front! q))))
	       (lazy-append results (loop q))))
	   (begin
	     ;; (print "empty")
	     '()))))))

(define (compare-children store conn prefix)
  ;; (printf "compare-children: ~S ~S ~S ~S~n" (current-thread) store conn prefix)
  (diff-children store
		 conn
		 (child-hashes (kv-store-hash-store store) prefix)
		 (request-child-hashes conn prefix)))

(define (mark-leaf-nodes-new conn prefix)
  ;; (printf "~S: mark-leaf-nodes-new: ~S ~S~n" (current-thread) conn prefix)
  (lazy-map
   (match-lambda (#(key-r hash-r leaf-r)
		  (vector 'new (u8vector->blob key-r) hash-r)))
   (request-leaf-hashes conn prefix)))

(define (mark-leaf-nodes-missing store prefix)
  ;; (printf "~S: mark-leaf-nodes-missing: ~S ~S~n" (current-thread) store prefix)
  (lazy-map
   (match-lambda (#(key-l hash-l leaf-l)
		  (vector 'missing (u8vector->blob key-l) #f)))
   (leaf-hashes (kv-store-hash-store store) prefix)))

(define (diff-children store conn local remote)
  ;; (printf "~S: diff-children: ~S ~S ~S ~S~n" (current-thread) store conn local remote)
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
		 (kv-connection-pending conn)
		 (delay (mark-leaf-nodes-new conn key-r)))
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
		 (kv-connection-pending conn)
		 (delay (mark-leaf-nodes-missing store key-l)))
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
			     (lazy-tail remote))
	      (lazy-seq
	       (cons (vector 'different (u8vector->blob key-l) hash-r)
		     (diff-children store
				    conn
				    (lazy-tail local)
				    (lazy-tail remote))))))
	 ((and (not leaf-l) (not leaf-r))
	  (if (blob=? hash-l hash-r)
	      (diff-children store
			     conn
			     (lazy-tail local)
			     (lazy-tail remote))
	      (begin
		(list-queue-add-back!
		 (kv-connection-pending conn)
		 (delay (compare-children store conn key-l)))
		(diff-children store
			       conn
			       (lazy-tail local)
			       (lazy-tail remote)))))
	 ((and leaf-l (not leaf-r))
	  ;; treat it like R is a prefix of L:
	  ;; compare leaf hashes of R to the single hash of L
	  (list-queue-add-back!
	   (kv-connection-pending conn)
	   (delay (diff-leaf-hashes
		   ;; TODO: (lazy-take local 1) is the same as below?
		   (list->lazy-seq
		    (list (vector key-l hash-l leaf-l)))
		   (request-leaf-hashes conn key-r))))
	  (diff-children store
			 conn
			 (lazy-tail local)
			 (lazy-tail remote)))
	 (else ;; (and (not leaf-l) leaf-r)
	  ;; treat it like L is prefix of R:
	  ;; compare leaf hashes of L to the single hash of R
	  (list-queue-add-back!
	   (kv-connection-pending conn)
	   (delay (diff-leaf-hashes
		   (leaf-hashes (kv-store-hash-store store) key-l)
		   ;; TODO: (lazy-take remote 1) is the same as below?
		   (list->lazy-seq
		    (list (vector key-r hash-r leaf-r))))))
	  (diff-children store
			 conn
			 (lazy-tail local)
			 (lazy-tail remote)))))
       ((u8vector<? key-l key-r)
	(if (u8vector-prefix? key-l key-r)
	    (if leaf-l
		(lazy-seq
		 (cons (vector 'missing (u8vector->blob key-l) #f)
		       (diff-children store conn (lazy-tail local) remote)))
		(if leaf-r
		    (begin
		      ;; compare leaf hashes of L to the single hash of R
		      (list-queue-add-back!
		       (kv-connection-pending conn)
		       (delay (diff-leaf-hashes
			       (leaf-hashes (kv-store-hash-store store) key-l)
			       ;; TODO: (lazy-take remote 1) is the same as below?
			       (list->lazy-seq
				(list (vector key-r hash-r leaf-r))))))
		      (diff-children store
				     conn
				     (lazy-tail local)
				     (lazy-tail remote)))
		    (begin
		      ;; compare children of L to a sequence containing
		      ;; the single child R
		      (list-queue-add-back!
		       (kv-connection-pending conn)
		       (delay (diff-children
			       store
			       conn
			       (child-hashes (kv-store-hash-store store) key-l)
			       ;; TODO: (lazy-take remote 1) is the same as below?
			       (list->lazy-seq
				(list (vector key-r hash-r leaf-r))))))
		      (diff-children store
				     conn
				     (lazy-tail local)
				     (lazy-tail remote)))))
	    (if leaf-l
		(lazy-seq
		 (cons (vector 'missing (u8vector->blob key-l) #f)
		       (diff-children store conn (lazy-tail local) remote)))
		(begin
		  (list-queue-add-back!
		   (kv-connection-pending conn)
		   (delay (mark-leaf-nodes-missing store key-l)))
		  (diff-children store conn (lazy-tail local) remote)))))
       ((u8vector>? key-l key-r)
	(if (u8vector-prefix? key-r key-l)
	    (if leaf-r
		(lazy-seq
		 (cons (vector 'new (u8vector->blob key-r) hash-r)
		       (diff-children store conn local (lazy-tail remote))))
		(if leaf-l
		    (begin
		      ;; compare leaf hashes of R to the single hash of L
		      (list-queue-add-back!
		       (kv-connection-pending conn)
		       (delay (diff-leaf-hashes
			       ;; TODO: (lazy-take local 1) is the same as below?
			       (list->lazy-seq
				(list (vector key-l hash-l leaf-l)))
			       (request-leaf-hashes conn key-r))))
		      (diff-children store
				     conn
				     (lazy-tail local)
				     (lazy-tail remote)))
		    (begin
		      ;; compare children of R to a sequence containing
		      ;; the single child L
		      (list-queue-add-back!
		       (kv-connection-pending conn)
		       (delay (diff-children
			       store
			       conn
			       ;; TODO: (lazy-take local 1) is the same as below?
			       (list->lazy-seq
				(list (vector key-l hash-l leaf-l)))
			       (request-child-hashes conn key-r))))
		      (diff-children store
				     conn
				     (lazy-tail local)
				     (lazy-tail remote)))))
	    (if leaf-r
		(lazy-seq
		 (cons (vector 'new (u8vector->blob key-r) hash-r)
		       (diff-children store conn local (lazy-tail remote))))
		(begin
		  (list-queue-add-back!
		   (kv-connection-pending conn)
		   (delay (mark-leaf-nodes-new conn key-r)))
		  (diff-children store conn local (lazy-tail remote)))))))))))

(define (diff-leaf-hashes local remote)
  ;; (printf "~S: diff-leaf-hashes: ~S ~S~n" (current-thread) local remote)
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

(define (receive-bencode conn)
  (let ((data (read-bencode (kv-connection-in conn))))
    ;; (printf "~S: received ~S~n" (current-thread) data)
    data))

(define (request-leaf-hashes conn prefix)
  ;; (printf "~S: request-leaf-hashes: ~S ~S~n" (current-thread) conn prefix)
  (write-bencode (vector "LEAF-HASHES" (u8vector->string prefix))
		 (kv-connection-out conn))
  (match (receive-bencode conn)
    (#("LEAF-HASHES" __prefix__ children)
     (read-children prefix children))))

(define (request-child-hashes conn prefix)
  ;; (printf "~S: request-child-hashes: ~S ~S~n" (current-thread) conn prefix)
  (write-bencode (vector "PREFIX" (u8vector->string prefix))
		 (kv-connection-out conn))
  (match (receive-bencode conn)
    (#("HASHES" __prefix__ children)
     (read-children prefix children))))

(define (read-children prefix children)
  ;; (printf "read-children: ~S ~S ~S~n" (current-thread) prefix children)
  (lazy-map
   (match-lambda
       (#(subkey hash leaf)
	(vector (u8vector-append prefix (string->u8vector subkey))
		(string->blob hash)
		(not (= 0 leaf)))))
   (list->lazy-seq (vector->list children))))



(define (kv-diff-accept store in out)
  ;; (printf "~S: kv-diff-accept: ~S ~S ~S~n" (current-thread) store in out)
  (let loop ()
    (and-let* ((data (read-bencode in)))
      ;; (printf "~S: received ~S~n" (current-thread) data)
      (match data
	(#("PREFIX" prefix)
	 (write-child-hashes store (string->u8vector prefix) out)
	 (loop))
	(#("LEAF-HASHES" prefix)
	 (write-leaf-hashes store (string->u8vector prefix) out)
	 (loop))
	))))

(define (write-child-hashes store prefix port)
  ;; (printf "~S: write-child-hashes: ~S ~S ~S~n" (current-thread) store prefix port)
  (with-output-to-port port
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
       (child-hashes (kv-store-hash-store store) prefix))
      (write-char #\e)
      (write-char #\e))))

(define (write-leaf-hashes store prefix port)
  ;; (printf "~S: write-leaf-hashes: ~S ~S ~S~n" (current-thread) store prefix port)
  (with-output-to-port port
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
       (leaf-hashes (kv-store-hash-store store) prefix))
      (write-char #\e)
      (write-char #\e))))

)
