(module dust.server

(start)

(import chicken scheme)

(use lmdb-lolevel
     unix-sockets
     posix
     files
     extras
     sodium
     bencode
     srfi-69
     lazy-seq
     pathname-expand
     data-structures
     dust.connection
     dust.environment
     dust.event-store)

(sodium-init)

(define (ping)
  "pong")

(define (create-stream)
  (receive (public-key secret-key) (sign-keypair)
    (let ((pubkey-hex (bin->hex public-key)))
      (create-directory (private-keys-path))
      (with-output-to-file (make-pathname (private-keys-path) pubkey-hex)
        (lambda ()
          (write-string (bin->hex secret-key))))
      (create-directory (make-pathname (streams-path) pubkey-hex) #t)
      pubkey-hex)))

(define (list-streams)
  (if (file-exists? (streams-path))
      (list->vector (directory (streams-path)))
      (vector)))

(define (dump stream-id)
  (let ((env (mdb-env-create))
        (path (make-pathname (streams-path) stream-id)))
    (mdb-env-set-mapsize env 10000000)
    (mdb-env-set-maxdbs env 4)
    (mdb-env-open env path MDB_NOSYNC
                  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (with-event-store
      env 0
      (lambda (store)
        (lazy-seq->list
         (lazy-map (lambda (p)
                     (cons (string->symbol (format-event-time-blob (car p)))
                           (blob->string (cdr p))))
                   (event-pairs store)))))))

(define (add-event stream-id message)
  (let ((env (mdb-env-create))
        (path (make-pathname (streams-path) stream-id)))
    (mdb-env-set-mapsize env 10000000)
    (mdb-env-set-maxdbs env 4)
    (mdb-env-open env path MDB_NOSYNC
                  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (with-event-store
     env 0
     (lambda (store)
       (format-event-time-blob
        (event-put store (string->blob message)))))))

(define-syntax make-dispatcher
  (syntax-rules ()
    ((_ name ...)
     (let ((ht (make-hash-table
                test: string=?
                hash: string-hash)))
       (hash-table-set! ht (symbol->string (quote name)) name)
       ...
       (lambda (data)
         (let ((f (hash-table-ref/default ht (alist-ref 'method data) #f))
               (args (alist-ref 'params data)))
           (if f
               `((data . ,(apply f (if args (vector->list args) '()))))
               `((error . "Unknown method")))))))))

(define dispatch-rpc
  (make-dispatcher
   ping
   create-stream
   list-streams
   dump
   add-event))

(define (accept-request listener)
  (receive (in out) (unix-accept listener)
    (let loop ((conn (connection in out)))
      (let ((data (receive-bencode conn)))
        (when data
          (write-bencode (dispatch-rpc data) (connection-out conn))
          (loop conn))))))

(define (accept-loop listener)
  (accept-request listener)
  (accept-loop listener))

(define (with-unix-socket path thunk)
  (let ((listener (unix-listen (pathname-expand path))))
    (handle-exceptions exn
      (begin (unix-close listener)
             (abort exn))
      (thunk listener))
    (unix-close listener)))

(define (start)
  (create-directory (dust-path))
  (with-unix-socket (socket-path) accept-loop))

)
