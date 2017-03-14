(module dust.server

(start)

(import chicken scheme)

(use lmdb-lolevel
     unix-sockets
     posix
     bencode
     srfi-69
     pathname-expand
     data-structures
     dust.connection
     dust.event-store)

(define (ping) "pong")

(define-syntax make-dispatcher
  (syntax-rules ()
    ((_ name ...)
     (let ((ht (make-hash-table
                test: string=?
                hash: string-hash)))
       (hash-table-set! ht (symbol->string (quote name)) name)
       ...
       (lambda (data)
         ((hash-table-ref ht (alist-ref 'method data))))))))

(define dispatch-rpc
  (make-dispatcher ping))

(define (handle-request conn)
  (write-bencode (dispatch-rpc (receive-bencode conn))
                 (connection-out conn)))

(define (accept-request listener)
  (receive (in out) (unix-accept listener)
    (handle-request (connection in out))))

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
  (create-directory (pathname-expand "~/.dust"))
  (with-unix-socket "~/.dust/dust.sock" accept-loop))

)
