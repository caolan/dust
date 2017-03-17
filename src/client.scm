(module dust.client

(connect
 with-client-connection
 command
 ping
 create-stream
 list-streams
 dump
 add-event)

(import chicken scheme foreign)

(use unix-sockets
     bencode
     posix
     extras
     pathname-expand
     dust.connection)

(define (connect path)
  (receive (in out) (unix-connect (pathname-expand path))
    (connection in out)))

(define (with-client-connection path thunk)
  (let ((conn (connect path)))
    (handle-exceptions exn
      (begin
        (close-connection conn)
        (abort exn))
      (begin
        (thunk conn)
        (close-connection conn)))))

(define (command conn method #!optional params)
  (write-bencode
   (cons `(method . ,method)
         (if params
             `((params . ,params))
             '()))
   (connection-out conn))
  (receive-bencode conn))

(define (ping conn)
  (command conn "ping"))

(define (create-stream conn)
  (command conn "create-stream"))

(define (list-streams conn)
  (command conn "list-streams"))

(define (dump conn stream-id)
  (command conn "dump" (vector stream-id)))

(define (add-event conn stream-id message)
  (command conn "add-event" (vector stream-id message)))

)
