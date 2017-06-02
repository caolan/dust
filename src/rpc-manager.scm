(module dust.rpc-manager

;; Exports
(rpc-manager-start
 rpc-manager-stop
 rpc-send
 rpc-cancel
 rpc-timeout-condition?)

(import chicken scheme)

(use srfi-69
     srfi-18
     log5scm
     gochan
     udp6
     sodium
     bencode
     matchable
     data-structures
     dust.alist-match)

(define-record request
  channel
  host
  port)

(define-record rpc-manager
  dispatcher
  listener
  incoming
  outgoing
  cancellations)

(define waiting (make-parameter #f))
(define handlers (make-parameter #f))
(define socket (make-parameter #f))

(define (dispatch-loop incoming outgoing cancellations)
  (gochan-select
   ((incoming -> msg) (apply handle-incoming msg))
   ((outgoing -> msg) (apply handle-outgoing msg))
   ((cancellations -> msg) (apply handle-cancellation msg)))
  (dispatch-loop incoming outgoing cancellations))

(define (handle-incoming data from-host from-port)
  (log-for (debug rpc-manager) "Received message from ~A:~A: ~S"
           from-host
           from-port
           data)
  (alist-match
   data
   (((type . "q") tid method)
    (handle-request method
                    tid
                    (alist-ref 'params data)
                    (and-let* ((sid (alist-ref 'sid data)))
                      (string->blob sid))
                    from-host
                    from-port))
   (((type . "r") tid)
    (handle-response tid data from-host from-port))
   (((type . "e") tid)
    (handle-response tid data from-host from-port))
   (else
    (log-for (error rpc-manager)
             "Invalid message format (from ~A:~A)~n"
             from-host
             from-port))))

(define (handle-outgoing data to-host to-port channel)
  (alist-match
   data
   ((tid)
    (condition-case
        (let ((str (bencode->string data)))
          (log-for (debug rpc-manager) "Sending request: ~S" data)
          (hash-table-set! (waiting)
                           tid
                           (make-request channel to-host to-port))
          (udp-sendto (socket) to-host to-port str))
      (exn (exn bencode)
           (gochan-send channel
                        `((type . "e")
                          (code . 0)
                          (desc . ,(sprintf "Invalid bencode request: ~S"
                                            data)))))))
   (else
    (gochan-send channel
                 `((type . "e")
                   (code . 0)
                   (desc . "Request is missing TID"))))))

(define (handle-cancellation tid)
  (log-for (debug rpc-manager) "Cancelling request: ~A" tid)
  (hash-table-delete! (waiting) tid))

(define (handle-request method tid params sid from-host from-port)
  (let ((handler (hash-table-ref/default (handlers) method #f)))
    (if handler
        (let ((response (handler tid params sid from-host from-port)))
          (condition-case
              (let ((str (bencode->string response)))
                (log-for (debug rpc-manager) "Sending response: ~S" response)
                (udp-sendto (socket)
                            from-host
                            from-port
                            str))
            ((exn bencode)
             (log-for (error rpc-manager)
                      "Invalid bencode returned by '~A' method handler~n"
                      method))))
        (log-for (error rpc-manager)
                 "No handler for method '~A' (from ~A:~A)~n"
                 method
                 from-host
                 from-port))))

(define (handle-response tid data from-host from-port)
  (cond
   ((string? tid)
    (let ((request (hash-table-ref/default (waiting) tid #f)))
      (cond ((not request)
             (log-for (error rpc-manager)
                      "Message for missing TID: ~A (from ~A:~A)"
                      (bin->hex (string->blob tid))
                      from-host
                      from-port))
            ;; make sure response comes from expected host/port
            ((and (string=? from-host (request-host request))
                  (= from-port (request-port request)))
             (hash-table-delete! (waiting) tid)
             (gochan-send (request-channel request) data))
            (else
             (log-for (error rpc-manager)
                      "Unexpected host/port for TID: ~A (from ~A:~A)"
                      (bin->hex (string->blob tid))
                      from-host
                      from-port)))))
   (else
    (log-for (error rpc-manager)
             "Invalid TID: ~S (from ~A:~A)" tid from-host from-port))))

(define (recv-loop incoming)
  (receive (n data from-host from-port) (udp-recvfrom (socket) 1200)
    (let ((msg (condition-case (string->bencode data)
                 ((exn bencode) #f))))
      (if msg
          (gochan-send incoming (list msg from-host from-port))
          (begin
            (log-for (error rpc-manager)
                     "Invalid incoming bencode message (from ~A:~A)~n"
                     from-host
                     from-port)))))
  (recv-loop incoming))

(define (rpc-manager-start sock rpc-handlers)
  (parameterize
      ((socket sock)
       (handlers rpc-handlers)
       (waiting (make-hash-table string=? string-hash)))
    (let* ((incoming (gochan 0))
           (outgoing (gochan 0))
           (cancellations (gochan 0))
           (dispatcher (go (dispatch-loop incoming outgoing cancellations)))
           (listener (go (recv-loop incoming))))
      (make-rpc-manager dispatcher
                        listener
                        incoming
                        outgoing
                        cancellations))))

(define (rpc-manager-stop manager)
  (thread-terminate! (rpc-manager-listener manager))
  (thread-terminate! (rpc-manager-dispatcher manager))
  (gochan-close (rpc-manager-incoming manager))
  (gochan-close (rpc-manager-outgoing manager))
  (gochan-close (rpc-manager-cancellations manager)))

(define (rpc-cancel manager tid)
  (gochan-send (rpc-manager-cancellations manager) (list tid)))

(define (rpc-request tid method params #!optional server-id)
  (append `((tid . ,tid)
            (type . "q")
            (method . ,method))
          (if params `((params . ,params)) '())
          (if server-id `((sid . ,(blob->string server-id))) '())))

(define (rpc-send manager to-host to-port method params #!key
                  (timeout 5000)
                  server-id)
  (let* ((channel (gochan 0))
         (tid (blob->string (random-blob 20)))
         (msg (rpc-request tid method params server-id)))
    (gochan-send
     (rpc-manager-outgoing manager)
     (list msg to-host to-port channel))
    (gochan-select
     ((channel -> msg)
      (alist-match
       msg
       (((type . "r") data)
        (log-for (debug rpc-manager)
                 "~S got RPC response: ~S" (current-thread) data)
        data)
       (((type . "e") code desc)
        (abort (make-composite-condition
                (make-property-condition 'exn 'message desc)
                (make-property-condition 'rpc 'code code))))
       (else
        (abort (make-composite-condition
                (make-property-condition
                 'exn 'message "Invalid response format")
                (make-property-condition 'rpc))))))
     (((gochan-after timeout) -> _)
      (rpc-cancel manager tid)
      (abort (make-rpc-timeout-condition
              (string-append
               method
               (sprintf " call timed out (~Ams)" timeout))))))))

(define (make-rpc-timeout-condition msg)
  (make-composite-condition
   (make-property-condition 'exn 'message msg)
   (make-property-condition 'rpc-timeout)))

(define rpc-timeout-condition?
  (condition-predicate 'rpc-timeout))
  
)
