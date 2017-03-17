(import chicken scheme)

(use dust.connection
     dust.environment
     dust.client)

(define commands
  (make-parameter (make-hash-table eq? symbol-hash)))

(define (display-usage commands)
  (print "USAGE"))

(define (display-command-usage name description options arguments)
  (printf "USAGE: ~S~n" name))

(define (make-command name description #!key
                      (options '())
                      (handler default-handler)
                      (arguments 0))
  (hash-table-set!
   (commands)
   name
   (lambda (args)
     (if (not (= (length args) arguments))
         (display-command-usage name description options arguments)
         (with-client-connection
          (socket-path)
          (lambda (conn)
            (apply handler (cons conn args))))))))

(define (run-command arguments)
  (if (= (length arguments) 0)
      (display-usage commands)
      (let ((method (string->symbol (car arguments)))
            (args (cdr arguments)))
        ((hash-table-ref (commands) method) args))))

(define-syntax define-command
  (syntax-rules ()
    ((_ name args ...)
     (make-command (quote name) args ...))))


(make-command 'ping
  "ping the dustd server"
  ;; options: ((q quiet) "don't display response")
  handler: (lambda (conn #!key quiet)
             (let ((response (ping conn)))
               (unless quiet (pp response)))))

(define-command dump
  "dump the data from a stream"
  arguments: 1
  handler: (lambda (conn stream-id)
             (pp (dump conn stream-id))))

(define-command add-event
  "add a message to a stream"
  arguments: 2
  handler: (lambda (conn stream-id message)
             (pp (add-event conn stream-id message))))

(define-command create-stream
  "create a new stream and keypair"
  handler: (lambda (conn)
             (pp (create-stream conn))))

(define-command list-streams
  "list available streams"
  handler: (lambda (conn)
             (pp (list-streams conn))))


(run-command (command-line-arguments))
