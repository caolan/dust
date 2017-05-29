(module dust.linked-threads

;; Exports
(thread-specific
 thread-specific-set!
 thread?
 make-thread
 thread-start!
 thread-send
 thread-receive!
 thread-messages
 thread-shutdown!
 thread-link
 thread-unlink
 thread-monitor
 thread-demonitor
 current-thread
 thread-exit-condition?
 exited-thread
 exited-thread-exception
 thread-start-linked!
 thread-start-monitored!
 thread-signal!
 thread-yield!
 thread-join!)

(import chicken scheme)
(use log5scm data-structures srfi-1 gochan (prefix srfi-18 srfi-18:))

(define-record thread-state
  specific
  mbox
  links
  stopping)

(define (add-state thread)
  (srfi-18:thread-specific-set!
   thread
   (make-thread-state '() (gochan 1000) (gochan 1000) #f))
  thread)

(define (thread-specific-set! thread x)
  (thread-state-specific-set! (srfi-18:thread-specific thread) x))

(define thread-specific
  (compose thread-state-specific srfi-18:thread-specific))

(define thread-mbox
  (compose thread-state-mbox srfi-18:thread-specific))

(define thread-links
  (compose thread-state-links srfi-18:thread-specific))

(define thread-stopping?
  (compose thread-state-stopping srfi-18:thread-specific))

(define (thread-stopping-set! thread flag)
  (let ((state (srfi-18:thread-specific thread)))
    (thread-state-stopping-set! state flag)))

(define (thread? thread)
  (and (srfi-18:thread? thread)
       (thread-state? (srfi-18:thread-specific thread))))

(define (make-thread-exit-condition exn)
  (make-property-condition 'thread-exit
                           'thread (current-thread)
                           'exn exn))

(define thread-exit-condition?
  (condition-predicate 'thread-exit))

(define exited-thread
  (condition-property-accessor 'thread-exit 'thread))

(define exited-thread-exception
  (condition-property-accessor 'thread-exit 'exn))

(define (make-thread-shutdown-condition)
  (make-property-condition 'thread-shutdown))

(define thread-shutdown-condition?
  (condition-predicate 'thread-shutdown))

(define (alert-linked-threads exn)
  (let ((links (thread-links (current-thread))))
    (let loop ((linked '())
               (monitoring '()))
      (gochan-select
       ((links -> msg)
        (case (car msg)
          ((link)
           (loop (if (member (cdr msg) linked)
                     linked
                     (cons (cdr msg) linked))
                 monitoring))
          ((unlink)
           (loop (filter (lambda (t)
                           (not (equal? t (cdr msg))))
                         linked)
                 monitoring))
          
          ((monitor)
           (loop linked
                 (if (member (cdr msg) monitoring)
                     monitoring
                     (cons (cdr msg) monitoring))))
          ((demonitor)
           (loop linked
                 (filter (lambda (t)
                           (not (equal? t (cdr msg))))
                         monitoring)))))
       (else
        ;; signal/shutdown linked threads
        (printf "~S signaling / shutting down linked threads~n"
                (current-thread))
        (for-each
         (lambda (t)
           (if (thread-stopping? t)
               (when (thread-shutdown-condition? exn)
                 (printf "~S waiting for shutting down thread ~S~n"
                         (current-thread)
                         t)
                 (thread-join! t))
               (if (thread-shutdown-condition? exn)
                   (thread-shutdown! t)
                   (thread-signal! t (make-thread-exit-condition exn)))))
         linked)
        ;; message monitoring threads
        (printf "~S messaging monitoring threads~n"
                (current-thread))
        (for-each
         (lambda (t)
           (unless (thread-stopping? t)
             (thread-send t (list 'thread-exit (current-thread) exn))))
         monitoring))))))

(define (with-handler thunk)
  (handle-exceptions exn
    (begin
      (thread-stopping-set! (current-thread) #t)
      (printf "~S got signal ~S~n" (current-thread) exn)
      (alert-linked-threads exn)
      (if (thread-shutdown-condition? exn)
          (begin
            (printf "~S terminating~n" (current-thread))
            (srfi-18:thread-terminate! (current-thread)))
          (abort exn)))
    (thunk)
    (alert-linked-threads #f)))

(define (make-thread thunk #!optional name)
  (add-state
   (srfi-18:make-thread
    (lambda () (with-handler thunk))
    name)))

(define (->thread x)
  (if (procedure? x) (make-thread x) x))

(define (start thread)
  ;; linked threads may already have been alerted to another thread
  ;; exiting, causing them to be in the ready state before explicity
  ;; being started
  (unless (and (thread? thread)
               (eq? 'ready (srfi-18:thread-state thread)))
    (srfi-18:thread-start! thread))
  thread)

(define (thread-start! thread)
  (start (->thread thread)))

(define (thread-start-linked! thread)
  (let ((t (->thread thread)))
    (thread-link t)
    (start t)))

(define (thread-start-monitored! thread)
  (let ((t (->thread thread)))
    (thread-monitor t)
    (start t)))

(define (thread-send thread msg)
  (log-for (debug linked-threads) "~S sending to ~S: ~S"
           (current-thread)
           thread
           msg)
  (gochan-send (thread-mbox thread) msg))

(define (thread-receive! #!optional timeout)
  (let* ((mbox (thread-mbox (current-thread)))
         (msg (if timeout
                  (gochan-select
                   ((mbox -> msg) msg)
                   (((gochan-after timeout) -> _)
                    (abort (make-composite-condition
                            (make-property-condition 'exn
                                                     'message "Timed out")
                            (make-property-condition 'messages)
                            (make-property-condition 'timeout)))))
                  (gochan-recv mbox))))
    (log-for (debug linked-threads) "~S received: ~S"
             (current-thread)
             msg)
    msg))

(define (thread-messages)
  (thread-mbox (current-thread)))

;; NOTE: using thread-terminate! from srfi-18 will immediate kill a
;; thread *without* alerting linked threads
(define (thread-shutdown! thread)
  (thread-unlink thread)
  (unless (thread-stopping? thread)
    (srfi-18:thread-signal! thread (make-thread-shutdown-condition)))
  (printf "~S waiting for ~S to shutdown~n"
          (current-thread)
          thread)
  (thread-join! thread)
  (printf "~S finished waiting for ~S to shutdown~n"
          (current-thread)
          thread))

;; Creates a bi-directional link between current-thread and other-thread
(define (thread-link other-thread)
  (gochan-send (thread-links (current-thread)) (cons 'link other-thread))
  (gochan-send (thread-links other-thread) (cons 'link (current-thread))))

(define (thread-unlink other-thread)
  (gochan-send (thread-links (current-thread)) (cons 'unlink other-thread))
  (gochan-send (thread-links other-thread) (cons 'unlink (current-thread))))

;; Creates a uni-directional monitor
(define (thread-monitor other-thread)
  (gochan-send (thread-links other-thread) (cons 'monitor (current-thread))))

(define (thread-demonitor other-thread)
  (gochan-send (thread-links other-thread) (cons 'demonitor (current-thread))))

(define (current-thread)
  (let ((thread (srfi-18:current-thread)))
    (unless (thread? thread)
      (let ((handler (current-exception-handler)))
        (current-exception-handler
         (lambda (exn)
           (with-handler (lambda () (handler exn))))))
      (add-state thread))
    thread))

;; (define current-thread srfi-18:current-thread)

(define (thread-signal! thread exn)
  (unless (thread-stopping? thread)
    (srfi-18:thread-signal! thread exn)))

(define thread-yield! srfi-18:thread-yield!)
(define thread-join! srfi-18:thread-join!)

)
