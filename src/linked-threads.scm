(module dust.linked-threads

;; Exports
(thread-specific
 thread-specific-set!
 thread?
 make-thread
 thread-start!
 thread-send
 thread-receive!
 thread-terminate!
 thread-link
 thread-monitor
 current-thread
 exited-thread
 exited-thread-exception
 thread-start-linked!
 thread-start-monitored!)

(import chicken scheme)
(use data-structures srfi-1 mailbox (prefix srfi-18 srfi-18:))

(define-record thread-state
  specific
  mbox
  links)

(define (add-state thread)
  (srfi-18:thread-specific-set!
   thread
   (make-thread-state '() (make-mailbox) (make-mailbox)))
  thread)

(define (thread-specific-set! thread x)
  (thread-state-specific-set! (srfi-18:thread-specific thread) x))

(define thread-specific
  (compose thread-state-specific srfi-18:thread-specific))

(define thread-mbox
  (compose thread-state-mbox srfi-18:thread-specific))

(define thread-links
  (compose thread-state-links srfi-18:thread-specific))

(define (thread? thread)
  (and (srfi-18:thread? thread)
       (thread-state? (srfi-18:thread-specific thread))))

(define (thread-exit-condition exn)
  (make-property-condition 'thread-exit
                           'thread (current-thread)
                           'exn exn))

(define exited-thread
  (condition-property-accessor 'thread-exit 'thread))

(define exited-thread-exception
  (condition-property-accessor 'thread-exit 'exn))

(define thread-shutdown-condition
  (make-property-condition 'thread-shutdown))

(define (alert-linked-threads exn)
  (let ((links (thread-links (current-thread))))
    (let loop ()
      (unless (mailbox-empty? links)
        (let ((msg (mailbox-receive! links)))
          (case (car msg)
            ((link)
             (srfi-18:thread-signal! (cdr msg)
                                     (thread-exit-condition exn)))
            ((monitor)
             (thread-send (cdr msg)
                          (list 'thread-exit (current-thread) exn)))))
        (loop)))))

(define (with-handler thunk)
  (handle-exceptions exn
    (begin
      (alert-linked-threads exn)
      (abort exn))
    (thunk)))

(define (make-thread thunk #!optional name)
  (add-state (srfi-18:make-thread (lambda () (with-handler thunk)))))

(define (->thread x)
  (if (procedure? x) (make-thread x) x))

(define (start thread)
  ;; linked threads may already have been alerted to another thread
  ;; exiting, causing them to be in the ready state before explicity
  ;; being started
  (unless (and (thread? thread)
               (eq? 'ready (srfi-18:thread-state thread)))
    (srfi-18:thread-start! thread)))

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
  (mailbox-send! (thread-mbox thread) msg))

(define (thread-receive! . args)
  (apply mailbox-receive! (thread-mbox (current-thread)) args))

;; NOTE: using thread-terminate! from srfi-18 will immediate kill a
;; thread *without* alerting linked threads
(define (thread-terminate! thread)
  (assert (thread? thread))
  (srfi-18:thread-signal! thread thread-shutdown-condition))

;; Creates a bi-directional link between current-thread and other-thread
(define (thread-link other-thread)
  (mailbox-send! (thread-links (current-thread)) (cons 'link other-thread))
  (mailbox-send! (thread-links other-thread) (cons 'link (current-thread))))

;; Creates a uni-directional monitor
(define (thread-monitor other-thread)
  (mailbox-send! (thread-links other-thread) (cons 'monitor (current-thread))))

(define (current-thread)
  (let ((thread (srfi-18:current-thread)))
    (unless (thread? thread)
      (let ((handler (current-exception-handler)))
        (current-exception-handler
         (lambda (exn)
           (with-handler (lambda () (handler exn))))))
      (add-state thread))
    thread))

)
