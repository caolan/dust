(module dust.thread-monitor *

(import chicken scheme)
(use gochan srfi-18)

(define-syntax go-monitored
  (syntax-rules ()
    ((_ channel body ...)
     (thread-start!
      (lambda ()
        (handle-exceptions exn
          (begin
            (gochan-send channel
                         (list 'thread-exit
                               (current-thread)
                               exn))
            (abort exn))
          body
          ...
          (gochan-send channel
                       (list 'thread-exit
                             (current-thread)
                             #f))))))))

)
