(module dust.lazy-seq-utils *

(import chicken scheme)
(use lazy-seq)

(define (lazy-find pred seq)
  (and (not (lazy-null? seq))
       (if (pred (lazy-head seq))
           (lazy-head seq)
           (lazy-find pred (lazy-tail seq)))))

)
