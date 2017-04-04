(module dust.bitstring-utils *

(import chicken scheme)
(use bitstring)

(define (bitstring-split b n)
  (bitmatch b
    (((before n bitstring) (after bitstring))
     (values before after))))

(define (bitstring-drop b n)
  (receive (before after) (bitstring-split b n)
    after))

(define (bitstring-take b n)
  (receive (before after) (bitstring-split b n)
    before))

;; Returns:
;; - negative if a < b
;; - zero if a == b
;; - positive if a > b
(define (bitstring-compare a b)
  (let loop ((i 0))
    (cond
     ((and (fx= i (bitstring-length a))
           (fx= i (bitstring-length b))) 0)
     ((fx= i (bitstring-length a)) -1)
     ((fx= i (bitstring-length b)) 1)
     ((eq? (bitstring-bit-set? a i) (bitstring-bit-set? b i))
      (loop (fx+ i 1)))
     ((bitstring-bit-set? a i) 1)
     (else -1))))

)
