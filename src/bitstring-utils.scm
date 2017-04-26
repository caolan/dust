(module dust.bitstring-utils *

(import chicken scheme)
(use bitstring)

(define (bitstring-split b n)
  (bitmatch b
    (((before n bitstring) (after bitstring))
     (values before after))))

(define (bitstring-drop b n)
  (receive (before after) (bitstring-split b (min n (bitstring-length b)))
    after))

(define (bitstring-drop-right b n)
  (receive (before after)
      (bitstring-split b (max 0 (- (bitstring-length b) n)))
    before))

(define (bitstring-take b n)
  (receive (before after) (bitstring-split b (min n (bitstring-length b)))
    before))

(define (bitstring-take-right b n)
  (receive (before after)
      (bitstring-split b (max 0 (- (bitstring-length b) n)))
    after))


;; Returns:
;; - negative if a < b
;; - zero if a == b
;; - positive if a > b

;; TODO: use memcmp on full bytes of a and b before comparing
;; remaining bits? see optimisation used in lmdb comparator
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

;; return #t if a is a prefix of b, #f otherwise
(define (bitstring-prefix? a b)
  (bitstring=? a (bitstring-take b (bitstring-length a))))
  
)
