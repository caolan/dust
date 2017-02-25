(module dust.u8vector-utils *

(import chicken scheme foreign lolevel)
(use srfi-4 lolevel)

(define (u8vector->string v)
  (blob->string (u8vector->blob/shared v)))

(define (string->u8vector s)
  (blob->u8vector/shared (string->blob s)))

(define (u8vector=? a b)
  ;; (printf "u8vector=?: ~S ~S~n" a b)
  (blob=? (u8vector->blob/shared a)
	  (u8vector->blob/shared b)))

(define (u8vector-take data n)
  ;; (printf "u8vector-take: ~S ~S~n" data n)
  (subu8vector data 0 n))

(define (u8vector-drop data n)
  ;; (printf "u8vector-drop: ~S ~S~n" data n)
  (if (= n 0)
      data
      (subu8vector data n (u8vector-length data))))

(define (u8vector-append a b)
  ;; (printf "u8vector-append: ~S ~S~n" a b)
  (let ((output (make-u8vector (+ (u8vector-length a)
				  (u8vector-length b)))))
    (move-memory! a output (u8vector-length a) 0 0)
    (move-memory! b output (u8vector-length b) 0 (u8vector-length a))
    output))

(define memcmp
  (foreign-lambda int "memcmp"
    (const c-pointer)
    (const c-pointer)
    size_t))

(define (u8vector>? a b)
  ;; (printf "u8vector>? ~S ~S~n" a b)
  (cond
   ((= 0 (u8vector-length a)) #f)
   ((= 0 (u8vector-length b)) #t)
   (else
    (let ((result (memcmp (location a)
			  (location b)
			  (min (u8vector-length a) (u8vector-length b)))))
      (cond ((negative? result) #f)
	    ((positive? result) #t)
	    ((= 0 result) (> (u8vector-length a) (u8vector-length b))))))))

(define (u8vector<? a b)
  ;; (printf "u8vector<? ~S ~S~n" a b)
  (cond
   ((= 0 (u8vector-length b)) #f)
   ((= 0 (u8vector-length a)) #t)
   (else
    (let ((result (memcmp (location a)
			  (location b)
			  (min (u8vector-length a) (u8vector-length b)))))
      (cond ((negative? result) #t)
	    ((positive? result) #f)
	    ((= 0 result) (< (u8vector-length a) (u8vector-length b))))))))

(define (u8vector<=? a b)
  (not (u8vector>? a b)))

(define (u8vector>=? a b)
  (not (u8vector<? a b)))

(define (u8vector-at-index? v parent index)
  ;; (printf "u8vector-at-index? ~S ~S ~S~n" v parent index)
  (and (<= (u8vector-length v) (- (u8vector-length parent) index))
       (= 0 (memcmp (location v)
		    (make-locative parent index)
		    (u8vector-length v)))))

(define (u8vector-prefix? a b)
  ;; (printf "u8vector-prefix? ~S ~S~n" a b)
  (or (= (u8vector-length a) 0)
      (u8vector-at-index? a b 0)))

(define (u8vector-shared-prefix a b)
  ;; (printf "u8vector-shared-prefix: ~S ~S~n" a b)
  (let loop ((index 0))
    (cond ((>= index (u8vector-length a))
	   (values a #u8{} (u8vector-drop b (u8vector-length a))))
	  ((>= index (u8vector-length b))
	   (values b (u8vector-drop a (u8vector-length b)) #u8{}))
	  ((= (u8vector-ref a index) (u8vector-ref b index))
	   (loop (+ index 1)))
	  (else
	   (values (u8vector-take a index)
		   (u8vector-drop a index)
		   (u8vector-drop b index))))))

(define (u8vector-truncate x maxlen)
  ;; (printf "u8vector-truncate: ~S ~S~n" x maxlen)
  (u8vector-take x (min (u8vector-length x) maxlen)))

)
