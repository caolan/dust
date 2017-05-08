(import chicken scheme foreign)

(foreign-declare "#include <miniupnpc/miniupnpc.h>")
(define API_VERSION (foreign-value "MINIUPNPC_API_VERSION" int))

(define feats '())

(when (>= API_VERSION 12)
  (set! feats (cons 'miniupnpc-local-port-defines feats)))

(when (>= API_VERSION 14)
  (set! feats (cons 'miniupnpc-discover-ttl feats)))

(with-output-to-file "miniupnpc-features"
  (lambda () (write feats)))
