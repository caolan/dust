(module dust.miniupnpc

;; Exports
(upnp-discover
 get-valid-igd
 control-url
 first-service-type
 get-external-address
 add-any-port-mapping
 add-port-mapping
 get-specific-port-mapping-entry
 MINIUPNPC_VERSION
 MINIUPNPC_API_VERSION)

(import chicken scheme foreign)
(use lolevel srfi-13 extras)

(foreign-declare "#include <miniupnpc/miniupnpc.h>")
(foreign-declare "#include <miniupnpc/upnpcommands.h>")

(define MINIUPNPC_VERSION
  (foreign-value "MINIUPNPC_VERSION" c-string))

(define MINIUPNPC_API_VERSION
  (foreign-value "MINIUPNPC_API_VERSION" int))

(cond-expand
  (miniupnpc-local-port-defines
   (set! UPNP_LOCAL_PORT_ANY (foreign-value "UPNP_LOCAL_PORT_ANY" int))
   (set! UPNP_LOCAL_PORT_SAME (foreign-value "UPNP_LOCAL_PORT_SAME" int)))
  (else
   (define UPNP_LOCAL_PORT_ANY 0)
   (define UPNP_LOCAL_PORT_SAME 1)))

(define UPNPCOMMAND_SUCCESS (foreign-value "UPNPCOMMAND_SUCCESS" int))

(define-record upnp-device-list pointer)
(define-record upnp-urls pointer)
(define-record igd-datas pointer)

(define (free-upnp-device-list! devices)
  ((foreign-lambda void "freeUPNPDevlist" (c-pointer (struct UPNPDev)))
   (upnp-device-list-pointer devices)))

(define (free-upnp-urls! urls)
  ((foreign-lambda void "FreeUPNPUrls" (c-pointer (struct UPNPUrls)))
   (upnp-urls-pointer urls)))

(define c_upnpDiscover
  (cond-expand
    (miniupnpc-discover-ttl
     (foreign-lambda (c-pointer (struct UPNPDev)) "upnpDiscover"
       int
       c-string
       c-string
       int
       int
       int
       (c-pointer int)))
    (else
     (foreign-lambda (c-pointer (struct UPNPDev)) "upnpDiscover"
       int
       c-string
       c-string
       int
       int
       (c-pointer int)))))

(define (upnp-discover #!key
                      (timeout 1000)
                      multicast-interface
                      minissdpdsock
                      (local-port UPNP_LOCAL_PORT_ANY)
                      ipv6
                      (ttl 2))
  (let-location ((err int))
    (let ((ptr
           (cond-expand
             (miniupnpc-discover-ttl
              (c_upnpDiscover timeout
                              multicast-interface
                              minissdpdsock
                              local-port
                              (if ipv6 1 0)
                              ttl
                              (location err)))
             (else
              (c_upnpDiscover timeout
                              multicast-interface
                              minissdpdsock
                              local-port
                              (if ipv6 1 0)
                              (location err))))))
      (if ptr
          (let ((devices (make-upnp-device-list ptr)))
            (set-finalizer! devices free-upnp-device-list!)
            devices)
          (abort (make-property-condition
                  'exn
                  'location 'upnp-discover
                  'message (sprintf "error code ~S" error)))))))

(define c_UPNP_GetValidIGD
  (foreign-lambda int "UPNP_GetValidIGD"
    (c-pointer (struct UPNPDev))
    (c-pointer (struct UPNPUrls))
    (c-pointer (struct IGDdatas))
    c-pointer
    int))

(define strlen
  (foreign-lambda int "strlen" c-pointer))

(define sizeof_UPNPUrls (foreign-value "sizeof(struct UPNPUrls)" int))
(define sizeof_IGDdatas (foreign-value "sizeof(struct IGDdatas)" int))

(define (get-valid-igd devices)
  (let ((c-urls (allocate sizeof_UPNPUrls))
        (c-data (allocate sizeof_IGDdatas))
        (lan-addr (make-string 64)))
    (let ((ret (c_UPNP_GetValidIGD
                (upnp-device-list-pointer devices)
                c-urls
                c-data
                (location lan-addr)
                (string-length lan-addr))))
      (if (= ret 0)
          (abort "No IGD found")
          ;; otherwise, possibly IGD found,
          ;; ret can be 1,2 or 3 - check miniupnpc.h
          (let ((urls (make-upnp-urls c-urls))
                (data (make-igd-datas c-data)))
            (set-finalizer! urls free-upnp-urls!)
            (values urls
                    data
                    (string-take lan-addr
                                 (strlen (location lan-addr)))))))))

(define c_UPNP_GetExternalIPAddress
  (foreign-lambda int "UPNP_GetExternalIPAddress"
    c-string
    c-string
    c-pointer))

(define (control-url urls)
  ((foreign-lambda* c-string (((c-pointer (struct UPNPUrls)) urls))
     "C_return(urls->controlURL);")
   (upnp-urls-pointer urls)))

(define (first-service-type data)
  ((foreign-lambda* c-string (((c-pointer (struct IGDdatas)) data))
     "C_return(data->first.servicetype);")
   (igd-datas-pointer data)))

(define (get-external-address control-url service-type)
  (let* ((ip (make-string 64))
         (ret (c_UPNP_GetExternalIPAddress
               control-url
               service-type
               (location ip))))
    (if (= UPNPCOMMAND_SUCCESS ret)
        (string-take ip (strlen (location ip)))
        (abort (make-property-condition
                'exn
                'location 'get-external-address
                'message (string-append
                          "UPNP_GetExternalIPAddress returned "
                          (number->string ret)))))))

(define (upnp-error-message code)
  (case code
    ((401) "Invalid action")
    ((402) "Invalid args")
    ((404) "Invalid var")
    ((501) "Action failed")
    ((606) "Action not authorized")
    ((714) "The specified value does not exist in the array")
    ((715) "The source IP address cannot be wild-carded")
    ((716) "The external port cannot be wild-carded")
    ((724) "Internal and external port values must be the same")
    ((725) "The NAT implementation only supports permanent lease times on port mappings")
    ((726) "Remote host must be a wildcard and cannot be a specific IP address or DNS name")
    ((727) "External port must be a wildcard and cannot be a specific port value")
    ((728) "There are not enough free ports available to complete port mapping")
    ((729) "Attempted port mapping is not allowed due to conflict with other mechanisms")
    ((732) "The internal port cannot be wild-carded")
    (else (sprintf "returned ~S" code))))

(define c_UPNP_AddAnyPortMapping
  (foreign-lambda int "UPNP_AddAnyPortMapping"
    c-string
    c-string
    c-string
    c-string
    c-string
    c-string
    c-string
    c-string
    c-string
    (c-pointer char)))

(define (add-any-port-mapping
         #!key control-url service-type external-port internal-port client
         description protocol remote-host lease-duration)
  (let* ((reserved-port (make-string 6))
         (ret (c_UPNP_AddAnyPortMapping control-url
                                        service-type
                                        (number->string external-port)
                                        (number->string internal-port)
                                        client
                                        description
                                        protocol
                                        remote-host
                                        (number->string lease-duration)
                                        (location reserved-port))))
    (if (= ret UPNPCOMMAND_SUCCESS)
        (string->number
         (string-take reserved-port (strlen (location reserved-port))))
        (abort
         (make-property-condition
          'exn
          'location 'add-any-port-mapping
          'message (upnp-error-message ret))))))

(define c_UPNP_AddPortMapping
  (foreign-lambda int "UPNP_AddPortMapping"
    c-string
    c-string
    c-string
    c-string
    c-string
    c-string
    c-string
    c-string
    c-string))

(define (add-port-mapping
         #!key control-url service-type external-port internal-port client
         description protocol remote-host lease-duration)
  (let ((ret (c_UPNP_AddPortMapping control-url
                                    service-type
                                    (number->string external-port)
                                    (number->string internal-port)
                                    client
                                    description
                                    protocol
                                    remote-host
                                    (number->string lease-duration))))
    (if (= ret UPNPCOMMAND_SUCCESS)
        external-port
        (abort
         (make-property-condition
          'exn
          'location 'add-any-port-mapping
          'message (upnp-error-message ret))))))

(define c_UPNP_GetSpecificPortMappingEntry
  (foreign-lambda int "UPNP_GetSpecificPortMappingEntry"
    c-string
    c-string
    c-string
    c-string
    c-string
    (c-pointer char)
    (c-pointer char)
    (c-pointer char)
    (c-pointer char)
    (c-pointer char)))

(define (get-specific-port-mapping-entry
         #!key control-url service-type external-port protocol remote-host)
  (let* ((client (make-string 16))
         (internal-port (make-string 6))
         (description (make-string 80))
         (enabled (make-string 4))
         (lease-duration (make-string 16))
         (ret (c_UPNP_GetSpecificPortMappingEntry
               control-url
               service-type
               (number->string external-port)
               protocol
               remote-host
               (location client)
               (location internal-port)
               (location description)
               (location enabled)
               (location lease-duration))))
    (if (= ret UPNPCOMMAND_SUCCESS)
        (values (string-take client (strlen (location client)))
                (string-take internal-port (strlen (location internal-port)))
                (string-take description (strlen (location description)))
                (string-take enabled (strlen (location enabled)))
                (string-take lease-duration (strlen (location lease-duration))))
        (abort
         (make-property-condition
          'exn
          'location 'get-specific-port-mapping-entry
          'message (upnp-error-message ret))))))
        
)
