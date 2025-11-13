// The following copyright notice relates to the parts of the documentation
// for librdkafka used in this file
/*
librdkafka - Apache Kafka C driver library

Copyright (c) 2012-2022, Magnus Edenhill
              2023, Confluent Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
*/

import { tuple } from '$lib/functions/common/tuple'
import invariant from 'tiny-invariant'
import { match } from 'ts-pattern'
import * as va from 'valibot'

const deduceType = (row: string[]) =>
  row[5].includes('Type: integer')
    ? ('number' as const)
    : row[5].includes('Type: boolean')
      ? ('boolean' as const)
      : row[5].includes('Type: enum value')
        ? ('enum' as const)
        : row[5].includes('Type: CSV list')
          ? ('list' as const)
          : row[5].includes('Type: array')
            ? ('array' as const)
            : ('string' as const)

/**
 * Columns: Property, C/P, Range, Default, Importance, Description
 *
 * C/P legend: C = Consumer, P = Producer, * = both
 *
 * @license librdkafka: This configuration documentation is based on https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md.
 * See LICENSE.librdkafka for a copyright notice
 */
export const librdkafkaOptions = [
  [
    'client.id                               ',
    '*',
    '               ',
    '      rdkafka',
    'low       ',
    `Client identifier.  \n*Type: string*`
  ],
  [
    'bootstrap.servers                       ',
    '*',
    '               ',
    '             ',
    'high      ',
    `Alias for \`metadata.broker.list\`: Initial list of brokers as a CSV list of broker host or host:port. The application may also use \`rd_kafka_brokers_add()\` to add brokers during runtime.  \n*Type: CSV list*`
  ],
  [
    'message.max.bytes                       ',
    '*',
    '1000 .. 1000000000',
    '      1000000',
    'medium    ',
    `Maximum Kafka protocol request message size. Due to differing framing overhead between protocol versions the producer is unable to reliably enforce a strict max message limit at produce time and may exceed the maximum size by one message in protocol ProduceRequests, the broker will enforce the the topic's \`max.message.bytes\` limit (see Apache Kafka documentation).  \n*Type: integer*`
  ],
  [
    'message.copy.max.bytes                  ',
    '*',
    '0 .. 1000000000',
    '        65535',
    'low       ',
    `Maximum size for message to be copied to buffer. Messages larger than this will be passed by reference (zero-copy) at the expense of larger iovecs.  \n*Type: integer*`
  ],
  [
    'receive.message.max.bytes               ',
    '*',
    '1000 .. 2147483647',
    '    100000000',
    'medium    ',
    `Maximum Kafka protocol response message size. This serves as a safety precaution to avoid memory exhaustion in case of protocol hickups. This value must be at least \`fetch.max.bytes\`  + 512 to allow for protocol overhead; the value is adjusted automatically unless the configuration property is explicitly set.  \n*Type: integer*`
  ],
  [
    'max.in.flight                           ',
    '*',
    '1 .. 1000000   ',
    '      1000000',
    'low       ',
    `Alias for \`max.in.flight.requests.per.connection\`: Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests. In particular, note that other mechanisms limit the number of outstanding consumer fetch request per broker to one.  \n*Type: integer*`
  ],
  [
    'topic.metadata.refresh.interval.ms      ',
    '*',
    '-1 .. 3600000  ',
    '       300000',
    'low       ',
    `Period of time in milliseconds at which topic and broker metadata is refreshed in order to proactively discover any new brokers, topics, partitions or partition leader changes. Use -1 to disable the intervalled refresh (not recommended). If there are no locally referenced topics (no topic objects created, no messages produced, no subscription or no assignment) then only the broker list will be refreshed every interval but no more often than every 10s.  \n*Type: integer*`
  ],
  [
    'metadata.max.age.ms                     ',
    '*',
    '1 .. 86400000  ',
    '       900000',
    'low       ',
    `Metadata cache max age. Defaults to topic.metadata.refresh.interval.ms * 3  \n*Type: integer*`
  ],
  [
    'topic.metadata.refresh.fast.interval.ms ',
    '*',
    '1 .. 60000     ',
    '          100',
    'low       ',
    `When a topic loses its leader a new metadata request will be enqueued immediately and then with this initial interval, exponentially increasing upto \`retry.backoff.max.ms\`], until the topic metadata has been refreshed. If not set explicitly, it will be defaulted to \`retry.backoff.ms\`. This is used to recover quickly from transitioning leader brokers.  \n*Type: integer*`
  ],
  [
    'topic.metadata.refresh.sparse           ',
    '*',
    'true, false    ',
    '         true',
    'low       ',
    `Sparse metadata requests (consumes less network bandwidth)  \n*Type: boolean*`
  ],
  [
    'topic.metadata.propagation.max.ms       ',
    '*',
    '0 .. 3600000   ',
    '        30000',
    'low       ',
    `Apache Kafka topic creation is asynchronous and it takes some time for a new topic to propagate throughout the cluster to all brokers. If a client requests topic metadata after manual topic creation but before the topic has been fully propagated to the broker the client is requesting metadata from, the topic will seem to be non-existent and the client will mark the topic as such, failing queued produced messages with \`ERR__UNKNOWN_TOPIC\`. This setting delays marking a topic as non-existent until the configured propagation max time has passed. The maximum propagation time is calculated from the time the topic is first referenced in the client, e.g., on produce().  \n*Type: integer*`
  ],
  [
    'topic.blacklist                         ',
    '*',
    '               ',
    '             ',
    'low       ',
    `Topic blacklist, a comma-separated list of regular expressions for matching topic names that should be ignored in broker metadata information as if the topics did not exist.  \n*Type: pattern list*`
  ],
  [
    'socket.timeout.ms                       ',
    '*',
    '10 .. 300000   ',
    '        60000',
    'low       ',
    `Default timeout for network requests. Producer: ProduceRequests will use the lesser value of \`socket.timeout.ms\` and remaining \`message.timeout.ms\` for the first message in the batch. Consumer: FetchRequests will use \`fetch.wait.max.ms\` + \`socket.timeout.ms\`. Admin: Admin requests will use \`socket.timeout.ms\` or explicitly set \`rd_kafka_AdminOptions_set_operation_timeout()\` value.  \n*Type: integer*`
  ],
  [
    'socket.send.buffer.bytes                ',
    '*',
    '0 .. 100000000 ',
    '            0',
    'low       ',
    `Broker socket send buffer size. System default is used if 0.  \n*Type: integer*`
  ],
  [
    'socket.receive.buffer.bytes             ',
    '*',
    '0 .. 100000000 ',
    '            0',
    'low       ',
    `Broker socket receive buffer size. System default is used if 0.  \n*Type: integer*`
  ],
  [
    'socket.keepalive.enable                 ',
    '*',
    'true, false    ',
    '        false',
    'low       ',
    `Enable TCP keep-alives (SO_KEEPALIVE) on broker sockets  \n*Type: boolean*`
  ],
  [
    'socket.nagle.disable                    ',
    '*',
    'true, false    ',
    '        false',
    'low       ',
    `Disable the Nagle algorithm (TCP_NODELAY) on broker sockets.  \n*Type: boolean*`
  ],
  [
    'socket.max.fails                        ',
    '*',
    '0 .. 1000000   ',
    '            1',
    'low       ',
    `Disconnect from broker when this number of send failures (e.g., timed out requests) is reached. Disable with 0. WARNING: It is highly recommended to leave this setting at its default value of 1 to avoid the client and broker to become desynchronized in case of request timeouts. NOTE: The connection is automatically re-established.  \n*Type: integer*`
  ],
  [
    'broker.address.ttl                      ',
    '*',
    '0 .. 86400000  ',
    '         1000',
    'low       ',
    `How long to cache the broker address resolving results (milliseconds).  \n*Type: integer*`
  ],
  [
    'broker.address.family                   ',
    '*',
    'any, v4, v6    ',
    '          any',
    'low       ',
    `Allowed broker IP address families: any, v4, v6  \n*Type: enum value*`
  ],
  [
    'socket.connection.setup.timeout.ms      ',
    '*',
    '1000 .. 2147483647',
    '        30000',
    'medium    ',
    `Maximum time allowed for broker connection setup (TCP connection setup as well SSL and SASL handshake). If the connection to the broker is not fully functional after this the connection will be closed and retried.  \n*Type: integer*`
  ],
  [
    'connections.max.idle.ms                 ',
    '*',
    '0 .. 2147483647',
    '            0',
    'medium    ',
    `Close broker connections after the specified time of inactivity. Disable with 0. If this property is left at its default value some heuristics are performed to determine a suitable default value, this is currently limited to identifying brokers on Azure (see librdkafka issue #3109 for more info).  \n*Type: integer*`
  ],
  [
    'reconnect.backoff.ms                    ',
    '*',
    '0 .. 3600000   ',
    '          100',
    'medium    ',
    `The initial time to wait before reconnecting to a broker after the connection has been closed. The time is increased exponentially until \`reconnect.backoff.max.ms\` is reached. -25% to +50% jitter is applied to each reconnect backoff. A value of 0 disables the backoff and reconnects immediately.  \n*Type: integer*`
  ],
  [
    'reconnect.backoff.max.ms                ',
    '*',
    '0 .. 3600000   ',
    '        10000',
    'medium    ',
    `The maximum time to wait before reconnecting to a broker after the connection has been closed.  \n*Type: integer*`
  ],
  [
    'log.thread.name                         ',
    '*',
    'true, false    ',
    '         true',
    'low       ',
    `Print internal thread name in log messages (useful for debugging librdkafka internals)  \n*Type: boolean*`
  ],
  [
    'log.connection.close                    ',
    '*',
    'true, false    ',
    '         true',
    'low       ',
    `Log broker disconnects. It might be useful to turn this off when interacting with 0.9 brokers with an aggressive \`connections.max.idle.ms\` value.  \n*Type: boolean*`
  ],
  [
    'api.version.request                     ',
    '*',
    'true, false    ',
    '         true',
    'high      ',
    `Request broker's supported API versions to adjust functionality to available protocol features. If set to false, or the ApiVersionRequest fails, the fallback version \`broker.version.fallback\` will be used. **NOTE**: Depends on broker version >=0.10.0. If the request is not supported by (an older) broker the \`broker.version.fallback\` fallback is used.  \n*Type: boolean*`
  ],
  [
    'api.version.request.timeout.ms          ',
    '*',
    '1 .. 300000    ',
    '        10000',
    'low       ',
    `Timeout for broker API version requests.  \n*Type: integer*`
  ],
  [
    'api.version.fallback.ms                 ',
    '*',
    '0 .. 604800000 ',
    '            0',
    'medium    ',
    `Dictates how long the \`broker.version.fallback\` fallback is used in the case the ApiVersionRequest fails. **NOTE**: The ApiVersionRequest is only issued when a new connection to the broker is made (such as after an upgrade).  \n*Type: integer*`
  ],
  [
    'broker.version.fallback                 ',
    '*',
    '               ',
    '       0.10.0',
    'medium    ',
    `Older broker versions (before 0.10.0) provide no way for a client to query for supported protocol features (ApiVersionRequest, see \`api.version.request\`) making it impossible for the client to know what features it may use. As a workaround a user may set this property to the expected broker version and the client will automatically adjust its feature set accordingly if the ApiVersionRequest fails (or is disabled). The fallback broker version will be used for \`api.version.fallback.ms\`. Valid values are: 0.9.0, 0.8.2, 0.8.1, 0.8.0. Any other value >= 0.10, such as 0.10.2.1, enables ApiVersionRequests.  \n*Type: string*`
  ],
  [
    'allow.auto.create.topics                ',
    '*',
    'true, false    ',
    '        false',
    'low       ',
    `Allow automatic topic creation on the broker when subscribing to or assigning non-existent topics. The broker must also be configured with \`auto.create.topics.enable=true\` for this configuration to take effect. Note: the default value (true) for the producer is different from the default value (false) for the consumer. Further, the consumer default value is different from the Java consumer (true), and this property is not supported by the Java producer. Requires broker version >= 0.11.0.0, for older broker versions only the broker configuration applies.  \n*Type: boolean*`
  ],
  [
    'security.protocol                       ',
    '*',
    'plaintext, ssl, sasl_plaintext, sasl_ssl',
    '    plaintext',
    'high      ',
    `Protocol used to communicate with brokers.  \n*Type: enum value*`
  ],
  [
    'ssl.cipher.suites                       ',
    '*',
    '               ',
    '             ',
    'low       ',
    `A cipher suite is a named combination of authentication, encryption, MAC and key exchange algorithm used to negotiate the security settings for a network connection using TLS or SSL network protocol. See manual page for \`ciphers(1)\` and \`SSL_CTX_set_cipher_list(3).  \n*Type: string*`
  ],
  [
    'ssl.curves.list                         ',
    '*',
    '               ',
    '             ',
    'low       ',
    `The supported-curves extension in the TLS ClientHello message specifies the curves (standard/named, or 'explicit' GF(2^k) or GF(p)) the client is willing to have the server use. See manual page for \`SSL_CTX_set1_curves_list(3)\`. OpenSSL >= 1.0.2 required.  \n*Type: string*`
  ],
  [
    'ssl.sigalgs.list                        ',
    '*',
    '               ',
    '             ',
    'low       ',
    `The client uses the TLS ClientHello signature_algorithms extension to indicate to the server which signature/hash algorithm pairs may be used in digital signatures. See manual page for \`SSL_CTX_set1_sigalgs_list(3)\`. OpenSSL >= 1.0.2 required.  \n*Type: string*`
  ],
  [
    'ssl.key.location                        ',
    '*',
    '               ',
    '             ',
    'low       ',
    `Path to client's private key (PEM) used for authentication.  \n*Type: string*`
  ],
  [
    'ssl.key.password                        ',
    '*',
    '               ',
    '             ',
    'low       ',
    `Private key passphrase (for use with \`ssl.key.location\` and \`set_ssl_cert()\`)  \n*Type: string*`
  ],
  [
    'ssl.key.pem                             ',
    '*',
    '               ',
    '             ',
    'low       ',
    `Client's private key string (PEM format) used for authentication.  \n*Type: string*`
  ],
  [
    'ssl_key                                 ',
    '*',
    '               ',
    '             ',
    'low       ',
    `Client's private key as set by rd_kafka_conf_set_ssl_cert()  \n*Type: see dedicated API*`
  ],
  [
    'ssl.certificate.location                ',
    '*',
    '               ',
    '             ',
    'low       ',
    `Path to client's public key (PEM) used for authentication.  \n*Type: string*`
  ],
  [
    'ssl.certificate.pem                     ',
    '*',
    '               ',
    '             ',
    'low       ',
    `Client's public key string (PEM format) used for authentication.  \n*Type: string*`
  ],
  [
    'ssl_certificate                         ',
    '*',
    '               ',
    '             ',
    'low       ',
    `Client's public key as set by rd_kafka_conf_set_ssl_cert()  \n*Type: see dedicated API*`
  ],
  [
    'ssl.ca.location                         ',
    '*',
    '               ',
    '             ',
    'low       ',
    `File or directory path to CA certificate(s) for verifying the broker's key. Defaults: On Windows the system's CA certificates are automatically looked up in the Windows Root certificate store. On Mac OSX this configuration defaults to \`probe\`. It is recommended to install openssl using Homebrew, to provide CA certificates. On Linux install the distribution's ca-certificates package. If OpenSSL is statically linked or \`ssl.ca.location\` is set to \`probe\` a list of standard paths will be probed and the first one found will be used as the default CA certificate location path. If OpenSSL is dynamically linked the OpenSSL library's default path will be used (see \`OPENSSLDIR\` in \`openssl version -a\`).  \n*Type: string*`
  ],
  [
    'ssl.ca.pem                              ',
    '*',
    '               ',
    '             ',
    'low       ',
    `CA certificate string (PEM format) for verifying the broker's key.  \n*Type: string*`
  ],
  [
    'ssl.ca.certificate.stores               ',
    '*',
    '               ',
    '         Root',
    'low       ',
    `Comma-separated list of Windows Certificate stores to load CA certificates from. Certificates will be loaded in the same order as stores are specified. If no certificates can be loaded from any of the specified stores an error is logged and the OpenSSL library's default CA location is used instead. Store names are typically one or more of: MY, Root, Trust, CA.  \n*Type: string*`
  ],
  [
    'ssl.crl.location                        ',
    '*',
    '               ',
    '             ',
    'low       ',
    `Path to CRL for verifying broker's certificate validity.  \n*Type: string*`
  ],
  [
    'ssl.keystore.location                   ',
    '*',
    '               ',
    '             ',
    'low       ',
    `Path to client's keystore (PKCS#12) used for authentication.  \n*Type: string*`
  ],
  [
    'ssl.keystore.password                   ',
    '*',
    '               ',
    '             ',
    'low       ',
    `Client's keystore (PKCS#12) password.  \n*Type: string*`
  ],
  [
    'ssl.providers                           ',
    '*',
    '               ',
    '             ',
    'low       ',
    `Comma-separated list of OpenSSL 3.0.x implementation providers. E.g., "default,legacy".  \n*Type: string*`
  ],
  [
    'ssl.engine.id                           ',
    '*',
    '               ',
    '      dynamic',
    'low       ',
    `OpenSSL engine id is the name used for loading engine.  \n*Type: string*`
  ],
  [
    'enable.ssl.certificate.verification     ',
    '*',
    'true, false    ',
    '         true',
    'low       ',
    `Enable OpenSSL's builtin broker (server) certificate verification. This verification can be extended by the application by implementing a certificate_verify_cb.  \n*Type: boolean*`
  ],
  [
    'ssl.endpoint.identification.algorithm   ',
    '*',
    'none, https    ',
    '        https',
    'low       ',
    `Endpoint identification algorithm to validate broker hostname using broker certificate. https - Server (broker) hostname verification as specified in RFC2818. none - No endpoint verification. OpenSSL >= 1.0.2 required.  \n*Type: enum value*`
  ],
  [
    'sasl.mechanism                          ',
    '*',
    '               ',
    '       GSSAPI',
    'high      ',
    `Alias for \`sasl.mechanisms\`: SASL mechanism to use for authentication. Supported: GSSAPI, PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER. \n*Type: string*`
  ],
  [
    'sasl.kerberos.service.name              ',
    '*',
    '               ',
    '        kafka',
    'low       ',
    `Kerberos principal name that Kafka runs as, not including /hostname@REALM  \n*Type: string*`
  ],
  [
    'sasl.kerberos.principal                 ',
    '*',
    '               ',
    '  kafkaclient',
    'low       ',
    `This client's Kerberos principal name. (Not supported on Windows, will use the logon user's principal).  \n*Type: string*`
  ],
  [
    'sasl.kerberos.min.time.before.relogin   ',
    '*',
    '0 .. 86400000  ',
    '        60000',
    'low       ',
    `Minimum time in milliseconds between key refresh attempts. Disable automatic key refresh by setting this property to 0.  \n*Type: integer*`
  ],
  [
    'sasl.username                           ',
    '*',
    '               ',
    '             ',
    'high      ',
    `SASL username for use with the PLAIN and SASL-SCRAM-.. mechanisms  \n*Type: string*`
  ],
  [
    'sasl.password                           ',
    '*',
    '               ',
    '             ',
    'high      ',
    `SASL password for use with the PLAIN and SASL-SCRAM-.. mechanism  \n*Type: string*`
  ],
  [
    'sasl.oauthbearer.config                 ',
    '*',
    '               ',
    '             ',
    'low       ',
    `SASL/OAUTHBEARER configuration. The format is implementation-dependent and must be parsed accordingly. The default unsecured token implementation (see https://tools.ietf.org/html/rfc7515#appendix-A.5) recognizes space-separated name=value pairs with valid names including principalClaimName, principal, scopeClaimName, scope, and lifeSeconds. The default value for principalClaimName is "sub", the default value for scopeClaimName is "scope", and the default value for lifeSeconds is 3600. The scope value is CSV format with the default value being no/empty scope. For example: \`principalClaimName=azp principal=admin scopeClaimName=roles scope=role1,role2 lifeSeconds=600\`. In addition, SASL extensions can be communicated to the broker via \`extension_NAME=value\`. For example: \`principal=admin extension_traceId=123\`  \n*Type: string*`
  ],
  [
    'enable.sasl.oauthbearer.unsecure.jwt    ',
    '*',
    'true, false    ',
    '        false',
    'low       ',
    `Enable the builtin unsecure JWT OAUTHBEARER token handler if no oauthbearer_refresh_cb has been set. This builtin handler should only be used for development or testing, and not in production.  \n*Type: boolean*`
  ],
  [
    'sasl.oauthbearer.method                 ',
    '*',
    'default, oidc  ',
    '      default',
    'low       ',
    `Set to "default" or "oidc" to control which login method to be used. If set to "oidc", the following properties must also be be specified: \`sasl.oauthbearer.client.id\`], \`sasl.oauthbearer.client.secret\`], and \`sasl.oauthbearer.token.endpoint.url\`.  \n*Type: enum value*`
  ],
  [
    'sasl.oauthbearer.client.id              ',
    '*',
    '               ',
    '             ',
    'low       ',
    `Public identifier for the application. Must be unique across all clients that the authorization server handles. Only used when \`sasl.oauthbearer.method\` is set to "oidc".  \n*Type: string*`
  ],
  [
    'sasl.oauthbearer.client.secret          ',
    '*',
    '               ',
    '             ',
    'low       ',
    `Client secret only known to the application and the authorization server. This should be a sufficiently random string that is not guessable. Only used when \`sasl.oauthbearer.method\` is set to "oidc".  \n*Type: string*`
  ],
  [
    'sasl.oauthbearer.scope                  ',
    '*',
    '               ',
    '             ',
    'low       ',
    `Client use this to specify the scope of the access request to the broker. Only used when \`sasl.oauthbearer.method\` is set to "oidc".  \n*Type: string*`
  ],
  [
    'sasl.oauthbearer.extensions             ',
    '*',
    '               ',
    '             ',
    'low       ',
    `Allow additional information to be provided to the broker. Comma-separated list of key=value pairs. E.g., "supportFeatureX=true,organizationId=sales-emea".Only used when \`sasl.oauthbearer.method\` is set to "oidc".  \n*Type: string*`
  ],
  [
    'sasl.oauthbearer.token.endpoint.url     ',
    '*',
    '               ',
    '             ',
    'low       ',
    `OAuth/OIDC issuer token endpoint HTTP(S) URI used to retrieve token. Only used when \`sasl.oauthbearer.method\` is set to "oidc".  \n*Type: string*`
  ],
  [
    'group.id                                ',
    'C',
    '               ',
    '             ',
    'high      ',
    `Client group id string. All clients sharing the same group.id belong to the same group.  \n*Type: string*`
  ],
  [
    'group.instance.id                       ',
    'C',
    '               ',
    '             ',
    'medium    ',
    `Enable static group membership. Static group members are able to leave and rejoin a group within the configured \`session.timeout.ms\` without prompting a group rebalance. This should be used in combination with a larger \`session.timeout.ms\` to avoid group rebalances caused by transient unavailability (e.g. process restarts). Requires broker version >= 2.3.0.  \n*Type: string*`
  ],
  [
    'partition.assignment.strategy           ',
    'C',
    '               ',
    'range,roundrobin',
    'medium    ',
    `The name of one or more partition assignment strategies. The elected group leader will use a strategy supported by all members of the group to assign partitions to group members. If there is more than one eligible strategy, preference is determined by the order of this list (strategies earlier in the list have higher priority). Cooperative and non-cooperative (eager) strategies must not be mixed. Available strategies: range, roundrobin, cooperative-sticky.  \n*Type: string*`
  ],
  [
    'session.timeout.ms                      ',
    'C',
    '1 .. 3600000   ',
    '        45000',
    'high      ',
    `Client group session and failure detection timeout. The consumer sends periodic heartbeats (heartbeat.interval.ms) to indicate its liveness to the broker. If no hearts are received by the broker for a group member within the session timeout, the broker will remove the consumer from the group and trigger a rebalance. The allowed range is configured with the **broker** configuration properties \`group.min.session.timeout.ms\` and \`group.max.session.timeout.ms\`. Also see \`max.poll.interval.ms\`.  \n*Type: integer*`
  ],
  [
    'heartbeat.interval.ms                   ',
    'C',
    '1 .. 3600000   ',
    '         3000',
    'low       ',
    `Group session keepalive heartbeat interval.  \n*Type: integer*`
  ],
  [
    'coordinator.query.interval.ms           ',
    'C',
    '1 .. 3600000   ',
    '       600000',
    'low       ',
    `How often to query for the current client group coordinator. If the currently assigned coordinator is down the configured query interval will be divided by ten to more quickly recover in case of coordinator reassignment.  \n*Type: integer*`
  ],
  [
    'max.poll.interval.ms                    ',
    'C',
    '1 .. 86400000  ',
    '       300000',
    'high      ',
    `Maximum allowed time between calls to consume messages (e.g., rd_kafka_consumer_poll()) for high-level consumers. If this interval is exceeded the consumer is considered failed and the group will rebalance in order to reassign the partitions to another consumer group member. Warning: Offset commits may be not possible at this point. Note: It is recommended to set \`enable.auto.offset.store=false\` for long-time processing applications and then explicitly store offsets (using offsets_store()) *after* message processing, to make sure offsets are not auto-committed prior to processing has finished. The interval is checked two times per second. See KIP-62 for more information.  \n*Type: integer*`
  ],
  [
    'auto.commit.interval.ms                 ',
    'C',
    '0 .. 86400000  ',
    '         5000',
    'medium    ',
    `The frequency in milliseconds that the consumer offsets are committed (written) to offset storage. (0 = disable). This setting is used by the high-level consumer.  \n*Type: integer*`
  ],
  [
    'queued.min.messages                     ',
    'C',
    '1 .. 10000000  ',
    '       100000',
    'medium    ',
    `Minimum number of messages per topic+partition librdkafka tries to maintain in the local consumer queue.  \n*Type: integer*`
  ],
  [
    'queued.max.messages.kbytes              ',
    'C',
    '1 .. 2097151   ',
    '        65536',
    'medium    ',
    `Maximum number of kilobytes of queued pre-fetched messages in the local consumer queue. If using the high-level consumer this setting applies to the single consumer queue, regardless of the number of partitions. When using the legacy simple consumer or when separate partition queues are used this setting applies per partition. This value may be overshot by fetch.message.max.bytes. This property has higher priority than queued.min.messages.  \n*Type: integer*`
  ],
  [
    'fetch.wait.max.ms                       ',
    'C',
    '0 .. 300000    ',
    '          500',
    'low       ',
    `Maximum time the broker may wait to fill the Fetch response with fetch.min.bytes of messages.  \n*Type: integer*`
  ],
  [
    'fetch.queue.backoff.ms                  ',
    'C',
    '0 .. 300000    ',
    '         1000',
    'medium    ',
    `How long to postpone the next fetch request for a topic+partition in case the current fetch queue thresholds (queued.min.messages or queued.max.messages.kbytes) have been exceded. This property may need to be decreased if the queue thresholds are set low and the application is experiencing long (~1s) delays between messages. Low values may increase CPU utilization.  \n*Type: integer*`
  ],
  [
    'max.partition.fetch.bytes               ',
    'C',
    '1 .. 1000000000',
    '      1048576',
    'medium    ',
    `Alias for \`fetch.message.max.bytes\`: Initial maximum number of bytes per topic+partition to request when fetching messages from the broker. If the client encounters a message larger than this value it will gradually try to increase it until the entire message can be fetched.  \n*Type: integer*`
  ],
  [
    'fetch.max.bytes                         ',
    'C',
    '0 .. 2147483135',
    '     52428800',
    'medium    ',
    `Maximum amount of data the broker shall return for a Fetch request. Messages are fetched in batches by the consumer and if the first message batch in the first non-empty partition of the Fetch request is larger than this value, then the message batch will still be returned to ensure the consumer can make progress. The maximum message batch size accepted by the broker is defined via \`message.max.bytes\` (broker config) or \`max.message.bytes\` (broker topic config). \`fetch.max.bytes\` is automatically adjusted upwards to be at least \`message.max.bytes\` (consumer config).  \n*Type: integer*`
  ],
  [
    'fetch.min.bytes                         ',
    'C',
    '1 .. 100000000 ',
    '            1',
    'low       ',
    `Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires the accumulated data will be sent to the client regardless of this setting.  \n*Type: integer*`
  ],
  [
    'fetch.error.backoff.ms                  ',
    'C',
    '0 .. 300000    ',
    '          500',
    'medium    ',
    `How long to postpone the next fetch request for a topic+partition in case of a fetch error.  \n*Type: integer*`
  ],
  [
    'isolation.level                         ',
    'C',
    'read_uncommitted, read_committed',
    'read_committed',
    'high      ',
    `Controls how to read messages written transactionally: \`read_committed\` - only return transactional messages which have been committed. \`read_uncommitted\` - return all messages, even transactional messages which have been aborted.  \n*Type: enum value*`
  ],
  [
    'enable.partition.eof                    ',
    'C',
    'true, false    ',
    '        false',
    'low       ',
    `Emit RD_KAFKA_RESP_ERR__PARTITION_EOF event whenever the consumer reaches the end of a partition.  \n*Type: boolean*`
  ],
  [
    'check.crcs                              ',
    'C',
    'true, false    ',
    '        false',
    'medium    ',
    `Verify CRC32 of consumed messages, ensuring no on-the-wire or on-disk corruption to the messages occurred. This check comes at slightly increased CPU usage.  \n*Type: boolean*`
  ],
  [
    'client.rack                             ',
    '*',
    '               ',
    '             ',
    'low       ',
    `A rack identifier for this client. This can be any string value which indicates where this client is physically located. It corresponds with the broker config \`broker.rack\`.  \n*Type: string*`
  ],
  [
    'enable.idempotence                      ',
    'P',
    'true, false    ',
    '        false',
    'high      ',
    `When set to \`true\`], the producer will ensure that messages are successfully produced exactly once and in the original produce order. The following configuration properties are adjusted automatically (if not modified by the user) when idempotence is enabled: \`max.in.flight.requests.per.connection=5\` (must be less than or equal to 5), \`retries=INT32_MAX\` (must be greater than 0), \`acks=all\`], \`queuing.strategy=fifo\`. Producer instantation will fail if user-supplied configuration is incompatible.  \n*Type: boolean*`
  ],
  [
    'enable.gapless.guarantee                ',
    'P',
    'true, false    ',
    '        false',
    'low       ',
    `**EXPERIMENTAL**: subject to change or removal. When set to \`true\`], any error that could result in a gap in the produced message series when a batch of messages fails, will raise a fatal error (ERR__GAPLESS_GUARANTEE) and stop the producer. Messages failing due to \`message.timeout.ms\` are not covered by this guarantee. Requires \`enable.idempotence=true\`.  \n*Type: boolean*`
  ],
  [
    'queue.buffering.max.messages            ',
    'P',
    '0 .. 2147483647',
    '       100000',
    'high      ',
    `Maximum number of messages allowed on the producer queue. This queue is shared by all topics and partitions. A value of 0 disables this limit.  \n*Type: integer*`
  ],
  [
    'queue.buffering.max.kbytes              ',
    'P',
    '1 .. 2147483647',
    '      1048576',
    'high      ',
    `Maximum total message size sum allowed on the producer queue. This queue is shared by all topics and partitions. This property has higher priority than queue.buffering.max.messages.  \n*Type: integer*`
  ],
  [
    'linger.ms                               ',
    'P',
    '0 .. 900000    ',
    '            5',
    'high      ',
    `Alias for \`queue.buffering.max.ms\`: Delay in milliseconds to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers. A higher value allows larger and more effective (less overhead, improved compression) batches of messages to accumulate at the expense of increased message delivery latency.  \n*Type: float*`
  ],
  [
    'retries                                 ',
    'P',
    '0 .. 2147483647',
    '   2147483647',
    'high      ',
    `Alias for \`message.send.max.retries\`: How many times to retry sending a failing Message. **Note:** retrying may cause reordering unless \`enable.idempotence\` is set to true.  \n*Type: integer*`
  ],
  [
    'retry.backoff.ms                        ',
    'P',
    '1 .. 300000    ',
    '          100',
    'medium    ',
    `The backoff time in milliseconds before retrying a protocol request, this is the first backoff time, and will be backed off exponentially until number of retries is exhausted, and it's capped by retry.backoff.max.ms.  \n*Type: integer*`
  ],
  [
    'retry.backoff.max.ms                    ',
    'P',
    '1 .. 300000    ',
    '         1000',
    'medium    ',
    `The max backoff time in milliseconds before retrying a protocol request, this is the atmost backoff allowed for exponentially backed off requests.  \n*Type: integer*`
  ],
  [
    'queue.buffering.backpressure.threshold  ',
    'P',
    '1 .. 1000000   ',
    '            1',
    'low       ',
    `The threshold of outstanding not yet transmitted broker requests needed to backpressure the producer's message accumulator. If the number of not yet transmitted requests equals or exceeds this number, produce request creation that would have otherwise been triggered (for example, in accordance with linger.ms) will be delayed. A lower number yields larger and more effective batches. A higher value can improve latency when using compression on slow machines.  \n*Type: integer*`
  ],
  [
    'compression.type                        ',
    'P',
    'none, gzip, snappy, lz4, zstd',
    '         none',
    'medium    ',
    `Alias for \`compression.codec\`: compression codec to use for compressing message sets. This is the default value for all topics, may be overridden by the topic configuration property \`compression.codec\`.   \n*Type: enum value*`
  ],
  [
    'batch.num.messages                      ',
    'P',
    '1 .. 1000000   ',
    '        10000',
    'medium    ',
    `Maximum number of messages batched in one MessageSet. The total MessageSet size is also limited by batch.size and message.max.bytes.  \n*Type: integer*`
  ],
  [
    'batch.size                              ',
    'P',
    '1 .. 2147483647',
    '      1000000',
    'medium    ',
    `Maximum size (in bytes) of all messages batched in one MessageSet, including protocol framing overhead. This limit is applied after the first message has been added to the batch, regardless of the first message's size, this is to ensure that messages that exceed batch.size are produced. The total MessageSet size is also limited by batch.num.messages and message.max.bytes.  \n*Type: integer*`
  ],
  [
    'sticky.partitioning.linger.ms           ',
    'P',
    '0 .. 900000    ',
    '           10',
    'low       ',
    `Delay in milliseconds to wait to assign new sticky partitions for each topic. By default, set to double the time of linger.ms. To disable sticky behavior, set to 0. This behavior affects messages with the key NULL in all cases, and messages with key lengths of zero when the consistent_random partitioner is in use. These messages would otherwise be assigned randomly. A higher value allows for more effective batching of these messages.  \n*Type: integer*`
  ],
  [
    'client.dns.lookup                       ',
    '*',
    'use_all_dns_ips, resolve_canonical_bootstrap_servers_only',
    'use_all_dns_ips',
    'low       ',
    `Controls how the client uses DNS lookups. By default, when the lookup returns multiple IP addresses for a hostname, they will all be attempted for connection before the connection is considered failed. This applies to both bootstrap and advertised servers. If the value is set to \`resolve_canonical_bootstrap_servers_only\`], each entry will be resolved and expanded into a list of canonical names. NOTE: Default here is different from the Java client's default behavior, which connects only to the first IP address returned for a hostname.   \n*Type: enum value*`
  ]
]
  .concat([
    [
      'acks                                     ',
      'P',
      ' -1 .. 1000                                                ',
      '            -1    ',
      ' high      ',
      `Alias for \`request.required.acks\`: This field indicates the number of acknowledgements the leader broker must receive from ISR brokers before responding to the request: *0*=Broker does not send any response/ack to client, *-1* or *all*=Broker will block until message is committed by all in sync replicas (ISRs). If there are less than \`min.insync.replicas\` (broker configuration) in the ISR set the produce request will fail.  \n*Type: integer*`
    ],
    [
      'request.timeout.ms                       ',
      'P',
      ' 1 .. 900000                                               ',
      '         30000    ',
      ' medium    ',
      `The ack timeout of the producer request in milliseconds. This value is only enforced by the broker and relies on \`request.required.acks\` being != 0.  \n*Type: integer*`
    ],
    [
      'delivery.timeout.ms                      ',
      'P',
      ' 0 .. 2147483647                                           ',
      '        300000    ',
      ' high      ',
      `Alias for \`message.timeout.ms\`: Local message timeout. This value is only enforced locally and limits the time a produced message waits for successful delivery. A time of 0 is infinite. This is the maximum time librdkafka may use to deliver a message (including retries). Delivery error occurs when either the retry count or the message timeout are exceeded. The message timeout is automatically adjusted to \`transaction.timeout.ms\` if \`transactional.id\` is configured.  \n*Type: integer*`
    ],
    [
      'partitioner                              ',
      'P',
      '                                                           ',
      ' consistent_random',
      ' high      ',
      `Partitioner: \`random\` - random distribution, \`consistent\` - CRC32 hash of key (Empty and NULL keys are mapped to single partition), \`consistent_random\` - CRC32 hash of key (Empty and NULL keys are randomly partitioned), \`murmur2\` - Java Producer compatible Murmur2 hash of key (NULL keys are mapped to single partition), \`murmur2_random\` - Java Producer compatible Murmur2 hash of key (NULL keys are randomly partitioned. This is functionally equivalent to the default partitioner in the Java Producer.), \`fnv1a\` - FNV-1a hash of key (NULL keys are mapped to single partition), \`fnv1a_random\` - FNV-1a hash of key (NULL keys are randomly partitioned).  \n*Type: string*`
    ],
    [
      'opaque                                   ',
      '*',
      '                                                           ',
      '                  ',
      ' low       ',
      `Application opaque (set with rd_kafka_topic_conf_set_opaque())  \n*Type: see dedicated API*`
    ],
    [
      'compression.type                         ',
      'P',
      ' none, gzip, snappy, lz4, zstd                             ',
      '          none    ',
      ' medium    ',
      `Alias for \`compression.codec\`: compression codec to use for compressing message sets. This is the default value for all topics, may be overridden by the topic configuration property \`compression.codec\`.   \n*Type: enum value*`
    ],
    [
      'compression.level                        ',
      'P',
      ' -1 .. 12                                                  ',
      '            -1    ',
      ' medium    ',
      `Compression level parameter for algorithm selected by configuration property \`compression.codec\`. Higher values will result in better compression at the cost of more CPU usage. Usable range is algorithm-dependent: [0-9] for gzip; [0-12] for lz4; only 0 for snappy; -1 = codec-dependent default compression level.  \n*Type: integer*`
    ],
    [
      'auto.offset.reset                        ',
      'C',
      ' smallest, earliest, beginning, largest, latest, end, error',
      '       largest    ',
      ' high      ',
      `Action to take when there is no initial offset in offset store or the desired offset is out of range: 'smallest','earliest' - automatically reset the offset to the smallest offset, 'largest','latest' - automatically reset the offset to the largest offset, 'error' - trigger an error (ERR__AUTO_OFFSET_RESET) which is retrieved by consuming messages and checking 'message->err'.  \n*Type: enum value*`
    ],
    [
      'consume.callback.max.messages            ',
      'C',
      ' 0 .. 1000000                                              ',
      '             0    ',
      ' low       ',
      `Maximum number of messages to dispatch in one \`rd_kafka_consume_callback*()\` call (0 = unlimited)  \n*Type: integer*`
    ]
  ])
  .concat([
    [
      'topics                                  ',
      'C',
      '               ',
      '             ',
      'high      ',
      `A comma-delimited list of Kafka topics to read from  \n*Type: array*`
    ],
    [
      'topic                                   ',
      'P',
      '               ',
      '             ',
      'high      ',
      `The Kafka topic to write to  \n*Type: string*`
    ]
  ])
  .map((row) => {
    const data = {
      name: row[0].trim(),
      label: row[0].trim().replaceAll('_', '.'),
      scope: row[1].trim() as 'C' | 'P' | '*',
      range: row[2].trim(),
      default: row[3].trim(),
      importance: row[4].trim(),
      description: row[5],
      tooltip: row[5],
      type: deduceType(row)
    }
    if (data.type === 'number') {
      return {
        ...data,
        type: 'number' as const,
        range: (([, min, max]) => ({ min: parseInt(min), max: parseInt(max) }))(
          data.range.match(/(\d+) .. (\d+)/) ?? []
        )
      }
    }
    if (data.type === 'enum') {
      return {
        ...data,
        type: 'enum' as const,
        range: data.range.split(', ')
      }
    }
    return { ...data, type: data.type }
  })

export const librdkafkaAuthOptions = [
  'security.protocol',
  'ssl.key.pem',
  'ssl.certificate.pem',
  'enable.ssl.certificate.verification',
  'ssl.ca.pem',
  'sasl.mechanism',
  'sasl.username',
  'sasl.password',
  'sasl.oauthbearer.config',
  'enable.sasl.oauthbearer.unsecure.jwt',
  'sasl.oauthbearer.method',
  'sasl.oauthbearer.client.id',
  'sasl.oauthbearer.client.secret',
  'sasl.oauthbearer.token.endpoint.url',
  'sasl.oauthbearer.scope',
  'sasl.oauthbearer.extensions'
] as const

export const librdkafkaNonAuthFieldsSchema = (() => {
  const nonAuthFields = librdkafkaOptions
    .filter((o) => !librdkafkaAuthOptions.includes(o.name as any))
    .map((o) =>
      tuple(
        o.name.replaceAll('.', '_'),
        match(o)
          .with({ type: 'number' }, (option) => {
            const error = `Must be in the range of ${option.range.min} to ${option.range.max}`
            return va.pipe(
              va.number(),
              va.minValue(option.range.min, error),
              va.maxValue(option.range.max, error)
            )
          })
          .with({ type: 'enum' }, (option) => va.picklist(option.range as [string, ...string[]]))
          .with({ type: 'array' }, { type: 'list' }, () => va.array(va.string()))
          .with({ type: 'boolean' }, () => va.boolean())
          .with({ type: 'string' }, () => va.pipe(va.string(), va.minLength(1)))
          .exhaustive()
      )
    )
  return va.partial(va.object(Object.fromEntries(nonAuthFields)))
})()

export type LibrdkafkaOptionType = string | number | boolean | string[]

const toKafkaOption = (
  optionName: string,
  v: LibrdkafkaOptionType,
  type: ReturnType<typeof deduceType>
) => {
  return match(type)
    .with('boolean', 'number', () => String(v))
    .with('list', () => {
      invariant(Array.isArray(v), 'librdkafka option ' + optionName + ' ' + v + ' is not an array')
      return v?.join(', ')
    })
    .with('array', () => {
      invariant(Array.isArray(v), 'librdkafka option ' + optionName + ' ' + v + ' is not an array')
      return v as unknown as string // TODO: the hiding of string[] type is temporary until better typing in kafka-related APIs
    })
    .with('string', 'enum', () => {
      invariant(
        typeof v === 'string',
        'librdkafka option ' + optionName + ' ' + v + ' is not a string'
      )
      return v
    })
    .exhaustive()
}

/**
 * Convert a form object with underscore-delimited fields to a config object with dot-delimited fields
 *
 * Underscore-delimited fields are used with react-hook-form because its implementation
 * conflicts with dot-delimited fields
 */
export const toLibrdkafkaConfig = (formFields: Partial<Record<string, LibrdkafkaOptionType>>) => {
  const config = {} as Record<string, string>
  Object.keys(formFields).forEach((fieldName) => {
    const v = formFields[fieldName]
    if (v === undefined) {
      return
    }
    const optionName = fieldName.replaceAll('_', '.')
    // TODO: Optimize .find()
    const type = librdkafkaOptions.find((option) => option.name === optionName)?.type ?? 'string'
    config[optionName] = toKafkaOption(optionName, v, type)
  })
  return config
}

export const toKafkaConfig = ({
  preset_service,
  ...formFields
}: Record<string, LibrdkafkaOptionType>) =>
  ({
    kafka_service: preset_service,
    ...toLibrdkafkaConfig(formFields)
  }) as ReturnType<typeof toLibrdkafkaConfig>

/**
 * Convert a config with dot-delimited fields to a form object with underscore-delimited fields
 *
 * Underscore-delimited fields are used with react-hook-form because its implementation
 * conflicts with dot-delimited fields
 */
export const fromLibrdkafkaConfig = (config: Record<string, string | string[]>) => {
  const formFields = {} as Record<string, LibrdkafkaOptionType>
  Object.keys(config).forEach((optionName) => {
    const v = config[optionName]
    const fieldName = optionName.replaceAll('.', '_')
    // TODO: Optimize .find()
    const type = librdkafkaOptions.find((option) => option.name === optionName)?.type
    if (!type) {
      // Ignore options that are not in librdkafka spec
      return
    }
    formFields[fieldName] = match(type)
      .with('boolean', () =>
        v === 'true'
          ? true
          : v === 'false'
            ? false
            : (invariant(
                false,
                'librdkafka option ' + optionName + ' ' + v + ' is not a boolean'
              ) as never)
      )
      .with('number', () => parseInt(v as string))
      .with('list', () => (v as string).split(', '))
      .with('array', () => v)
      .with('string', 'enum', () => v)
      .exhaustive()
  })

  return formFields
}

export const fromKafkaConfig = ({
  kafka_service,
  ...config
}: Record<string, string | string[]>) => {
  return {
    ...(kafka_service ? { preset_service: kafka_service } : {}),
    ...fromLibrdkafkaConfig(config)
  } as ReturnType<typeof fromLibrdkafkaConfig>
}

export type LibrdkafkaOptions = (typeof librdkafkaOptions)[number]
