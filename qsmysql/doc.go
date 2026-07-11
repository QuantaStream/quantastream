// Package qsmysql owns the MySQL wire-adapter byte contract.
//
// qsmysql is intentionally protocol-focused. It may encode and decode MySQL
// packets, greetings, commands, and resultset wire shapes, but it must not own
// SQL planning, bitmap execution, catalog semantics, or native query routing.
// Those concerns stay in qsbridge and qsruntime.
//
// The first implementation slice is deliberately socket-free. Packet framing,
// handshake payloads, and command decode can be tested as pure byte models
// before a TCP listener, authentication exchange, or resultset serializer is
// mounted.
package qsmysql
