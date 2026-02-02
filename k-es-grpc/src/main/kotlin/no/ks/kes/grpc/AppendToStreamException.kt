package no.ks.kes.grpc

class AppendToStreamException (msg: String, e: Throwable?): RuntimeException(msg, e) {}