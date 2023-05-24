package no.ks.kes.grpc

class WriteAbortedException(msg: String, e: Throwable): RuntimeException(msg, e) {}