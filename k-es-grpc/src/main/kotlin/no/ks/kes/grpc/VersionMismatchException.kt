package no.ks.kes.grpc

import com.eventstore.dbclient.WrongExpectedVersionException

class VersionMismatchException(msg: String, e: WrongExpectedVersionException): RuntimeException(msg, e) {}