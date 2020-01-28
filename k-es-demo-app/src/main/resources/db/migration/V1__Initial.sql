CREATE TABLE cmd
(
    id              int IDENTITY PRIMARY KEY,
    aggregateId     nvarchar(255)  NOT NULL,
    nextExecution   datetime       NOT NULL,
    retries         INTEGER        NOT NULL,
    serializationId nvarchar(255)  NOT NULL,
    error           BIT            NOT NULL,
    errorId         nvarchar(255),
    data            nvarchar(4000) NOT NULL
)

CREATE TABLE saga
(
    correlationId   nvarchar(255)  NOT NULL,
    serializationId nvarchar(255)  NOT NULL,
    data            nvarchar(4000) NOT NULL,
    CONSTRAINT PK_saga PRIMARY KEY (correlationId, serializationId),
)

CREATE TABLE timeout
(
    sagaCorrelationId   nvarchar(255)  NOT NULL,
    sagaSerializationId nvarchar(255)  NOT NULL,
    timeoutId           nvarchar(255)  NOT NULL,
    timeout             datetime       NOT NULL,
    error               BIT            NOT NULL,
    errorId             nvarchar(255),
    data                nvarchar(4000) NOT NULL,
    CONSTRAINT PK_timeout PRIMARY KEY (sagaCorrelationId, sagaSerializationId, timeoutId),
)

CREATE TABLE hwm
(
    sagaHwm INTEGER NOT NULL
)

INSERT INTO hwm VALUES (0)