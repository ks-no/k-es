CREATE TABLE cmd (
    id int IDENTITY PRIMARY KEY,
    aggregateId nvarchar(255) NOT NULL,
    nextExecution datetime,
    retries INTEGER NOT NULL,
    serializationId nvarchar(255) NOT NULL,
    error BIT NOT NULL,
    errorId nvarchar(255),
    data nvarchar(4000)
);

CREATE TABLE saga (
   correlationId nvarchar(255) NOT NULL,
   serializationId nvarchar(255) NOT NULL,
   data nvarchar(4000) NOT NULL,
   CONSTRAINT PK_saga PRIMARY KEY (correlationId, serializationId),
)
