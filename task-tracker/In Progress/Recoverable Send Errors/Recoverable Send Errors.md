We need a way to recover from recoverable send errors yielded while enqueuing records for a producer.

Eg: if the producer queue is full, we want to store the record and start a commit.

May have to add an api for partition queues that allows for forceful committing.