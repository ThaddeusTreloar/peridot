
We have added initial support for AsyncConsumer::async_poll, however, the QueueHead logic interprets Poll::Pending as there being no waiting messages and initiates a Commit. This can cause a race condition where the Poll::Pending is returned, but before QueueHead gets the consumer position, the consumer position is updated, and the QueueHead returns an offset higher than that which was seen by the pipeline. 

Originally, pulling commit offsets from consumer position was a solution to measuring changelog progress as a state store reading from a changelog where the latest transaction failed, would never rebuild as the QueueHead would never see an offset equal to the LSO.

Some potential mitigation strategies are:
 - QueueHead gets the consumer position before calling to poll.
 - 
