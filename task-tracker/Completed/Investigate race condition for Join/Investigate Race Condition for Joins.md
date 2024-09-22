When the left stream of a join is woken by a state store advancing it's event time, the stream join returns no RHS. Further message on left will join correctly. Suspecting some kind of race condition or instruction ordering problem.

Currently cannot reproduce the issue. will come back to this.

# Solution

It was found that the source stream for the state store had not found any messages yet, and therefore, woke all dependencies. There is not much to be done here as this behaviour is covered by `max.idle.time` setting