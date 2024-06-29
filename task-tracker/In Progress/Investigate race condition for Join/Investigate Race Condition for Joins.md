When the left stream of a join is woken by a state store advancing it's event time, the stream join returns no RHS. Further message on left will join correctly. Suspecting some kind of race condition or instruction ordering problem.

Currently cannot reproduce the issue. will come back to this.