[<img width="215" src="https://github.com/hraberg/eyvind/raw/master/strandernas-svall.jpg" alt="Return to Ithaca" title="Return to Ithaca" align="right" />](https://en.wikipedia.org/wiki/Eyvind_Johnson)

# Eyvind

*for a narrative art, far-seeing in lands and ages, in the service of freedom*
-- Eyvind Johnson, Nobel Prize in Literature 1974 (shared with Harry Martinson)

*I was hacking on this during summer 2015, it doesn't really do anything yet. Has a Bitcask and some CRDT implementations in Clojure that might be of interest. I hope to revisit this in 2016*


### Eyvind is (not yet) an experimental distributed rule engine written in Clojure.

Foundations inspired by Riak Core. Higher level inspired by Dedalus, Linear Meld, CRDTs and Angelic CHR. Aims to provide Constraint Handling Rules as language, built on a lower-level distributed Datalog and linear logic. Minimal set of dependencies (JeroMQ and tools.logging) to facilitate use as library. Standalone mode with REST (and ZeroMQ) API to be provided as add-on.


## References

### Dynamo / Riak

http://basho.com/assets/bitcask-intro.pdf
http://www.slideshare.net/eredmond/distributed-stat-structures
http://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf

### Constraint Handling Rules

http://www.informatik.uni-ulm.de/pm/fileadmin/pm/home/fruehwirth/Papers/chr-lnai08.pdf
https://lifeware.inria.fr/~tmartine/papers/martinez11chr.pdf

### Distributed Datalog and Linear Logic

http://db.cs.berkeley.edu/papers/datalog2011-dedalus.pdf
http://www.eecs.berkeley.edu/Pubs/TechRpts/2012/EECS-2012-167.pdf
https://www.andrew.cmu.edu/user/liminjia/research/papers/incremental-ppdp.pdf
http://arxiv.org/pdf/1405.3556v1.pdf

### Convergent and Commutative Replicated Data Types (CRDTs)

http://hal.upmc.fr/inria-00555588/document
http://www.infoq.com/articles/Highly-Distributed-Computations-Without-Synchronization
http://research.microsoft.com/pubs/240462/ecoop15-extended-tr.pdf


## License

Copyright © 2015 Håkan Råberg

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
