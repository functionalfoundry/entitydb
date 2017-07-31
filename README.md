# workflo/entitydb


[![Clojars Project](https://img.shields.io/clojars/v/workflo/entitydb.svg)](https://clojars.org/workflo/entitydb)
| [API docs](https://workfloapp.github.io/entitydb/)
| [![Build Status](https://travis-ci.org/workfloapp/entitydb.svg?branch=master)](https://travis-ci.org/workfloapp/entitydb)

**NOTE: Everything here is considered work in progress and subject to change.**

Database for entities defined with `defentity` from `workflo/macros`.


## Database format

The database format is defined with the `workflo.entitydb.specs.v1/entitydb`
spec. An `entitydb` database is structured as follows:

```
{;; Actual database content
 :workflo.entitydb.v1/data
 {<entity-name-1> {<entity-id-1> <entity-data-1>
                   ...}
  ...}

 ;; Indexes
 :workflo.entitydb.v1/indexes
 {<index-name-1> <index-data-1>
  ...}}
```

where `:workflo.entitydb.v1/indexes` is entirely optional.

**NOTE: There is no support for indexes yet. We will work out a concept
soon.**

A specific database with two `:team` and `:user` entities might look
like this:

```
{:workflo.entitydb.v1/data
 {:user {"596e2b0e7ca846ed9508775ebe6f3541"
         {:workflo/id "596e2b0e7ca846ed9508775ebe6f3541"
          :user/name "Joe"
          :user/email "joe@email.com"}
         "596e2b0e9b814772aabf6a997273b3ed"
         {:workflo/id "596e2b0e9b814772aabf6a997273b3ed"
          :user/name "Linda"
          :user/email "linda@email.com"}}
  :team {"596e2b0e679e46aea7388db22ccd4b57"
         {:workflo/id "596e2b0e679e46aea7388db22ccd4b57"
          :team/name "Team Alpha"
          :team/members #{{:workflo/id "596e2b0e7ca846ed9508775ebe6f3541"}
                          {:workflo/id "596e2b0e9b814772aabf6a997273b3ed"}}}}}}}
```


## Testing

1. Install [boot](http://boot-clj.com/)
1. Clone this repository
2. Run the tests:
   - `boot test` to run tests once
   - `boot watch test` to run tests continuously on changes


## Copyright

This project is licensed under the [MIT license](https://mit-license.org/).

Copyright © 2017 Workflo, Inc.
