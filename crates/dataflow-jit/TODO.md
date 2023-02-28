# Dataflow JIT Todo list

- [ ] Cleanup dataflow generation code
  - [ ] Implement dynamic scoping so that we can have arbitrarily nested scopes
- [ ] Window operators
  - [ ] Consult with Mihai to figure out the best design for this
    - Two main options, either one (or a few) very generalized operators or a handful of specialized ones,
      most window aggregations are likely fairly similar (since we're compiling SQL which has limited
      expressivity) so we'd get the benefit of clearer semantics and more efficient (through specialization)
      operators
- [ ] More codegen debug checks, make sure that none of our pointers ever go out of bounds of any of our objects
- [ ] Add debug layout checks to all dataflow operators
- [ ] Validation infrastructure
  - [ ] Type checking, make sure all accesses and operations are properly typed
  - [ ] Well-formedness checks
  - [ ] Initialization checks, make sure that all relevant columns are properly initialized
- [ ] Row vectors
  - Instead of `OrdZSet<Row, Weight>` we want a custom collection that holds untyped values, a truly untyped `DynVec`
  - Requires changes in a lot of the more fundamental `Trace`-related traits
- [ ] Implement `bincode::{Encode, Decode}` for row values
- [ ] Better layout algorithm
  - [ ] Add switch to make all null flags standalone booleans (that is, instead of bitsets make them each take up one byte)
  - [ ] Allow null niching strings
  - [ ] Spread out null flags as much as possible so that we have as many single byte flags as possible (they're more efficient to operate over)
- [ ] Optimizations of both the inner functions and the dataflows themselves
  - [ ] Eliminate const filters
  - [ ] Fuse filters, maps and filter maps
  - [ ] Turn non-mutating filter maps into filters
  - [ ] Linear operators can probably be fused with neg as well
  - [ ] Dataflow propagation, we can optimize things based off of their input attributes (e.g. a stream that comes
        from a `filter (x > 10)` tells us that `x` will always be greater than ten)
  - [ ] Automatically generate owned versions of functions
    - Automatically evaluate whether or not the owned version is actually better than the borrowed one
  - [ ] Once native row collections are implemented we can operate over them directly within our functions instead of
        using callback functions. This should be a significant speedup for things like filter and map
  - [ ] Auto-vectorization of operations over native row collections
  - [ ] When operating over persistent collections we can lazily deserialize/decode values, potentially benefiting projections
- [x] Add date and timestamp generation to proptesting so we can fuzz the relevant code
- [ ] Deduplicate functions, could help in reducing the number of functions we optimize and compile and could
      even allow us to deduplicate nodes with previously different inner functions
- [ ] Configurable difference/weight type, currently hardcoded to `i32`
- [ ] Dataflow planning optimizations
  - [ ] Push/pull differentiation and integration
  - [ ] Automatic incrementalization
  - [ ] Push/pull exchanges
  - [ ] Push/pull gathers
