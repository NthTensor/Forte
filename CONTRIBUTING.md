# Contributing

Forte is a community project, and we welcome contributions. By submitting code to
forte, you are agreeing to license it under under our dual MIT/Apache 2.0
licensing.

## Patches

When submitting patches, please be sure to:

1. Use [Conventional](https://www.conventionalcommits.org/en/v1.0.0/) commit
   messages.

2. Keep your PR history linear. Do not create merge commits.

3. Update the changelog.

## Comments

All code must be thoroughly commented. Comments should: 

1. Always be full sentences, with grammar and punctuation.

2. Be clear, timely, and helpful, and self-contained.

3. Explain both why particular a design was chosen, and what it does.

4. Document all assumptions.

## Code Standard

This crate aims for a very high standard of formal correctness and memory
safety. In vague terms, we aim to match (or exceed) the general standard and
quality of safety comments found in the standard library, or mature and
well-respected rust projects such as `tokio` and `zerocopy`.

In more concrete terms, the `unsafe` code in this repo is governed by the
following rules:

1. All `unsafe` code should be isolated and documented.

   - Every `unsafe` definition must have a `# Safety` comment with enumerated
     caller obligations.
     
   - Every `unsafe` block must contain a single unsafe operation.
   
   - Every `unsafe` block must be prefixed with a `SAFETY:` comment that
     discharges each obligation on the contained `unsafe` operation, with a
     premise-by-premise safety argument.
   
2. All uses of raw pointers must be shown to obey rust's aliasing, provinance,
   ownership and validity rules.
   
3. All concurrent or atomic code must be shown to be data-race free under the
   semantics of the rust abstract machine.
   
   - Operations that may conflict must show an explicit happens-before edge.
   
   - Memory-coherence based arguments are inadmisable.
   
4. All instances where we execute functions or closures provided by users must
   be shown to be panic safe.
   
5. All instances of global mutable state must be shown to be panic safe.

6. Safety documentation must contain no arguments about any other properties
   (deadlocks, livelocks, etc).

The table-stakes requirement is that code be _sound_, meaning that safe code (or
unsafe code obeying a stated safety contract) can be shown to never produce UB.
