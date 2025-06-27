# Rayon Compat

This is a way to run `rayon` on top of `forte`! The `rayon-compat` crate mocks the important bits of the api of `rayon_core` in a pretty simple and crude way, which is none-the-less enough to support most of what `rayon` needs.

To use this crate, apply the following cargo patch like one of these:
```
// If you want to clone forte and use it locally
[patch.crates-io]
rayon-core = { path = "path to this repo", package = "rayon-compat" }

// If you want to use the latest published version of forte
[patch.crates-io]
rayon-core = { path = "https://github.com/NthTensor/Forte", package = "rayon-compat" }
```
