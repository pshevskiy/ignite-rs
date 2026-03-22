#![cfg(not(feature = "ssl"))]

mod common;

// All invoke tests require a custom Docker image with deployed Java
// EntryProcessor classes (Phase 3). The wire-format verification tests
// are in invoke_protocol_test.rs.
//
// Blocked Java methods:
// - testInvokeSimpleCase, testInvokeAllSimpleCase, testExceptionHandling,
//   testInvokeInTransaction, testSerialization, testWithKeepBinary
