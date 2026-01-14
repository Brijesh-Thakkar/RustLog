/// Storage layer for RustLog
/// 
/// This module provides the foundational append-only log abstraction.
/// 
/// Current scope:
/// - Segment: append-only log file with offset assignment
/// - Index: offset → byte position mapping for fast lookups
/// - Mmap: read-only, zero-copy access to sealed files
/// 
/// Not yet implemented:
/// - Partition management
/// - Topic management
pub mod index;
pub mod mmap;
pub mod segment;
