use std::fs::create_dir_all;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, SyncSender};

use super::single::SplitEnum;
use crate::error::Result;
use crate::io;

/// Accepts rows assigned to a split and writes them in an appropriate way.
///
/// If a max chunk size as been specified it will round-robin the rows between
/// the splits.
pub(crate) struct SplitWriter {
    /// Sending halves of channels.
    ///
    /// We use a SyncSender here because we may end up reading much faster
    /// than writing, and we need to limit the size of the buffers.
    chunk_senders: Vec<SyncSender<String>>,

    /// Index of the chunk_sender which should receive the next row.
    next_index: usize,
}

impl SplitWriter {
    pub fn new(
        path: &Path,
        split: &SplitEnum,
        chunk_size: Option<u64>,
        total_rows: Option<u64>,
        compression: io::Compression,
    ) -> Result<(Self, Vec<ChunkWriter>)> {
        let n_chunks = match (split, chunk_size, total_rows) {
            // Just use one sender since there is no chunking required.
            (_, None, _) => 1,

            // Create one sender per chunk.
            (SplitEnum::Rows(r), Some(c), _) => (r.total / c as f64).ceil() as u64,

            // TODO:
            // We don't know how many chunks will be required. Create two
            // chunks; we'll fix this later.
            (SplitEnum::Proportion(_), Some(_), None) => 2,

            // Use as many senders as we estimate there will be chunks for this
            // split.
            (SplitEnum::Proportion(p), Some(c), Some(t)) => {
                ((t as f64) * p.proportion / c as f64).ceil() as u64 + 1
            }
        };

        let mut chunk_senders = Vec::new();
        let mut chunk_writers = Vec::new();
        for chunk_id in 0..n_chunks {
            let (sender, receiver) = std::sync::mpsc::sync_channel(100);
            chunk_senders.push(sender);

            let chunk_id = if n_chunks == 1 {
                None
            } else {
                Some(chunk_id)
            };
            let chunk_writer = ChunkWriter::new(
                path.to_path_buf(),
                split.name().to_string(),
                compression,
                chunk_id,
                chunk_size,
                receiver,
            );
            chunk_writers.push(chunk_writer);
        }

        Ok((
            SplitWriter {
                chunk_senders,
                next_index: 0,
            },
            chunk_writers,
        ))
    }

    /// Send a row to this split.
    ///
    /// The sender will assign it to the correct chunk (if there was no maximum
    /// chunk size specified, there is effectively only one chunk!)
    /// This will round-robin through the chunks.
    pub fn send(&mut self, row: String) -> Result<bool> {
        match self.chunk_senders.get(self.next_index) {
            Some(sender) => {
                sender.send(row)?;
                self.next_index += 1;
            }
            None => {
                // Start again at the next chunk
                self.chunk_senders[0].send(row)?;
                self.next_index = 1;
            }
        }
        Ok(true)
    }

    /// Send a row to all splits.
    ///
    /// Used for the header row.
    pub fn send_all(&mut self, row: &str) -> Result<()> {
        for sender in &self.chunk_senders {
            sender.send(row.to_string())?
        }
        Ok(())
    }

    pub fn finish(self) {
        for sender in self.chunk_senders {
            drop(sender);
        }
    }
}

/// Writes rows to files once they've been assigned to a split.
pub struct ChunkWriter {
    path: PathBuf,
    name: String,
    compression: io::Compression,
    pub chunk_id: Option<u64>,
    pub chunk_size: Option<u64>,
    pub receiver: Receiver<String>,
}

impl ChunkWriter {
    fn new(
        path: PathBuf,
        name: String,
        compression: io::Compression,
        chunk_id: Option<u64>,
        chunk_size: Option<u64>,
        receiver: Receiver<String>,
    ) -> Self {
        ChunkWriter {
            path,
            name,
            compression,
            chunk_id,
            chunk_size,
            receiver,
        }
    }

    pub fn output(&self, chunk_id: Option<u64>) -> Result<io::OutputWriter> {
        let mut filename = self.path.clone();
        let original_filename = self.path.file_stem().unwrap();
        filename.pop();
        filename.push(&self.name);
        create_dir_all(&filename)?;
        let chunk_part = match chunk_id {
            None => "".to_string(),
            Some(c) => format!(".{c:0>4}"),
        };
        let extension = match self.compression {
            io::Compression::GzipCompression => ".gz",
            io::Compression::Uncompressed => "",
        };
        filename.push(format!(
            "{}.{}{}.csv{}",
            original_filename.to_string_lossy(),
            &self.name,
            chunk_part,
            extension,
        ));
        io::open_output(filename, self.compression)
    }
    /// Handle writing of a row to this chunk.
    pub fn handle_row(&self, file: &mut io::OutputWriter, row: &str) -> Result<()> {
        file.write_all(row.as_bytes())?;
        Ok(())
    }
}
